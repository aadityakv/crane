package store

import (
	"errors"
	"math"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func applyCheckpoint(work *RecoveredWork, notice model.CheckpointNotice, proven map[model.DeliveryID]struct{}) error {
	if err := notice.Validate(); err != nil {
		return err
	}
	index := sourceIndex(work.Sources, notice.Source)
	if index >= 0 {
		prior := work.Sources[index]
		if notice.Watermark == prior.Watermark {
			if notice.RaftIndex != prior.RaftIndex {
				return model.ErrIdentityReuse
			}
			if prior.CheckpointRevision == 0 {
				assignment, ok := findAssignment(work, notice.JobID)
				if !ok {
					return ErrCheckpointAuthorityUnavailable
				}
				report, err := matchingLegacyCompletionReport(work, assignment, notice, prior)
				if err != nil {
					return errors.Join(ErrCheckpointAuthorityUnavailable, err)
				}
				if report.ExpectedCheckpointRevision == math.MaxUint64 {
					return ErrCapacity
				}
				prior.CheckpointRevision = report.ExpectedCheckpointRevision + 1
				prior.CheckpointAuthority = checkpointAuthority(assignment, report)
				work.Sources[index] = prior
				return nil
			}
			if notice.Epoch != prior.CheckpointAuthority.CoordinatorEpoch {
				return model.ErrIdentityReuse
			}
			return nil
		}
		if notice.Watermark < prior.Watermark || notice.RaftIndex < prior.RaftIndex {
			return errors.New("checkpoint regression")
		}
		if notice.RaftIndex == prior.RaftIndex {
			return model.ErrIdentityReuse
		}
	}
	if notice.Epoch != work.Fence {
		return errors.New("checkpoint coordinator fence mismatch")
	}
	assignment, ok := findAssignment(work, notice.JobID)
	if !ok || assignment.CoordinatorEpoch != notice.Epoch {
		return errors.New("checkpoint references no current assignment authority")
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Source)
	if err != nil || notice.Watermark > eof {
		return errors.New("checkpoint source or watermark is outside installed topology")
	}
	prior := SourceCursor{Source: notice.Source, NextSequence: 1, EOF: eof}
	if index >= 0 {
		prior = work.Sources[index]
	}
	if notice.Watermark == 0 || notice.Watermark == math.MaxUint64 || notice.Watermark > model.LimitsV1().MaxSourceSequences || prior.CheckpointRevision == math.MaxUint64 {
		return errors.New("checkpoint watermark or revision outside v1 bounds")
	}
	// Committed-watermark adoption (Task 24 defect #2 ruling): the notice
	// arrived over the current fence (validated above), so the coordinator's
	// fail-closed committed-watermark validation is the commit proof. When no
	// exact pending CompletionReport correlates, the strictly higher watermark
	// (or first watermark of a reassigned owner) persists under the CURRENT
	// authority proof derived from the durable installed assignment.
	report, _ := matchingCompletionReport(work, assignment, notice, prior, eof)
	updated := prior
	updated.Watermark, updated.RaftIndex = notice.Watermark, notice.RaftIndex
	if report != nil {
		if report.ExpectedCheckpointRevision == math.MaxUint64 {
			return ErrCapacity
		}
		updated.CheckpointRevision = report.ExpectedCheckpointRevision + 1
		updated.CheckpointAuthority = checkpointAuthority(assignment, report)
	} else {
		token, tokenOK := findToken(assignment.Assignment, notice.Source)
		if !tokenOK {
			return errors.New("checkpoint source has no installed token")
		}
		if prior.CheckpointRevision == math.MaxUint64 {
			return ErrCapacity
		}
		updated.CheckpointRevision = prior.CheckpointRevision + 1
		updated.CheckpointAuthority = CheckpointAuthority{JobControlRevision: assignment.JobControlRevision, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, SourceToken: token, CoordinatorEpoch: notice.Epoch}
	}
	if updated.NextSequence <= notice.Watermark {
		updated.NextSequence = notice.Watermark + 1
	}
	if index >= 0 {
		work.Sources[index] = updated
	} else {
		work.Sources = append(work.Sources, updated)
	}
	for i := range work.Deliveries {
		delivery := work.Deliveries[i]
		if delivery.ID.Tuple.SourceTask == notice.Source && delivery.ID.Tuple.SourceSequence <= notice.Watermark {
			if delivery.State != Completed && delivery.State != Compacted {
				return errors.New("checkpoint covers incomplete delivery")
			}
		}
	}
	keptDeliveries := work.Deliveries[:0]
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.SourceTask != notice.Source || delivery.ID.Tuple.SourceSequence > notice.Watermark {
			keptDeliveries = append(keptDeliveries, delivery)
		}
	}
	work.Deliveries = keptDeliveries
	kept := work.Outboxes[:0]
	for _, outbox := range work.Outboxes {
		if outbox.ID.Tuple.SourceTask != notice.Source || outbox.ID.Tuple.SourceSequence > notice.Watermark {
			kept = append(kept, outbox)
			continue
		}
		// A compacted outbox can never be recreated — the source cursor's
		// watermark/next-sequence monotonicity forbids re-emitting its
		// sequence and both creation funnels reject an already-present
		// identity — so its cached proof is dead weight: prune it.
		delete(proven, outbox.ID)
	}
	work.Outboxes = kept
	return nil
}

func applyCheckpointObservation(work *RecoveredWork, observation CommittedCheckpoint) error {
	if err := validateCheckpointObservation(observation); err != nil {
		return err
	}
	assignment, ok := findAssignment(work, observation.Notice.JobID)
	if !ok || assignment.CoordinatorEpoch != work.Fence || observation.Notice.Epoch != work.Fence || observation.JobControlRevision != assignment.JobControlRevision || observation.AssignmentRevision != assignment.Assignment.Revision || observation.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("checkpoint observation authority mismatch")
	}
	stage, ok := findStage(assignment.Topology, observation.Notice.Source.StageID)
	eof, eofErr := model.SourceEOF(assignment.Topology, observation.Notice.Source)
	if !ok || stage.Role != model.StageSource || eofErr != nil || observation.Notice.Watermark > eof {
		return errors.New("checkpoint observation source is not a source stage")
	}
	index := checkpointObservationIndex(work.Checkpoints, observation.Notice.Source)
	if index >= 0 {
		prior := work.Checkpoints[index]
		if prior.Notice.RaftIndex == observation.Notice.RaftIndex {
			if prior == observation {
				return nil
			}
			return model.ErrIdentityReuse
		}
		if observation.Notice.RaftIndex < prior.Notice.RaftIndex || observation.Notice.Watermark < prior.Notice.Watermark {
			return errors.New("stale checkpoint observation")
		}
		work.Checkpoints[index] = observation
		return nil
	}
	maxCheckpoints := model.LimitsV1().MaxRetainedJobs * model.LimitsV1().MaxTasksPerJob
	if uint64(len(work.Checkpoints)) >= maxCheckpoints {
		return ErrCapacity
	}
	work.Checkpoints = append(work.Checkpoints, observation)
	sort.Slice(work.Checkpoints, func(i, j int) bool {
		return taskLess(work.Checkpoints[i].Notice.Source, work.Checkpoints[j].Notice.Source)
	})
	return nil
}

func validateCheckpointObservation(observation CommittedCheckpoint) error {
	if err := observation.Notice.Validate(); err != nil {
		return err
	}
	if observation.JobControlRevision == 0 || observation.AssignmentRevision == 0 || observation.AssignmentDigest == ([32]byte{}) {
		return errors.New("invalid checkpoint observation assignment fence")
	}
	return nil
}

// applyLegacyCheckpoint replays only an already-durable schema-v1 Task14
// checkpoint. New writes use schema v2 and always enter applyCheckpoint.
func applyLegacyCheckpoint(work *RecoveredWork, notice model.CheckpointNotice, proven map[model.DeliveryID]struct{}) error {
	if err := notice.Validate(); err != nil {
		return err
	}
	if notice.Epoch != work.Fence {
		return errors.New("checkpoint coordinator fence mismatch")
	}
	assignment, ok := findAssignment(work, notice.JobID)
	if !ok {
		return errors.New("checkpoint references unknown assignment")
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Source)
	if err != nil || notice.Watermark > eof {
		return errors.New("checkpoint source or watermark is outside installed topology")
	}
	index := sourceIndex(work.Sources, notice.Source)
	if index >= 0 {
		prior := work.Sources[index]
		if notice.Watermark < prior.Watermark || notice.RaftIndex < prior.RaftIndex {
			return errors.New("checkpoint regression")
		}
		if notice.Watermark == prior.Watermark && notice.RaftIndex == prior.RaftIndex {
			return nil
		}
		prior.Watermark, prior.RaftIndex = notice.Watermark, notice.RaftIndex
		work.Sources[index] = prior
	} else {
		if notice.Watermark == math.MaxUint64 || notice.Watermark > model.LimitsV1().MaxSourceSequences {
			return errors.New("checkpoint watermark outside v1 bounds")
		}
		work.Sources = append(work.Sources, SourceCursor{Source: notice.Source, NextSequence: notice.Watermark + 1, EOF: eof, Watermark: notice.Watermark, RaftIndex: notice.RaftIndex})
	}
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.SourceTask == notice.Source && delivery.ID.Tuple.SourceSequence <= notice.Watermark && delivery.State != Completed && delivery.State != Compacted {
			return errors.New("checkpoint covers incomplete delivery")
		}
	}
	keptDeliveries := work.Deliveries[:0]
	for _, delivery := range work.Deliveries {
		if delivery.ID.Tuple.SourceTask != notice.Source || delivery.ID.Tuple.SourceSequence > notice.Watermark {
			keptDeliveries = append(keptDeliveries, delivery)
		}
	}
	work.Deliveries = keptDeliveries
	keptOutboxes := work.Outboxes[:0]
	for _, outbox := range work.Outboxes {
		if outbox.ID.Tuple.SourceTask != notice.Source || outbox.ID.Tuple.SourceSequence > notice.Watermark {
			keptOutboxes = append(keptOutboxes, outbox)
			continue
		}
		// Compacted identities never return (see applyCheckpoint); prune the
		// dead proof alongside the dropped record.
		delete(proven, outbox.ID)
	}
	work.Outboxes = keptOutboxes
	return nil
}

func exactLegacyCheckpointProofAvailable(work *RecoveredWork, notice model.CheckpointNotice) bool {
	if notice.Validate() != nil {
		return false
	}
	index := sourceIndex(work.Sources, notice.Source)
	if index >= 0 && notice.Watermark == work.Sources[index].Watermark {
		prior := work.Sources[index]
		if prior.CheckpointRevision != 0 {
			return notice.RaftIndex == prior.RaftIndex && notice.Epoch == prior.CheckpointAuthority.CoordinatorEpoch
		}
		assignment, ok := findAssignment(work, notice.JobID)
		if !ok || assignment.CoordinatorEpoch != notice.Epoch {
			return false
		}
		_, err := matchingLegacyCompletionReport(work, assignment, notice, prior)
		return err == nil
	}
	if notice.Epoch != work.Fence {
		return false
	}
	assignment, ok := findAssignment(work, notice.JobID)
	if !ok || assignment.CoordinatorEpoch != notice.Epoch {
		return false
	}
	eof, err := model.SourceEOF(assignment.Topology, notice.Source)
	if err != nil || notice.Watermark > eof {
		return false
	}
	prior := SourceCursor{Source: notice.Source, NextSequence: 1, EOF: eof}
	if index >= 0 {
		prior = work.Sources[index]
	}
	_, err = matchingCompletionReport(work, assignment, notice, prior, eof)
	return err == nil
}

func matchingCompletionReport(work *RecoveredWork, assignment InstalledAssignment, notice model.CheckpointNotice, prior SourceCursor, eof uint64) (*model.CompletionReport, error) {
	source, exists := findToken(assignment.Assignment, notice.Source)
	if !exists {
		return nil, errors.New("checkpoint source has no installed token")
	}
	for index := range work.PendingEvents {
		event := &work.PendingEvents[index]
		report := event.Completion
		if report == nil || report.JobID != notice.JobID || report.Source != notice.Source || report.New != notice.Watermark {
			continue
		}
		legacyRevision := prior.Watermark != 0 && prior.CheckpointRevision == 0 && prior.CheckpointAuthority == (CheckpointAuthority{})
		if report.Validate() != nil || report.JobControlRevision != assignment.JobControlRevision || report.AssignmentRevision != assignment.Assignment.Revision || report.Token != source || report.Epoch != notice.Epoch || !legacyRevision && report.ExpectedCheckpointRevision != prior.CheckpointRevision || report.Prior != prior.Watermark || report.EOF != eof || event.WorkerID != source.WorkerID || event.WorkerEpoch != source.WorkerEpoch || event.TransactionID != report.WorkerTransactionID {
			return nil, errors.New("checkpoint completion event authority mismatch")
		}
		return report, nil
	}
	return nil, errors.New("checkpoint has no exact durable completion event")
}

func matchingLegacyCompletionReport(work *RecoveredWork, assignment InstalledAssignment, notice model.CheckpointNotice, prior SourceCursor) (*model.CompletionReport, error) {
	source, exists := findToken(assignment.Assignment, notice.Source)
	if !exists {
		return nil, errors.New("legacy checkpoint source has no installed token")
	}
	for index := range work.PendingEvents {
		event := &work.PendingEvents[index]
		report := event.Completion
		if report == nil || report.JobID != notice.JobID || report.Source != notice.Source || report.New != notice.Watermark {
			continue
		}
		if report.Validate() != nil || report.JobControlRevision != assignment.JobControlRevision || report.AssignmentRevision != assignment.Assignment.Revision || report.Token != source || report.Epoch != notice.Epoch || report.New != prior.Watermark || report.EOF != prior.EOF || event.WorkerID != source.WorkerID || event.WorkerEpoch != source.WorkerEpoch || event.TransactionID != report.WorkerTransactionID {
			return nil, errors.New("legacy checkpoint completion authority mismatch")
		}
		return report, nil
	}
	return nil, errors.New("legacy checkpoint has no exact durable completion event")
}

func checkpointAuthority(assignment InstalledAssignment, report *model.CompletionReport) CheckpointAuthority {
	return CheckpointAuthority{JobControlRevision: report.JobControlRevision, AssignmentRevision: report.AssignmentRevision, AssignmentDigest: assignment.Assignment.Digest, SourceToken: report.Token, CoordinatorEpoch: report.Epoch}
}

// ApplyCheckpoint persists a monotonic watermark before compacting covered replay state.
func (store *Store) ApplyCheckpoint(notice model.CheckpointNotice) error {
	payload, err := encodeCheckpoint(notice)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordCheckpoint, Payload: payload}}}, BoundaryCheckpointApplied)
}

// ObserveCheckpoint durably records one authenticated committed checkpoint for
// a local participant that does not own the source cursor.
func (store *Store) ObserveCheckpoint(notice protocol.CheckpointNotice) error {
	observation := CommittedCheckpoint{Notice: notice.Notice, JobControlRevision: notice.JobControlRevision, AssignmentRevision: notice.AssignmentRevision, AssignmentDigest: notice.AssignmentDigest}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	if index := checkpointObservationIndex(store.work.Checkpoints, observation.Notice.Source); index >= 0 && store.work.Checkpoints[index].Notice.RaftIndex == observation.Notice.RaftIndex {
		if store.work.Checkpoints[index] == observation {
			return nil
		}
		return model.ErrIdentityReuse
	}
	payload, err := encodeCheckpointObservation(observation)
	if err != nil {
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordCheckpointObservation, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	store.durable(BoundaryCheckpointObserved)
	return nil
}

func encodeCheckpoint(n model.CheckpointNotice) ([]byte, error) {
	if err := n.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(checkpointRecordSchema)
	w.job(n.JobID)
	w.task(n.Source)
	w.u64(n.Watermark)
	w.u64(n.RaftIndex)
	w.epoch(n.Epoch)
	return w.bytes(), nil
}

func decodeCheckpoint(payload []byte) (model.CheckpointNotice, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil {
		return model.CheckpointNotice{}, err
	}
	if schema != domainRecordSchema && schema != checkpointRecordSchema {
		return model.CheckpointNotice{}, errors.New("unsupported checkpoint record schema")
	}
	var n model.CheckpointNotice
	if n.JobID, err = r.job(); err != nil {
		return n, err
	}
	if n.Source, err = r.task(); err != nil {
		return n, err
	}
	if n.Watermark, err = r.u64(); err != nil {
		return n, err
	}
	if n.RaftIndex, err = r.u64(); err != nil {
		return n, err
	}
	if n.Epoch, err = r.epoch(); err != nil || !r.done() {
		return n, errors.New("invalid checkpoint record")
	}
	return n, n.Validate()
}

func encodeCheckpointObservation(observation CommittedCheckpoint) ([]byte, error) {
	if err := validateCheckpointObservation(observation); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(checkpointObservationRecordSchema)
	w.job(observation.Notice.JobID)
	w.task(observation.Notice.Source)
	w.u64(observation.Notice.Watermark)
	w.u64(observation.Notice.RaftIndex)
	w.epoch(observation.Notice.Epoch)
	w.u64(observation.JobControlRevision)
	w.u64(observation.AssignmentRevision)
	w.fixed32(observation.AssignmentDigest)
	return w.bytes(), nil
}

func decodeCheckpointObservation(payload []byte) (CommittedCheckpoint, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil || schema != checkpointObservationRecordSchema {
		return CommittedCheckpoint{}, errors.New("unsupported checkpoint observation record schema")
	}
	var observation CommittedCheckpoint
	if observation.Notice.JobID, err = r.job(); err != nil {
		return observation, err
	}
	if observation.Notice.Source, err = r.task(); err != nil {
		return observation, err
	}
	if observation.Notice.Watermark, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.Notice.RaftIndex, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.Notice.Epoch, err = r.epoch(); err != nil {
		return observation, err
	}
	if observation.JobControlRevision, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.AssignmentRevision, err = r.u64(); err != nil {
		return observation, err
	}
	if observation.AssignmentDigest, err = r.fixed32(); err != nil || !r.done() {
		return observation, errors.New("invalid checkpoint observation record")
	}
	return observation, validateCheckpointObservation(observation)
}

func checkpointObservationIndex(v []CommittedCheckpoint, id model.TaskID) int {
	for i := range v {
		if v[i].Notice.Source == id {
			return i
		}
	}
	return -1
}
