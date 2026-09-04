package store

import (
	"errors"

	"github.com/aadityakv/crane/internal/crane/model"
)

func applySource(work *RecoveredWork, cursor SourceCursor, outboxes []OutboxRecord) error {
	if uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("source outbox count exceeds v1 bounds")
	}
	if err := cursor.Source.Validate(); err != nil {
		return err
	}
	assignment, ok := findAssignment(work, cursor.Source.JobID)
	if !ok {
		return errors.New("source references unknown assignment")
	}
	wantEOF, err := model.SourceEOF(assignment.Topology, cursor.Source)
	if err != nil || cursor.EOF != wantEOF {
		return errors.New("source cursor EOF does not match installed topology")
	}
	if cursor.NextSequence == 0 || cursor.NextSequence > model.LimitsV1().MaxSourceSequences+1 || cursor.Watermark > model.LimitsV1().MaxSourceSequences || cursor.EOF > model.LimitsV1().MaxSourceSequences || cursor.EOF != 0 && cursor.NextSequence > cursor.EOF+1 || cursor.Watermark >= cursor.NextSequence || cursor.Watermark != 0 && cursor.RaftIndex == 0 || cursor.Watermark == 0 && cursor.CheckpointRevision != 0 || !validCheckpointAuthority(cursor) {
		return errors.New("source cursor outside bounds")
	}
	source, ok := findToken(assignment.Assignment, cursor.Source)
	if !ok {
		return errors.New("source cursor has no installed assignment token")
	}
	expected, err := expectedSourceOutboxes(cursor, source, assignment)
	if err != nil {
		return err
	}
	index := sourceIndex(work.Sources, cursor.Source)
	exactRetry := index >= 0 && cursor == work.Sources[index]
	if len(outboxes) != len(expected) {
		if exactRetry {
			return model.ErrIdentityReuse
		}
		return errors.New("source outboxes are not the complete topology-derived set")
	}
	if !outboxesCanonical(outboxes) {
		if exactRetry {
			return model.ErrIdentityReuse
		}
		return errors.New("source outboxes are not in canonical identity order")
	}
	seen := make(map[model.DeliveryID]struct{}, len(outboxes))
	for _, outbox := range outboxes {
		if _, duplicate := seen[outbox.ID]; duplicate {
			return errors.New("duplicate source outbox")
		}
		seen[outbox.ID] = struct{}{}
		want, exists := expected[outbox.ID]
		if !exists || outbox.Completed || outbox.Accepted || outbox.RetryDeadlineUnixNano != 0 || !equalOutboxDefinition(want, outbox) {
			if exactRetry {
				return model.ErrIdentityReuse
			}
			return errors.New("source outbox does not match immutable source route")
		}
		if err := validateOutbox(outbox, assignment, work.Fence); err != nil {
			return err
		}
	}
	if index >= 0 {
		prior := work.Sources[index]
		if cursor == prior {
			for _, outbox := range outboxes {
				stored := outboxIndex(work.Outboxes, outbox.ID)
				if stored < 0 || !equalOutboxDefinition(work.Outboxes[stored], outbox) {
					return model.ErrIdentityReuse
				}
			}
			return nil
		}
		if cursor.NextSequence <= prior.NextSequence || cursor.NextSequence != prior.NextSequence+1 || cursor.Watermark != prior.Watermark || cursor.RaftIndex != prior.RaftIndex || cursor.CheckpointRevision != prior.CheckpointRevision || cursor.CheckpointAuthority != prior.CheckpointAuthority {
			return errors.New("source cursor regression or sequence gap")
		}
	} else if cursor.NextSequence > 2 || cursor.Watermark != 0 || cursor.RaftIndex != 0 || cursor.CheckpointRevision != 0 || cursor.CheckpointAuthority != (CheckpointAuthority{}) {
		return errors.New("initial source cursor skips durable sequence or checkpoint state")
	}
	for _, outbox := range outboxes {
		if outboxIndex(work.Outboxes, outbox.ID) >= 0 {
			return model.ErrIdentityReuse
		}
	}
	if index < 0 {
		work.Sources = append(work.Sources, cursor)
	} else {
		work.Sources[index] = cursor
	}
	for _, outbox := range outboxes {
		work.Outboxes = append(work.Outboxes, outbox.Clone())
	}
	return nil
}

func validCheckpointAuthority(cursor SourceCursor) bool {
	proof := cursor.CheckpointAuthority
	if cursor.CheckpointRevision == 0 {
		return proof == (CheckpointAuthority{})
	}
	return proof.JobControlRevision != 0 && proof.AssignmentRevision != 0 && proof.AssignmentDigest != ([32]byte{}) && proof.SourceToken.Validate() == nil && proof.SourceToken.Task == cursor.Source && proof.SourceToken.AssignmentRevision == proof.AssignmentRevision && proof.CoordinatorEpoch.Validate() == nil
}

func expectedSourceOutboxes(cursor SourceCursor, source model.AssignmentToken, assignment InstalledAssignment) (map[model.DeliveryID]OutboxRecord, error) {
	result := make(map[model.DeliveryID]OutboxRecord)
	if cursor.NextSequence <= 1 {
		return result, nil
	}
	sequence := cursor.NextSequence - 1
	tuple, exists, err := model.SourceTuple(assignment.Topology, cursor.Source, sequence)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("source cursor advances beyond immutable EOF")
	}
	tupleID := model.DeriveSourceTupleID(cursor.Source.JobID, cursor.Source, sequence)
	for _, edge := range assignment.Topology.Spec().Edges {
		if edge.SourceStageID != cursor.Source.StageID {
			continue
		}
		partitions, err := model.Route(assignment.Topology, edge, tupleID, tuple)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			task := model.TaskID{JobID: cursor.Source.JobID, StageID: edge.DestinationStageID, Partition: partition}
			destination, ok := findToken(assignment.Assignment, task)
			if !ok {
				return nil, errors.New("source outbox destination has no assignment token")
			}
			id := model.DeliveryID{Tuple: tupleID, EdgeID: edge.EdgeID, DestinationTask: task}
			if _, duplicate := result[id]; duplicate {
				return nil, errors.New("source route derived duplicate outbox")
			}
			result[id] = OutboxRecord{ID: id, Tuple: cloneTuple(tuple), Producer: source, Destination: destination, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, CoordinatorEpoch: assignment.CoordinatorEpoch}
		}
	}
	return result, nil
}

// AdvanceSource atomically persists a source cursor and the outboxes it created.
func (store *Store) AdvanceSource(cursor SourceCursor, outboxes []OutboxRecord) error {
	payload, err := encodeSource(cursor, outboxes)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordSource, Payload: payload}}}, BoundarySourceAdvanced)
}

func encodeSource(cursor SourceCursor, outboxes []OutboxRecord) ([]byte, error) {
	return encodeSourceSchema(cursor, outboxes, sourceRecordSchema)
}

func encodeSourceSchema(cursor SourceCursor, outboxes []OutboxRecord, schema uint16) ([]byte, error) {
	if err := cursor.Source.Validate(); err != nil {
		return nil, err
	}
	if schema != domainRecordSchema && schema != sourceRecordSchema {
		return nil, errors.New("unsupported source record schema")
	}
	if uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("too many source outboxes")
	}
	w := newRecordWriter()
	w.u16(schema)
	w.task(cursor.Source)
	w.u64(cursor.NextSequence)
	w.u64(cursor.EOF)
	w.u64(cursor.Watermark)
	w.u64(cursor.RaftIndex)
	if schema == sourceRecordSchema {
		w.u64(cursor.CheckpointRevision)
		w.u64(cursor.CheckpointAuthority.JobControlRevision)
		w.u64(cursor.CheckpointAuthority.AssignmentRevision)
		w.fixed32(cursor.CheckpointAuthority.AssignmentDigest)
		w.token(cursor.CheckpointAuthority.SourceToken)
		w.epoch(cursor.CheckpointAuthority.CoordinatorEpoch)
	}
	w.u16(uint16(len(outboxes)))
	for _, outbox := range outboxes {
		b, e := encodeOutbox(outbox)
		if e != nil {
			return nil, e
		}
		w.blob(b)
	}
	return w.bytes(), nil
}

func decodeSource(payload []byte) (SourceCursor, []OutboxRecord, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil || schema != domainRecordSchema && schema != sourceRecordSchema {
		return SourceCursor{}, nil, errors.New("unsupported source record schema")
	}
	var cursor SourceCursor
	if cursor.Source, err = r.task(); err != nil {
		return cursor, nil, err
	}
	if cursor.NextSequence, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if cursor.EOF, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if cursor.Watermark, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if cursor.RaftIndex, err = r.u64(); err != nil {
		return cursor, nil, err
	}
	if schema == sourceRecordSchema {
		if cursor.CheckpointRevision, err = r.u64(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.JobControlRevision, err = r.u64(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.AssignmentRevision, err = r.u64(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.AssignmentDigest, err = r.fixed32(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.SourceToken, err = r.token(); err != nil {
			return cursor, nil, err
		}
		if cursor.CheckpointAuthority.CoordinatorEpoch, err = r.epoch(); err != nil {
			return cursor, nil, err
		}
	}
	count, err := r.u16()
	if err != nil {
		return cursor, nil, err
	}
	if uint64(count) > model.LimitsV1().MaxDerivedDeliveries || r.remaining() < int(count)*4 {
		return cursor, nil, errors.New("source outbox collection exceeds bounds or remaining bytes")
	}
	outboxes := make([]OutboxRecord, int(count))
	for i := range outboxes {
		b, e := r.blob(MaxRecordPayloadBytes)
		if e != nil {
			return cursor, nil, e
		}
		outboxes[i], e = decodeOutbox(b)
		if e != nil {
			return cursor, nil, e
		}
	}
	if !r.done() {
		return cursor, nil, errors.New("trailing source bytes")
	}
	return cursor, outboxes, nil
}

func sourceIndex(v []SourceCursor, id model.TaskID) int {
	for i := range v {
		if v[i].Source == id {
			return i
		}
	}
	return -1
}
