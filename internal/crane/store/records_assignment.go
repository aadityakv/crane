package store

import (
	"bytes"
	"errors"
	"sort"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func applyFence(work *RecoveredWork, epoch model.CoordinatorEpoch) error {
	if err := epoch.Validate(); err != nil {
		return err
	}
	comparison := compareEpochOrder(epoch, work.Fence)
	if work.Fence == (model.CoordinatorEpoch{}) {
		work.Fence = epoch
		return nil
	}
	if comparison < 0 || comparison == 0 && epoch != work.Fence {
		return errors.New("stale or colliding coordinator epoch")
	}
	if comparison > 0 {
		work.Fence = epoch
	}
	return nil
}

func applyAssignment(work *RecoveredWork, installed InstalledAssignment) error {
	if work.Fence == (model.CoordinatorEpoch{}) || installed.CoordinatorEpoch != work.Fence {
		return errors.New("assignment coordinator fence mismatch")
	}
	decoded, err := model.DecodeTopology(installed.SpecificationBytes)
	if err != nil {
		return err
	}
	if err := installed.Assignment.Validate(decoded); err != nil {
		return err
	}
	if installed.JobControlRevision == 0 || installed.SchedulingState < model.SchedulingClosed || installed.SchedulingState > model.SchedulingDraining {
		return errors.New("invalid installed assignment metadata")
	}
	installed.Topology = decoded
	index := assignmentIndex(work.Assignments, installed.Assignment.JobID)
	if index >= 0 {
		prior := work.Assignments[index]
		if installed.Assignment.Revision < prior.Assignment.Revision {
			return errors.New("stale assignment revision")
		}
		if installed.Assignment.Revision == prior.Assignment.Revision {
			if equalInstalledAssignment(prior, installed) {
				return nil
			}
			contentEqual := equalInstalledAssignmentContent(prior, installed)
			if contentEqual && compareEpochOrder(installed.CoordinatorEpoch, prior.CoordinatorEpoch) > 0 {
				// Leadership rebind: content identical under strictly newer
				// committed authority durably rebinds the fence owner and
				// records the incoming worker-local scheduling state.
				work.Assignments[index] = cloneInstalled(installed)
				return nil
			}
			if contentEqual && installed.CoordinatorEpoch == prior.CoordinatorEpoch && admissionSchedulingProgression(prior.SchedulingState, installed.SchedulingState) {
				// Admission progressions at the equal current fence: verified
				// activation (Closed→Running) and re-fence before
				// re-verification (Running→Closed) change only worker-local
				// admission state, never attempts or custody.
				work.Assignments[index] = cloneInstalled(installed)
				return nil
			}
			if installed.Assignment.Digest != prior.Assignment.Digest || !bytes.Equal(installed.SpecificationBytes, prior.SpecificationBytes) || !equalTokens(installed.Assignment.Tasks, prior.Assignment.Tasks) || !equalReplicas(installed.Assignment.ResultReplicas, prior.Assignment.ResultReplicas) || installed.JobControlRevision <= prior.JobControlRevision {
				return model.ErrIdentityReuse
			}
			// Lifecycle fencing changes JobControlRevision independently of the
			// complete immutable AssignmentSet revision.
			work.Assignments[index] = cloneInstalled(installed)
			return nil
		}
		if installed.JobControlRevision < prior.JobControlRevision || !bytes.Equal(installed.SpecificationBytes, prior.SpecificationBytes) {
			return errors.New("assignment replacement regresses job revision or changes immutable topology")
		}
		for _, oldToken := range prior.Assignment.Tasks {
			for _, newToken := range installed.Assignment.Tasks {
				if newToken.Task != oldToken.Task {
					continue
				}
				if newToken.Attempt < oldToken.Attempt || (newToken.WorkerID != oldToken.WorkerID || newToken.WorkerEpoch != oldToken.WorkerEpoch) && newToken.Attempt <= oldToken.Attempt {
					return errors.New("assignment replacement regresses task attempt")
				}
				break
			}
		}
		work.Assignments[index] = cloneInstalled(installed)
	} else {
		if uint64(len(work.Assignments)) >= model.LimitsV1().MaxRetainedJobs {
			return ErrCapacity
		}
		work.Assignments = append(work.Assignments, cloneInstalled(installed))
	}
	sort.Slice(work.Assignments, func(i, j int) bool {
		return bytes.Compare(work.Assignments[i].Assignment.JobID[:], work.Assignments[j].Assignment.JobID[:]) < 0
	})
	return nil
}

// Fence durably advances the sole coordinator authority.
func (store *Store) Fence(epoch model.CoordinatorEpoch) error {
	payload, err := encodeFence(epoch)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordFence, Payload: payload}}}, BoundaryFence)
}

// InstallAssignment atomically validates, owns, and replaces one complete assignment.
func (store *Store) InstallAssignment(set model.AssignmentSet, specification model.TopologySpec, jobRevision uint64, scheduling model.SchedulingState, epoch model.CoordinatorEpoch) error {
	validated, err := model.ValidateTopology(specification)
	if err != nil {
		return err
	}
	if err = set.Validate(validated); err != nil {
		return err
	}
	installed := InstalledAssignment{Assignment: set, SpecificationBytes: validated.CanonicalBytes(), Topology: validated, JobControlRevision: jobRevision, SchedulingState: scheduling, CoordinatorEpoch: epoch}
	payload, err := encodeAssignment(installed)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordAssignment, Payload: payload}}}, assignmentBoundary(scheduling))
}

func encodeFence(epoch model.CoordinatorEpoch) ([]byte, error) {
	if err := epoch.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.epoch(epoch)
	return w.bytes(), nil
}

func decodeFence(payload []byte) (model.CoordinatorEpoch, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return model.CoordinatorEpoch{}, err
	}
	epoch, err := r.epoch()
	if err != nil || !r.done() {
		return model.CoordinatorEpoch{}, errors.New("invalid fence record")
	}
	return epoch, epoch.Validate()
}

func encodeAssignment(a InstalledAssignment) ([]byte, error) {
	decoded, err := model.DecodeTopology(a.SpecificationBytes)
	if err != nil {
		return nil, err
	}
	message := protocol.AssignmentSetInstall{Assignment: a.Assignment, Specification: decoded.Spec(), SpecificationDigest: decoded.Digest(), JobControlRevision: a.JobControlRevision, SchedulingState: a.SchedulingState, CoordinatorEpoch: a.CoordinatorEpoch}
	encoded, err := protocol.MarshalAssignmentSetInstall(message)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.blob(encoded)
	return w.bytes(), nil
}

func decodeAssignment(payload []byte) (InstalledAssignment, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return InstalledAssignment{}, err
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil || !r.done() {
		return InstalledAssignment{}, errors.New("invalid assignment record")
	}
	message, err := protocol.UnmarshalAssignmentSetInstall(encoded)
	if err != nil {
		return InstalledAssignment{}, err
	}
	topology, err := model.ValidateTopology(message.Specification)
	if err != nil {
		return InstalledAssignment{}, err
	}
	if topology.Digest() != message.SpecificationDigest {
		return InstalledAssignment{}, errors.New("assignment specification digest mismatch")
	}
	return InstalledAssignment{Assignment: message.Assignment, SpecificationBytes: topology.CanonicalBytes(), Topology: topology, JobControlRevision: message.JobControlRevision, SchedulingState: message.SchedulingState, CoordinatorEpoch: message.CoordinatorEpoch}, nil
}

func cloneInstalled(v InstalledAssignment) InstalledAssignment {
	v.Assignment.Tasks = append([]model.AssignmentToken(nil), v.Assignment.Tasks...)
	v.Assignment.ResultReplicas = append([]model.ResultReplicaSet(nil), v.Assignment.ResultReplicas...)
	v.SpecificationBytes = append([]byte(nil), v.SpecificationBytes...)
	v.Topology, _ = model.DecodeTopology(v.SpecificationBytes)
	return v
}

func assignmentIndex(v []InstalledAssignment, id model.JobID) int {
	for i := range v {
		if v[i].Assignment.JobID == id {
			return i
		}
	}
	return -1
}

func findAssignment(w *RecoveredWork, id model.JobID) (InstalledAssignment, bool) {
	i := assignmentIndex(w.Assignments, id)
	if i < 0 {
		return InstalledAssignment{}, false
	}
	return w.Assignments[i], true
}

func assignmentTargetsWorker(assignment model.AssignmentSet, nodeID uint16, workerEpoch model.WorkerEpoch) bool {
	for _, token := range assignment.Tasks {
		if token.WorkerID == nodeID && token.WorkerEpoch == workerEpoch {
			return true
		}
	}
	for _, replica := range assignment.ResultReplicas {
		if replica.PrimaryNodeID == nodeID && replica.PrimaryEpoch == workerEpoch || replica.SecondaryNodeID == nodeID && replica.SecondaryEpoch == workerEpoch {
			return true
		}
	}
	return false
}

func containsToken(set model.AssignmentSet, token model.AssignmentToken) bool {
	for _, v := range set.Tasks {
		if v == token {
			return true
		}
	}
	return false
}

func findReplica(set model.AssignmentSet, task model.TaskID) (model.ResultReplicaSet, bool) {
	for _, v := range set.ResultReplicas {
		if v.SinkTask == task {
			return v, true
		}
	}
	return model.ResultReplicaSet{}, false
}

func findToken(set model.AssignmentSet, task model.TaskID) (model.AssignmentToken, bool) {
	for _, token := range set.Tasks {
		if token.Task == task {
			return token, true
		}
	}
	return model.AssignmentToken{}, false
}

func equalInstalledAssignment(a, b InstalledAssignment) bool {
	return equalInstalledAssignmentContent(a, b) && a.SchedulingState == b.SchedulingState && a.CoordinatorEpoch == b.CoordinatorEpoch
}

func equalInstalledAssignmentContent(a, b InstalledAssignment) bool {
	return a.Assignment.JobID == b.Assignment.JobID && a.Assignment.Revision == b.Assignment.Revision && a.Assignment.Digest == b.Assignment.Digest && a.JobControlRevision == b.JobControlRevision && bytes.Equal(a.SpecificationBytes, b.SpecificationBytes) && equalTokens(a.Assignment.Tasks, b.Assignment.Tasks) && equalReplicas(a.Assignment.ResultReplicas, b.Assignment.ResultReplicas)
}

// admissionSchedulingProgression reports whether one scheduling change at an
// equal fence and revision is one of the two admitted worker-local admission
// progressions of the current coordinator's install protocol.
func admissionSchedulingProgression(prior, incoming model.SchedulingState) bool {
	return prior == model.Running && incoming == model.Closed || prior == model.Closed && incoming == model.Running
}

func equalTokens(a, b []model.AssignmentToken) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalReplicas(a, b []model.ResultReplicaSet) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
