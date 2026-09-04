package state

import (
	"sort"

	"crane/internal/crane/model"
)

// ClientHistoryView is one owned public-command retry record.
type ClientHistoryView struct {
	ClientID model.ClientID // ClientID identifies the retained client session.
	Sequence uint64         // Sequence is the last accepted request sequence.
	Digest   [32]byte       // Digest binds the last accepted logical command.
	Result   []byte         // Result is an owned copy of the cached deterministic result.
}

// SubjectHistoryView is one owned internal-command retry and revision record.
type SubjectHistoryView struct {
	Subject         SubjectKey        // Subject identifies the independently revised state.
	Revision        uint64            // Revision is the authoritative subject revision.
	ID              InternalCommandID // ID is the most recently accepted operation identity.
	Digest          [32]byte          // Digest binds that operation's defining bytes.
	Target          []byte            // Target owns the most recently accepted target bytes.
	Result          []byte            // Result owns the most recently cached result bytes.
	Applied         bool              // Applied reports whether the most recent operation mutated state.
	AppliedRevision uint64            // AppliedRevision is the revision that produced AppliedResult.
	AppliedTarget   []byte            // AppliedTarget owns the last successfully applied target.
	AppliedResult   []byte            // AppliedResult owns the last successfully applied result.
}

// WorkerEventView is one owned globally ordered worker-incarnation cursor.
type WorkerEventView struct {
	WorkerID      uint16            // WorkerID identifies the reporting worker.
	WorkerEpoch   model.WorkerEpoch // WorkerEpoch binds the exact reporting incarnation.
	TransactionID uint64            // TransactionID is the greatest committed worker transaction.
	Digest        [32]byte          // Digest binds that transaction's exact event.
}

// View is one atomic, independently owned read of all replicated Crane state.
// Callers may mutate the returned copies without affecting the live machine.
type View struct {
	AppliedIndex           uint64                 // AppliedIndex is the last successfully applied Crane command index.
	CoordinatorRevision    uint64                 // CoordinatorRevision is the coordinator subject revision.
	CoordinatorEpoch       model.CoordinatorEpoch // CoordinatorEpoch is the current leadership fence.
	Clients                []ClientHistoryView    // Clients are sorted by ClientID.
	Subjects               []SubjectHistoryView   // Subjects are sorted by canonical SubjectKey bytes.
	Workers                []WorkerRecord         // Workers are sorted by NodeID.
	Jobs                   []JobRecord            // Jobs are sorted by JobID and deep-own all nested values.
	WorkerEvents           []WorkerEventView      // WorkerEvents are sorted by WorkerID and WorkerEpoch.
	EstimatedSnapshotBytes uint64                 // EstimatedSnapshotBytes is the exact canonical payload size.
}

// View returns one atomic deep-owned snapshot of the current replicated state.
func (machine *Machine) View() View {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	estimated, _ := machine.estimateCanonicalSnapshotBytesLocked()

	view := View{
		AppliedIndex: machine.lastAppliedIndex, CoordinatorRevision: machine.coordinatorRevision,
		CoordinatorEpoch: machine.coordinatorEpoch, EstimatedSnapshotBytes: estimated,
		Clients: make([]ClientHistoryView, 0, len(machine.clients)), Subjects: make([]SubjectHistoryView, 0, len(machine.subjects)),
		Workers: make([]WorkerRecord, 0, len(machine.workers)), Jobs: make([]JobRecord, 0, len(machine.jobs)),
		WorkerEvents: make([]WorkerEventView, 0, len(machine.workerEvents)),
	}
	for id, history := range machine.clients {
		view.Clients = append(view.Clients, ClientHistoryView{ClientID: id, Sequence: history.sequence, Digest: history.digest, Result: owned(history.result)})
	}
	sort.Slice(view.Clients, func(i, j int) bool {
		return compareSnapshotClientID(view.Clients[i].ClientID, view.Clients[j].ClientID) < 0
	})
	for subject, history := range machine.subjects {
		view.Subjects = append(view.Subjects, SubjectHistoryView{
			Subject: subject, Revision: history.revision, ID: history.id, Digest: history.digest,
			Target: owned(history.target), Result: owned(history.result), Applied: history.applied,
			AppliedRevision: history.appliedRevision, AppliedTarget: owned(history.appliedTarget), AppliedResult: owned(history.appliedResult),
		})
	}
	sort.Slice(view.Subjects, func(i, j int) bool {
		return compareSnapshotSubject(view.Subjects[i].Subject, view.Subjects[j].Subject) < 0
	})
	for _, worker := range machine.workers {
		view.Workers = append(view.Workers, worker)
	}
	sort.Slice(view.Workers, func(i, j int) bool {
		return compareSnapshotWorkerID(view.Workers[i].NodeID, view.Workers[j].NodeID) < 0
	})
	for _, job := range machine.jobs {
		view.Jobs = append(view.Jobs, cloneJobRecord(job))
	}
	sort.Slice(view.Jobs, func(i, j int) bool { return compareSnapshotJobID(view.Jobs[i].JobID, view.Jobs[j].JobID) < 0 })
	for key, cursor := range machine.workerEvents {
		view.WorkerEvents = append(view.WorkerEvents, WorkerEventView{WorkerID: key.WorkerID, WorkerEpoch: key.WorkerEpoch, TransactionID: cursor.TransactionID, Digest: cursor.Digest})
	}
	sort.Slice(view.WorkerEvents, func(i, j int) bool {
		left := workerEventKey{WorkerID: view.WorkerEvents[i].WorkerID, WorkerEpoch: view.WorkerEvents[i].WorkerEpoch}
		right := workerEventKey{WorkerID: view.WorkerEvents[j].WorkerID, WorkerEpoch: view.WorkerEvents[j].WorkerEpoch}
		return compareSnapshotWorkerEvent(left, right) < 0
	})
	return view
}
