package state

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const (
	estimatedSnapshotBaseBytes = model.StateCommandSnapshotBaseBytesV1
	clientHistoryFixedBytes    = model.StateCommandClientHistoryFixedV1
	subjectHistoryFixedBytes   = model.StateCommandSubjectHistoryFixedV1
)

type mutationPlan struct {
	result     []byte
	commit     func()
	reject     bool
	capacity   bool
	stateDelta int64
}

type clientHistory struct {
	sequence uint64
	digest   [32]byte
	result   []byte
}

type subjectHistory struct {
	revision uint64
	id       InternalCommandID
	digest   [32]byte
	target   []byte
	result   []byte
	applied  bool

	appliedTarget []byte
	appliedResult []byte
}

// Machine owns deterministic command deduplication state. Construction has no
// I/O, goroutine, time, or randomness side effect.
type Machine struct {
	mu sync.Mutex

	clients  map[model.ClientID]clientHistory
	subjects map[SubjectKey]subjectHistory

	coordinatorRevision uint64
	coordinatorEpoch    model.CoordinatorEpoch
	workers             map[uint16]WorkerRecord
	jobs                map[model.JobID]JobRecord

	estimatedSnapshotBytes uint64
}

func NewMachine() *Machine {
	return &Machine{
		clients: make(map[model.ClientID]clientHistory), subjects: make(map[SubjectKey]subjectHistory),
		workers: make(map[uint16]WorkerRecord), jobs: make(map[model.JobID]JobRecord),
		estimatedSnapshotBytes: estimatedSnapshotBaseBytes,
	}
}

// Apply decodes one concrete command and returns expected business rejection as
// an encoded CommandResult with a nil error.
func (machine *Machine) Apply(index, term uint64, encoded []byte) ([]byte, error) {
	if index == 0 || term == 0 {
		return nil, fmt.Errorf("%w: zero apply index or term", ErrMalformedCommand)
	}
	decoded, err := UnmarshalCommand(encoded)
	if err != nil {
		return nil, err
	}

	machine.mu.Lock()
	defer machine.mu.Unlock()
	switch command := decoded.(type) {
	case BeginCoordinatorEpoch:
		return machine.applyBeginCoordinatorLocked(index, term, command)
	case RegisterWorker:
		return machine.applyRegisterWorkerLocked(command)
	case DrainWorker:
		return machine.applyDrainWorkerLocked(command)
	case DeactivateWorker:
		return machine.applyDeactivateWorkerLocked(command)
	case ReplaceWorkerEpoch:
		return machine.applyReplaceWorkerEpochLocked(command)
	case SubmitJob:
		return machine.applySubmitJobLocked(command)
	case CancelJob:
		return machine.applyCancelJobLocked(command)
	default:
		return nil, errors.New("impossible decoded command type")
	}
}

func (machine *Machine) applyBeginCoordinatorLocked(index, term uint64, command BeginCoordinatorEpoch) ([]byte, error) {
	target := beginTarget(command)
	appliedTarget := beginAppliedTarget(term, command)
	return machine.applyInternalResolvedLocked(command.Envelope, target, appliedTarget, func(nextRevision uint64) (mutationPlan, error) {
		if machine.coordinatorRevision != nextRevision-1 {
			return mutationPlan{}, errors.New("impossible coordinator revision divergence")
		}
		current := machine.coordinatorEpoch
		if current != (model.CoordinatorEpoch{}) {
			if current.Term == term && current.Coordinator == command.Coordinator && current.Nonce == command.Nonce {
				result, err := MarshalCommandResult(CommandResult{Code: ResultSuccess, Subject: SubjectCoordinator, Revision: machine.coordinatorRevision, Epoch: current})
				return mutationPlan{result: result}, err
			}
			if term < current.Term || (term == current.Term && index <= current.BeginIndex) {
				result, resultErr := MarshalCommandResult(CommandResult{Code: ResultStaleEpoch, Subject: SubjectCoordinator, Revision: machine.coordinatorRevision, Epoch: current})
				return mutationPlan{result: result, reject: true}, resultErr
			}
		}
		epoch := model.CoordinatorEpoch{Term: term, BeginIndex: index, Coordinator: command.Coordinator, Nonce: command.Nonce}
		if err := epoch.Validate(); err != nil {
			return mutationPlan{}, fmt.Errorf("impossible coordinator epoch: %w", err)
		}
		result, err := MarshalCommandResult(CommandResult{Code: ResultSuccess, Subject: SubjectCoordinator, Revision: nextRevision, Epoch: epoch})
		if err != nil {
			return mutationPlan{}, err
		}
		return mutationPlan{result: result, commit: func() {
			machine.coordinatorRevision = nextRevision
			machine.coordinatorEpoch = epoch
		}}, nil
	})
}

func (machine *Machine) applyClientLocked(request model.ClientRequestID, digest [32]byte, prepare func() (mutationPlan, error)) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("invalid client request: %w", err)
	}
	if isZero32(digest) {
		return nil, errors.New("zero client command digest")
	}
	history, exists := machine.clients[request.ClientID]
	if exists {
		switch {
		case request.Sequence == history.sequence:
			if digest == history.digest {
				return owned(history.result), nil
			}
			return marshalBusinessResult(ResultIdentityReuse, SubjectKey{}, 0, model.CoordinatorEpoch{})
		case request.Sequence < history.sequence:
			return marshalBusinessResult(ResultStaleRequest, SubjectKey{}, 0, model.CoordinatorEpoch{})
		case history.sequence == math.MaxUint64 || request.Sequence > history.sequence+1:
			return marshalBusinessResult(ResultSkippedRequest, SubjectKey{}, 0, model.CoordinatorEpoch{})
		}
	} else {
		if request.Sequence != 1 {
			return marshalBusinessResult(ResultSkippedRequest, SubjectKey{}, 0, model.CoordinatorEpoch{})
		}
		if uint64(len(machine.clients)) >= model.StateCommandMaxClientSessionsV1 {
			return marshalBusinessResult(ResultCapacityExhausted, SubjectKey{}, 0, model.CoordinatorEpoch{})
		}
	}

	plan, err := prepare()
	if err != nil {
		return nil, err
	}
	if uint64(len(plan.result)) > model.StateCommandMaxCachedResultBytesV1 {
		plan = mutationPlan{result: mustBusinessResult(ResultResultTooLarge, SubjectKey{}, 0, model.CoordinatorEpoch{}), reject: true}
	}
	if plan.capacity {
		return owned(plan.result), nil
	}
	newSize, ok := machine.preflightClientHistoryLength(history, exists, uint64(len(plan.result)))
	if !ok {
		return marshalBusinessResult(ResultCapacityExhausted, SubjectKey{}, 0, model.CoordinatorEpoch{})
	}
	newSize, ok = preflightMutationDelta(newSize, plan.stateDelta)
	if !ok {
		return marshalBusinessResult(ResultCapacityExhausted, SubjectKey{}, 0, model.CoordinatorEpoch{})
	}
	newHistory := clientHistory{sequence: request.Sequence, digest: digest, result: owned(plan.result)}
	if plan.commit != nil && !plan.reject {
		plan.commit()
	}
	machine.clients[request.ClientID] = newHistory
	machine.estimatedSnapshotBytes = newSize
	return owned(newHistory.result), nil
}

func (machine *Machine) applyInternalLocked(envelope Envelope, target []byte, prepare func(uint64) (mutationPlan, error)) ([]byte, error) {
	return machine.applyInternalResolvedLocked(envelope, target, target, prepare)
}

func (machine *Machine) applyInternalResolvedLocked(envelope Envelope, target, appliedTarget []byte, prepare func(uint64) (mutationPlan, error)) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	if envelope.Internal == nil || envelope.Client != nil {
		return nil, errors.New("generic internal engine requires internal identity")
	}
	internal := envelope.Internal
	if internal.Digest != internalDigest(envelope, target) {
		return nil, ErrCommandDigestMismatch
	}
	history, exists := machine.subjects[internal.Subject]
	if exists && history.id == internal.ID {
		if history.digest == internal.Digest && bytes.Equal(history.target, target) {
			return owned(history.result), nil
		}
		return marshalBusinessResult(ResultIdentityReuse, internal.Subject, history.revision, machine.epochForSubject(internal.Subject, history.revision))
	}
	if exists && history.appliedTarget != nil && bytes.Equal(history.appliedTarget, appliedTarget) {
		newSize, ok := machine.preflightSubjectHistoryLengths(history, true, uint64(len(target)), uint64(len(history.appliedResult)), uint64(len(history.appliedTarget)), uint64(len(history.appliedResult)))
		if !ok {
			return marshalBusinessResult(ResultCapacityExhausted, internal.Subject, history.revision, machine.epochForSubject(internal.Subject, history.revision))
		}
		newHistory := subjectHistory{
			revision: history.revision, id: internal.ID, digest: internal.Digest,
			target: owned(target), result: owned(history.appliedResult), applied: true,
			appliedTarget: owned(history.appliedTarget), appliedResult: owned(history.appliedResult),
		}
		machine.subjects[internal.Subject] = newHistory
		machine.estimatedSnapshotBytes = newSize
		return owned(newHistory.result), nil
	}
	currentRevision := uint64(0)
	if exists {
		currentRevision = history.revision
	}
	if !exists && uint64(len(machine.subjects)) >= model.StateCommandMaxSubjectHistoriesV1 {
		return marshalBusinessResult(ResultCapacityExhausted, internal.Subject, 0, model.CoordinatorEpoch{})
	}
	if internal.ExpectedRevision != currentRevision {
		result, err := marshalBusinessResult(ResultRevisionMismatch, internal.Subject, currentRevision, machine.epochForSubject(internal.Subject, currentRevision))
		if err != nil {
			return nil, err
		}
		if !machine.cacheInternalOperation(internal, target, result, history, exists, false) {
			return marshalBusinessResult(ResultCapacityExhausted, internal.Subject, currentRevision, machine.epochForSubject(internal.Subject, currentRevision))
		}
		return result, nil
	}
	if currentRevision == math.MaxUint64 {
		return nil, errors.New("impossible subject revision overflow")
	}
	plan, err := prepare(currentRevision + 1)
	if err != nil {
		return nil, err
	}
	if uint64(len(plan.result)) > model.StateCommandMaxCachedResultBytesV1 {
		plan = mutationPlan{result: mustBusinessResult(ResultResultTooLarge, internal.Subject, currentRevision, machine.epochForSubject(internal.Subject, currentRevision)), reject: true}
	}
	if plan.capacity {
		return owned(plan.result), nil
	}
	appliedTargetLength, appliedResultLength := uint64(0), uint64(0)
	if exists {
		appliedTargetLength = uint64(len(history.appliedTarget))
		appliedResultLength = uint64(len(history.appliedResult))
	}
	if !plan.reject {
		appliedTargetLength = uint64(len(appliedTarget))
		appliedResultLength = uint64(len(plan.result))
	}
	newSize, ok := machine.preflightSubjectHistoryLengths(history, exists, uint64(len(target)), uint64(len(plan.result)), appliedTargetLength, appliedResultLength)
	if !ok {
		return marshalBusinessResult(ResultCapacityExhausted, internal.Subject, currentRevision, machine.epochForSubject(internal.Subject, currentRevision))
	}
	newSize, ok = preflightMutationDelta(newSize, plan.stateDelta)
	if !ok {
		return marshalBusinessResult(ResultCapacityExhausted, internal.Subject, currentRevision, machine.epochForSubject(internal.Subject, currentRevision))
	}
	newHistory := subjectHistory{
		revision: currentRevision, id: internal.ID, digest: internal.Digest,
		target: owned(target), result: owned(plan.result), applied: !plan.reject,
	}
	if exists {
		newHistory.appliedTarget = owned(history.appliedTarget)
		newHistory.appliedResult = owned(history.appliedResult)
	}
	if !plan.reject {
		newHistory.revision = currentRevision + 1
		newHistory.appliedTarget = owned(appliedTarget)
		newHistory.appliedResult = owned(plan.result)
	}
	if plan.commit != nil && !plan.reject {
		plan.commit()
	}
	machine.subjects[internal.Subject] = newHistory
	machine.estimatedSnapshotBytes = newSize
	return owned(newHistory.result), nil
}

func (machine *Machine) cacheInternalOperation(internal *InternalEnvelope, target, result []byte, old subjectHistory, exists, applied bool) bool {
	if !exists && uint64(len(machine.subjects)) >= model.StateCommandMaxSubjectHistoriesV1 {
		return false
	}
	newSize, ok := machine.preflightSubjectHistoryLengths(old, exists, uint64(len(target)), uint64(len(result)), uint64(len(old.appliedTarget)), uint64(len(old.appliedResult)))
	if !ok {
		return false
	}
	history := subjectHistory{
		revision: old.revision, id: internal.ID, digest: internal.Digest,
		target: owned(target), result: owned(result), applied: applied,
		appliedTarget: owned(old.appliedTarget), appliedResult: owned(old.appliedResult),
	}
	machine.subjects[internal.Subject] = history
	machine.estimatedSnapshotBytes = newSize
	return true
}

func (machine *Machine) epochForSubject(subject SubjectKey, revision uint64) model.CoordinatorEpoch {
	if subject.Kind == SubjectCoordinator && revision != 0 {
		return machine.coordinatorEpoch
	}
	return model.CoordinatorEpoch{}
}

func (machine *Machine) preflightClientHistoryLength(old clientHistory, exists bool, resultLength uint64) (uint64, bool) {
	current := machine.estimatedSnapshotBytes
	if exists {
		oldSize, ok := clientHistoryEstimatedBytes(old)
		if !ok || oldSize > current {
			return 0, false
		}
		current -= oldSize
	}
	nextSize, ok := checkedAddMany(clientHistoryFixedBytes, resultLength)
	if !ok {
		return 0, false
	}
	return checkedSnapshotAdd(current, nextSize)
}

func (machine *Machine) preflightSubjectHistoryLengths(old subjectHistory, exists bool, targetLength, resultLength, appliedTargetLength, appliedResultLength uint64) (uint64, bool) {
	current := machine.estimatedSnapshotBytes
	if exists {
		oldSize, ok := subjectHistoryEstimatedBytes(old)
		if !ok || oldSize > current {
			return 0, false
		}
		current -= oldSize
	}
	nextSize, ok := checkedAddMany(subjectHistoryFixedBytes, targetLength, resultLength, appliedTargetLength, appliedResultLength)
	if !ok {
		return 0, false
	}
	return checkedSnapshotAdd(current, nextSize)
}

func clientHistoryEstimatedBytes(history clientHistory) (uint64, bool) {
	return checkedAddMany(clientHistoryFixedBytes, uint64(len(history.result)))
}

func subjectHistoryEstimatedBytes(history subjectHistory) (uint64, bool) {
	return checkedAddMany(subjectHistoryFixedBytes, uint64(len(history.target)), uint64(len(history.result)), uint64(len(history.appliedTarget)), uint64(len(history.appliedResult)))
}

func checkedAddMany(values ...uint64) (uint64, bool) {
	var total uint64
	for _, value := range values {
		if total > math.MaxUint64-value {
			return 0, false
		}
		total += value
	}
	return total, true
}

func checkedSnapshotAdd(left, right uint64) (uint64, bool) {
	total, ok := checkedAddMany(left, right)
	return total, ok && total <= model.StateCommandMaxSnapshotBytesV1
}

func preflightMutationDelta(current uint64, delta int64) (uint64, bool) {
	if delta < 0 {
		magnitude := uint64(-(delta + 1)) + 1
		if magnitude > current {
			return 0, false
		}
		return current - magnitude, true
	}
	return checkedSnapshotAdd(current, uint64(delta))
}

func marshalBusinessResult(code ResultCode, subject SubjectKey, revision uint64, epoch model.CoordinatorEpoch) ([]byte, error) {
	result := CommandResult{Code: code, Subject: subject.Kind, Revision: revision, Epoch: epoch}
	switch subject.Kind {
	case SubjectWorker:
		result.WorkerID = subject.WorkerID
	case SubjectJobControl, SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest:
		result.JobID = subject.JobID
	}
	return MarshalCommandResult(result)
}

func mustBusinessResult(code ResultCode, subject SubjectKey, revision uint64, epoch model.CoordinatorEpoch) []byte {
	result, err := marshalBusinessResult(code, subject, revision, epoch)
	if err != nil {
		panic(err)
	}
	return result
}

func owned(input []byte) []byte { return append([]byte(nil), input...) }
