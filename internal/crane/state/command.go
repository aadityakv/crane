package state

import (
	"errors"
	"fmt"

	"crane/internal/crane/model"
)

// CommandSchemaVersion is the only accepted canonical state-command schema.
const CommandSchemaVersion = model.StateCommandSchemaVersionV1

const (
	commandFixedEnvelopeBytes = model.StateCommandFixedEnvelopeBytesV1
	clientEnvelopeBytes       = model.StateCommandClientEnvelopeBytesV1
	internalEnvelopeBytes     = model.StateCommandInternalEnvelopeBytesV1
	subjectKeyBytes           = model.StateCommandSubjectKeyBytesV1
	beginTargetBytes          = model.StateCommandBeginTargetBytesV1
	commandResultBytes        = model.StateCommandCommandResultBytesV1
)

const internalCommandDigestDomain = "crane/internal-command/v1\x00"

// CommandKind selects one concrete, non-opaque replicated transition.
type CommandKind uint16

const (
	CommandBeginCoordinatorEpoch CommandKind = 1  // CommandBeginCoordinatorEpoch creates a leadership fence.
	CommandRegisterWorker        CommandKind = 2  // CommandRegisterWorker creates or revives a worker.
	CommandDrainWorker           CommandKind = 3  // CommandDrainWorker disables new placement.
	CommandDeactivateWorker      CommandKind = 4  // CommandDeactivateWorker invalidates an incarnation.
	CommandReplaceWorkerEpoch    CommandKind = 5  // CommandReplaceWorkerEpoch changes an incarnation.
	CommandSubmitJob             CommandKind = 6  // CommandSubmitJob creates an immutable job.
	CommandCancelJob             CommandKind = 7  // CommandCancelJob terminally cancels a job.
	CommandRecordSourceEOF       CommandKind = 8  // CommandRecordSourceEOF commits one source bound.
	CommandInstallAssignments    CommandKind = 9  // CommandInstallAssignments commits initial placement.
	CommandReplaceAssignments    CommandKind = 10 // CommandReplaceAssignments resolves invalidated targets.
	CommandAdvanceCheckpoint     CommandKind = 11 // CommandAdvanceCheckpoint commits worker progress.
	CommandSealManifest          CommandKind = 12 // CommandSealManifest commits one result artifact.
	CommandTransitionJob         CommandKind = 13 // CommandTransitionJob advances normal lifecycle.
	CommandFailJob               CommandKind = 14 // CommandFailJob commits terminal failure.
)

// InternalCommandID is the stable deduplication identity of one coordinator command.
type InternalCommandID [32]byte

// SubjectKind selects the independently revisioned replicated subject.
type SubjectKind uint8

const (
	SubjectNone             SubjectKind = 0 // SubjectNone is reserved for unbound results.
	SubjectCoordinator      SubjectKind = 1 // SubjectCoordinator revisions leadership fences.
	SubjectWorker           SubjectKind = 2 // SubjectWorker revisions one node record.
	SubjectJobControl       SubjectKind = 3 // SubjectJobControl revisions job-wide control.
	SubjectSourceEOF        SubjectKind = 4 // SubjectSourceEOF revisions one source bound.
	SubjectSourceCheckpoint SubjectKind = 5 // SubjectSourceCheckpoint revisions source progress.
	SubjectResultManifest   SubjectKind = 6 // SubjectResultManifest revisions one sink artifact.
)

// SubjectKey is the canonical tagged union identifying one revision history.
type SubjectKey struct {
	Kind     SubjectKind  // Kind selects the active union member.
	JobID    model.JobID  // JobID identifies job-scoped subjects.
	TaskID   model.TaskID // TaskID identifies source or sink scoped subjects.
	WorkerID uint16       // WorkerID identifies worker-scoped subjects.
}

func (key SubjectKey) Validate() error {
	zeroJob := model.JobID{}
	zeroTask := model.TaskID{}
	switch key.Kind {
	case SubjectCoordinator:
		if key.JobID != zeroJob || key.TaskID != zeroTask || key.WorkerID != 0 {
			return fmt.Errorf("%w: coordinator subject has nonzero union field", ErrInvalidCommandSubject)
		}
	case SubjectWorker:
		if key.WorkerID == 0 || key.JobID != zeroJob || key.TaskID != zeroTask {
			return fmt.Errorf("%w: worker subject union is noncanonical", ErrInvalidCommandSubject)
		}
	case SubjectJobControl:
		if key.JobID.Validate() != nil || key.TaskID != zeroTask || key.WorkerID != 0 {
			return fmt.Errorf("%w: job-control subject union is noncanonical", ErrInvalidCommandSubject)
		}
	case SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest:
		if key.JobID.Validate() != nil || key.TaskID.Validate() != nil || key.TaskID.JobID != key.JobID || key.WorkerID != 0 {
			return fmt.Errorf("%w: task subject union is noncanonical", ErrInvalidCommandSubject)
		}
	default:
		return fmt.Errorf("%w: unknown subject kind", ErrInvalidCommandSubject)
	}
	return nil
}

// ClientEnvelope carries public request identity and its logical digest.
type ClientEnvelope struct {
	Request model.ClientRequestID // Request provides client/session deduplication identity.
	Digest  [32]byte              // Digest binds the logical public operation.
}

// InternalEnvelope carries exact coordinator identity and subject fencing.
type InternalEnvelope struct {
	ID               InternalCommandID // ID uniquely identifies the coordinator operation.
	Digest           [32]byte          // Digest binds the fence, subject, revision, and target.
	Subject          SubjectKey        // Subject selects the independent history.
	ExpectedRevision uint64            // ExpectedRevision conditionally fences the transition.
}

// Envelope is the common schema, compatibility, coordinator, and identity fence.
type Envelope struct {
	SchemaVersion        uint16                 // SchemaVersion rejects unknown encodings.
	ConsensusFingerprint [32]byte               // ConsensusFingerprint rejects mixed contracts.
	Kind                 CommandKind            // Kind selects the concrete target layout.
	CoordinatorEpoch     model.CoordinatorEpoch // CoordinatorEpoch fences every non-begin mutation.
	Client               *ClientEnvelope        // Client is set only for public operations.
	Internal             *InternalEnvelope      // Internal is set only for coordinator operations.
}

func (envelope Envelope) Validate() error {
	if envelope.SchemaVersion != CommandSchemaVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedCommandSchema, envelope.SchemaVersion)
	}
	if envelope.ConsensusFingerprint == ([32]byte{}) || envelope.ConsensusFingerprint != model.ConsensusFingerprint() {
		return ErrConsensusFingerprintMismatch
	}
	if envelope.Kind < CommandBeginCoordinatorEpoch || envelope.Kind > CommandFailJob {
		return fmt.Errorf("%w: %d", ErrUnknownCommandKind, envelope.Kind)
	}
	if envelope.Kind == CommandBeginCoordinatorEpoch {
		if envelope.CoordinatorEpoch != (model.CoordinatorEpoch{}) {
			return errors.New("begin coordinator epoch carries a preexisting fence")
		}
	} else if err := envelope.CoordinatorEpoch.Validate(); err != nil {
		return fmt.Errorf("command coordinator fence: %w", err)
	}
	if (envelope.Client == nil) == (envelope.Internal == nil) {
		return errors.New("command must have exactly one identity")
	}
	if envelope.Client != nil {
		if err := envelope.Client.Request.Validate(); err != nil {
			return fmt.Errorf("client request: %w", err)
		}
		if isZero32(envelope.Client.Digest) {
			return errors.New("zero client command digest")
		}
	}
	if envelope.Internal != nil {
		if envelope.Internal.ID == (InternalCommandID{}) {
			return errors.New("zero internal command ID")
		}
		if isZero32(envelope.Internal.Digest) {
			return errors.New("zero internal command digest")
		}
		if err := envelope.Internal.Subject.Validate(); err != nil {
			return fmt.Errorf("internal subject: %w", err)
		}
	}
	return nil
}

// BeginCoordinatorEpoch commits the sole constructor of a coordinator fence.
type BeginCoordinatorEpoch struct {
	Envelope    Envelope // Envelope carries the coordinator singleton subject.
	Coordinator uint16   // Coordinator is the elected node ID.
	Nonce       [16]byte // Nonce distinguishes leadership at the same node.
}

func (command BeginCoordinatorEpoch) Validate() error {
	if err := command.Envelope.Validate(); err != nil {
		return err
	}
	if command.Envelope.Kind != CommandBeginCoordinatorEpoch || command.Envelope.Internal == nil || command.Envelope.Client != nil {
		return errors.New("begin coordinator epoch requires internal identity")
	}
	if command.Envelope.Internal.Subject != (SubjectKey{Kind: SubjectCoordinator}) {
		return fmt.Errorf("%w: begin coordinator epoch requires coordinator subject", ErrInvalidCommandSubject)
	}
	if command.Coordinator == 0 {
		return errors.New("zero coordinator")
	}
	if command.Nonce == ([16]byte{}) {
		return errors.New("zero coordinator nonce")
	}
	target := beginTarget(command)
	if command.Envelope.Internal.Digest != internalDigest(command.Envelope, target) {
		return ErrCommandDigestMismatch
	}
	return nil
}

// NewBeginCoordinatorEpoch constructs one complete digest-bound internal command.
func NewBeginCoordinatorEpoch(id InternalCommandID, expectedRevision uint64, coordinator uint16, nonce [16]byte) (BeginCoordinatorEpoch, error) {
	command := BeginCoordinatorEpoch{
		Envelope: Envelope{
			SchemaVersion: CommandSchemaVersion, ConsensusFingerprint: model.ConsensusFingerprint(),
			Kind:     CommandBeginCoordinatorEpoch,
			Internal: &InternalEnvelope{ID: id, Subject: SubjectKey{Kind: SubjectCoordinator}, ExpectedRevision: expectedRevision},
		},
		Coordinator: coordinator, Nonce: nonce,
	}
	command.Envelope.Internal.Digest = internalDigest(command.Envelope, beginTarget(command))
	if err := command.Validate(); err != nil {
		return BeginCoordinatorEpoch{}, err
	}
	return command, nil
}

// ResultCode is the stable replicated business-result category.
type ResultCode uint16

const (
	ResultSuccess           ResultCode = 1 // ResultSuccess reports a committed or exact replayed success.
	ResultIdentityReuse     ResultCode = 2 // ResultIdentityReuse reports changed bytes under one identity.
	ResultStaleRequest      ResultCode = 3 // ResultStaleRequest reports an older client sequence.
	ResultSkippedRequest    ResultCode = 4 // ResultSkippedRequest reports a client sequence gap.
	ResultCapacityExhausted ResultCode = 5 // ResultCapacityExhausted is stateless and retryable.
	ResultRevisionMismatch  ResultCode = 6 // ResultRevisionMismatch reports a failed condition.
	ResultStaleEpoch        ResultCode = 7 // ResultStaleEpoch reports the current coordinator fence.
	ResultResultTooLarge    ResultCode = 8 // ResultResultTooLarge reports an uncacheable outcome.
	// State-specific names map onto the stable v1 result categories.
	ResultNotFound          ResultCode = ResultRevisionMismatch
	ResultInvalidTransition ResultCode = ResultRevisionMismatch
	ResultIdentityCollision ResultCode = ResultIdentityReuse
	ResultInvalidTarget     ResultCode = ResultRevisionMismatch
	ResultStaleWorkerEvent  ResultCode = ResultRevisionMismatch
)

// CommandResult is the canonical cached outcome of one accepted operation.
type CommandResult struct {
	Code     ResultCode             // Code reports success or deterministic rejection.
	Subject  SubjectKind            // Subject identifies the affected revision domain.
	Revision uint64                 // Revision reports the authoritative subject revision.
	JobID    model.JobID            // JobID correlates job-scoped results.
	WorkerID uint16                 // WorkerID correlates worker-scoped results.
	Epoch    model.CoordinatorEpoch // Epoch reports coordinator success or the current stale fence.
}

func (result CommandResult) Validate() error {
	if result.Code < ResultSuccess || result.Code > ResultResultTooLarge {
		return errors.New("unknown result code")
	}
	rule, ok := model.StateCommandResultRuleV1(uint16(result.Code), uint8(result.Subject))
	if !ok {
		return fmt.Errorf("%w: result code %d does not permit subject %d", ErrInvalidCommandSubject, result.Code, result.Subject)
	}
	switch rule.Revision {
	case model.StateCommandRevisionZero:
		if result.Revision != 0 {
			return errors.New("result revision must be zero")
		}
	case model.StateCommandRevisionNonZero:
		if result.Revision == 0 {
			return errors.New("result revision must be nonzero")
		}
	case model.StateCommandRevisionAny:
	default:
		return errors.New("impossible result revision policy")
	}
	zeroJob := model.JobID{}
	zeroEpoch := model.CoordinatorEpoch{}
	switch rule.Identity {
	case model.StateCommandIdentityUnbound, model.StateCommandIdentityCoordinator:
		if result.JobID != zeroJob || result.WorkerID != 0 {
			return fmt.Errorf("%w: unbound/coordinator result has nonzero identity field", ErrInvalidCommandSubject)
		}
	case model.StateCommandIdentityWorker:
		if result.WorkerID == 0 || result.JobID != zeroJob {
			return fmt.Errorf("%w: worker result identity is noncanonical", ErrInvalidCommandSubject)
		}
	case model.StateCommandIdentityJob:
		if result.JobID.Validate() != nil || result.WorkerID != 0 {
			return fmt.Errorf("%w: job result identity is noncanonical", ErrInvalidCommandSubject)
		}
	default:
		return errors.New("impossible result identity policy")
	}
	switch rule.Epoch {
	case model.StateCommandEpochZero:
		if result.Epoch != zeroEpoch {
			return fmt.Errorf("%w: result requires zero coordinator epoch", ErrInvalidCommandSubject)
		}
	case model.StateCommandEpochCoordinatorRevision:
		if result.Revision == 0 {
			if result.Epoch != zeroEpoch {
				return fmt.Errorf("%w: zero-revision coordinator result has epoch", ErrInvalidCommandSubject)
			}
		} else if err := result.Epoch.Validate(); err != nil {
			return fmt.Errorf("%w: coordinator result epoch: %v", ErrInvalidCommandSubject, err)
		}
	case model.StateCommandEpochCurrentFence:
		if result.Epoch != zeroEpoch {
			if err := result.Epoch.Validate(); err != nil {
				return fmt.Errorf("%w: stale result coordinator epoch: %v", ErrInvalidCommandSubject, err)
			}
		}
	default:
		return errors.New("impossible result epoch policy")
	}
	return nil
}

func isZero32(value [32]byte) bool { return value == ([32]byte{}) }
