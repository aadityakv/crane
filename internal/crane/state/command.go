package state

import (
	"errors"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
)

const CommandSchemaVersion = model.StateCommandSchemaVersionV1

const (
	commandFixedEnvelopeBytes = model.StateCommandFixedEnvelopeBytesV1
	clientEnvelopeBytes       = model.StateCommandClientEnvelopeBytesV1
	internalEnvelopeBytes     = model.StateCommandInternalEnvelopeBytesV1
	subjectKeyBytes           = model.StateCommandSubjectKeyBytesV1
	beginTargetBytes          = model.StateCommandBeginTargetBytesV1
	commandResultBytes        = model.StateCommandCommandResultBytesV1
)

const internalCommandDigestDomain = "cs425/crane/internal-command/v1\x00"

type CommandKind uint16

const (
	CommandBeginCoordinatorEpoch CommandKind = 1
	CommandRegisterWorker        CommandKind = 2
	CommandDrainWorker           CommandKind = 3
	CommandDeactivateWorker      CommandKind = 4
	CommandReplaceWorkerEpoch    CommandKind = 5
	CommandSubmitJob             CommandKind = 6
	CommandCancelJob             CommandKind = 7
	CommandRecordSourceEOF       CommandKind = 8
	CommandInstallAssignments    CommandKind = 9
	CommandReplaceAssignments    CommandKind = 10
	CommandAdvanceCheckpoint     CommandKind = 11
	CommandSealManifest          CommandKind = 12
	CommandTransitionJob         CommandKind = 13
	CommandFailJob               CommandKind = 14
)

type InternalCommandID [32]byte

type SubjectKind uint8

const (
	SubjectNone             SubjectKind = 0
	SubjectCoordinator      SubjectKind = 1
	SubjectWorker           SubjectKind = 2
	SubjectJobControl       SubjectKind = 3
	SubjectSourceEOF        SubjectKind = 4
	SubjectSourceCheckpoint SubjectKind = 5
	SubjectResultManifest   SubjectKind = 6
)

type SubjectKey struct {
	Kind     SubjectKind
	JobID    model.JobID
	TaskID   model.TaskID
	WorkerID uint16
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

type ClientEnvelope struct {
	Request model.ClientRequestID
	Digest  [32]byte
}

type InternalEnvelope struct {
	ID               InternalCommandID
	Digest           [32]byte
	Subject          SubjectKey
	ExpectedRevision uint64
}

type Envelope struct {
	SchemaVersion        uint16
	ConsensusFingerprint [32]byte
	Kind                 CommandKind
	Client               *ClientEnvelope
	Internal             *InternalEnvelope
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

type BeginCoordinatorEpoch struct {
	Envelope    Envelope
	Coordinator uint16
	Nonce       [16]byte
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

type ResultCode uint16

const (
	ResultSuccess           ResultCode = 1
	ResultIdentityReuse     ResultCode = 2
	ResultStaleRequest      ResultCode = 3
	ResultSkippedRequest    ResultCode = 4
	ResultCapacityExhausted ResultCode = 5
	ResultRevisionMismatch  ResultCode = 6
	ResultStaleEpoch        ResultCode = 7
	ResultResultTooLarge    ResultCode = 8
	// State-specific names map onto the stable v1 result categories.
	ResultNotFound          ResultCode = ResultRevisionMismatch
	ResultInvalidTransition ResultCode = ResultRevisionMismatch
	ResultIdentityCollision ResultCode = ResultIdentityReuse
	ResultInvalidTarget     ResultCode = ResultRevisionMismatch
	ResultStaleWorkerEvent  ResultCode = ResultRevisionMismatch
)

type CommandResult struct {
	Code     ResultCode
	Subject  SubjectKind
	Revision uint64
	JobID    model.JobID
	WorkerID uint16
	Epoch    model.CoordinatorEpoch
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
	default:
		return errors.New("impossible result epoch policy")
	}
	return nil
}

func isZero32(value [32]byte) bool { return value == ([32]byte{}) }
