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
			return errors.New("coordinator subject has nonzero union field")
		}
	case SubjectWorker:
		if key.WorkerID == 0 || key.JobID != zeroJob || key.TaskID != zeroTask {
			return errors.New("worker subject union is noncanonical")
		}
	case SubjectJobControl:
		if key.JobID.Validate() != nil || key.TaskID != zeroTask || key.WorkerID != 0 {
			return errors.New("job-control subject union is noncanonical")
		}
	case SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest:
		if key.JobID.Validate() != nil || key.TaskID.Validate() != nil || key.TaskID.JobID != key.JobID || key.WorkerID != 0 {
			return errors.New("task subject union is noncanonical")
		}
	default:
		return errors.New("unknown subject kind")
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
	if envelope.Kind != CommandBeginCoordinatorEpoch {
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
		return errors.New("begin coordinator epoch requires coordinator subject")
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
	zeroJob := model.JobID{}
	zeroEpoch := model.CoordinatorEpoch{}
	if result.Subject == SubjectNone {
		if result.Code == ResultSuccess || result.Revision != 0 || result.JobID != zeroJob || result.WorkerID != 0 || result.Epoch != zeroEpoch {
			return errors.New("noncanonical unbound result")
		}
		return nil
	}
	if result.Code == ResultSuccess && result.Revision == 0 {
		return errors.New("success result has zero revision")
	}
	switch result.Subject {
	case SubjectCoordinator:
		if result.JobID != zeroJob || result.WorkerID != 0 {
			return errors.New("coordinator result has nonzero union field")
		}
		if result.Revision == 0 {
			if result.Epoch != zeroEpoch {
				return errors.New("zero-revision coordinator result has epoch")
			}
		} else if err := result.Epoch.Validate(); err != nil {
			return fmt.Errorf("coordinator result epoch: %w", err)
		}
	case SubjectWorker:
		if result.WorkerID == 0 || result.JobID != zeroJob || result.Epoch != zeroEpoch {
			return errors.New("worker result union is noncanonical")
		}
	case SubjectJobControl, SubjectSourceEOF, SubjectSourceCheckpoint, SubjectResultManifest:
		if result.JobID.Validate() != nil || result.WorkerID != 0 || result.Epoch != zeroEpoch {
			return errors.New("job result union is noncanonical")
		}
	default:
		return errors.New("unknown result subject")
	}
	return nil
}

func isZero32(value [32]byte) bool { return value == ([32]byte{}) }
