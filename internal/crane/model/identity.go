package model

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
)

// ErrIdentityReuse reports one existing identity presented with different
// canonical defining bytes.
var ErrIdentityReuse = errors.New("identity reuse with different defining bytes")

const (
	jobIDDomain         = "crane/job/v1\x00"
	sourceTupleIDDomain = "crane/source-tuple/v1\x00"
	childTupleIDDomain  = "crane/derived-tuple/v1\x00"
)

// ClientID identifies a submitter independently of an individual request.
type ClientID [16]byte

// ClientRequestID identifies one sequenced request from a client.
type ClientRequestID struct {
	ClientID ClientID
	Sequence uint64
}

// JobID is the stable, deterministic identifier for one submitted topology.
type JobID [16]byte

// TaskID identifies one zero-based partition of a stage in a job.
type TaskID struct {
	JobID     JobID
	StageID   uint16
	Partition uint16
}

// WorkerEpoch fences a worker incarnation.
type WorkerEpoch [16]byte

// CoordinatorEpoch fences coordinator leadership.
type CoordinatorEpoch struct {
	Term        uint64
	BeginIndex  uint64
	Coordinator uint16
	Nonce       [16]byte
}

// AssignmentToken identifies one assignment revision and attempt.
type AssignmentToken struct {
	Task               TaskID
	WorkerID           uint16
	WorkerEpoch        WorkerEpoch
	Attempt            uint64
	SpecificationHash  [32]byte
	AssignmentRevision uint64
}

// TupleID identifies a source tuple and its deterministic derivation path.
type TupleID struct {
	JobID          JobID
	SourceTask     TaskID
	SourceSequence uint64
	PathDigest     [32]byte
}

// DeliveryID identifies custody of a logical tuple across one edge.
type DeliveryID struct {
	Tuple           TupleID
	EdgeID          uint16
	DestinationTask TaskID
}

// Validate rejects an uninitialized client identity.
func (id ClientID) Validate() error {
	if isZero16([16]byte(id)) {
		return errors.New("zero client ID")
	}
	return nil
}

// Validate rejects an uninitialized request identity.
func (id ClientRequestID) Validate() error {
	if err := id.ClientID.Validate(); err != nil {
		return err
	}
	if id.Sequence == 0 {
		return errors.New("zero client request sequence")
	}
	return nil
}

// Validate rejects an uninitialized job identity.
func (id JobID) Validate() error {
	if isZero16([16]byte(id)) {
		return errors.New("zero job ID")
	}
	return nil
}

// Validate rejects a task without a job or stage.
func (id TaskID) Validate() error {
	if err := id.JobID.Validate(); err != nil {
		return err
	}
	if id.StageID == 0 {
		return errors.New("zero task stage ID")
	}
	return nil
}

// Validate rejects an uninitialized worker epoch.
func (epoch WorkerEpoch) Validate() error {
	if isZero16([16]byte(epoch)) {
		return errors.New("zero worker epoch")
	}
	return nil
}

// Validate rejects an uninitialized coordinator epoch.
func (epoch CoordinatorEpoch) Validate() error {
	if epoch.Term == 0 || epoch.BeginIndex == 0 || epoch.Coordinator == 0 || isZero16(epoch.Nonce) {
		return errors.New("zero coordinator epoch component")
	}
	return nil
}

// Validate rejects an incomplete assignment token.
func (token AssignmentToken) Validate() error {
	if err := token.Task.Validate(); err != nil {
		return fmt.Errorf("assignment task: %w", err)
	}
	if token.WorkerID == 0 {
		return errors.New("zero assignment worker ID")
	}
	if err := token.WorkerEpoch.Validate(); err != nil {
		return fmt.Errorf("assignment worker epoch: %w", err)
	}
	if token.Attempt == 0 {
		return errors.New("zero assignment attempt")
	}
	if token.AssignmentRevision == 0 {
		return errors.New("zero assignment revision")
	}
	return nil
}

// Validate rejects incomplete tuples and cross-job embedded identities.
func (id TupleID) Validate() error {
	if err := id.JobID.Validate(); err != nil {
		return err
	}
	if err := id.SourceTask.Validate(); err != nil {
		return fmt.Errorf("tuple source task: %w", err)
	}
	if id.SourceTask.JobID != id.JobID {
		return errors.New("tuple source task job ID mismatch")
	}
	if id.SourceSequence == 0 {
		return errors.New("zero source sequence")
	}
	if isZero32(id.PathDigest) {
		return errors.New("zero tuple path digest")
	}
	return nil
}

// Validate rejects incomplete deliveries and cross-job destination identities.
func (id DeliveryID) Validate() error {
	if err := id.Tuple.Validate(); err != nil {
		return fmt.Errorf("delivery tuple: %w", err)
	}
	if id.EdgeID == 0 {
		return errors.New("zero delivery edge ID")
	}
	if err := id.DestinationTask.Validate(); err != nil {
		return fmt.Errorf("delivery destination task: %w", err)
	}
	if id.DestinationTask.JobID != id.Tuple.JobID {
		return errors.New("delivery destination task job ID mismatch")
	}
	return nil
}

// DeriveJobID derives a stable job ID from a request identity and topology digest.
func DeriveJobID(request ClientRequestID, topologyDigest [32]byte) JobID {
	encoded := make([]byte, 0, len(jobIDDomain)+16+8+32)
	encoded = append(encoded, jobIDDomain...)
	encoded = append(encoded, request.ClientID[:]...)
	encoded = appendUint64(encoded, request.Sequence)
	encoded = append(encoded, topologyDigest[:]...)
	digest := sha256.Sum256(encoded)
	var job JobID
	copy(job[:], digest[:len(job)])
	return job
}

// DeriveSourceTupleID derives the path digest of one source emission.
func DeriveSourceTupleID(job JobID, source TaskID, sequence uint64) TupleID {
	if job.Validate() != nil || source.Validate() != nil || source.JobID != job || sequence == 0 {
		return TupleID{}
	}
	encoded := make([]byte, 0, len(sourceTupleIDDomain)+16+20+8)
	encoded = append(encoded, sourceTupleIDDomain...)
	encoded = append(encoded, job[:]...)
	encoded = appendTaskID(encoded, source)
	encoded = appendUint64(encoded, sequence)
	return TupleID{JobID: job, SourceTask: source, SourceSequence: sequence, PathDigest: sha256.Sum256(encoded)}
}

// DeriveChildTupleID preserves the source identity and derives a distinct path.
func DeriveChildTupleID(parent TupleID, producer TaskID, edgeID, outputOrdinal uint16) TupleID {
	if parent.Validate() != nil || producer.Validate() != nil || producer.JobID != parent.JobID || edgeID == 0 {
		return TupleID{}
	}
	encoded := make([]byte, 0, len(childTupleIDDomain)+76+20+2+2)
	encoded = append(encoded, childTupleIDDomain...)
	encoded = appendTupleID(encoded, parent)
	encoded = appendTaskID(encoded, producer)
	encoded = appendUint16(encoded, edgeID)
	encoded = appendUint16(encoded, outputOrdinal)
	return TupleID{JobID: parent.JobID, SourceTask: parent.SourceTask, SourceSequence: parent.SourceSequence, PathDigest: sha256.Sum256(encoded)}
}

// ValidateIdentityReuse accepts an exact retry and rejects a collision where
// the same comparable identity is paired with different defining bytes.
// Distinct identities are independent records and are accepted.
func ValidateIdentityReuse[T comparable](storedID, presentedID T, storedDefinition, presentedDefinition []byte) error {
	if storedID == presentedID && !bytes.Equal(storedDefinition, presentedDefinition) {
		return ErrIdentityReuse
	}
	return nil
}

func appendTaskID(destination []byte, task TaskID) []byte {
	destination = append(destination, task.JobID[:]...)
	destination = appendUint16(destination, task.StageID)
	return appendUint16(destination, task.Partition)
}

func appendTupleID(destination []byte, tuple TupleID) []byte {
	destination = append(destination, tuple.JobID[:]...)
	destination = appendTaskID(destination, tuple.SourceTask)
	destination = appendUint64(destination, tuple.SourceSequence)
	return append(destination, tuple.PathDigest[:]...)
}

func isZero16(value [16]byte) bool { return value == [16]byte{} }

func isZero32(value [32]byte) bool { return value == [32]byte{} }
