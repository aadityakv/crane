// Package protocol defines Crane's concrete canonical network payloads.
package protocol

import (
	"errors"
	"fmt"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	// TupleDeliverySchemaVersion is the canonical v1 tuple delivery schema.
	TupleDeliverySchemaVersion uint16 = 1
	// TupleACKSchemaVersion is the canonical v1 tuple acknowledgement schema.
	TupleACKSchemaVersion uint16 = 1
	// TupleNACKSchemaVersion is the canonical v1 tuple rejection schema.
	TupleNACKSchemaVersion uint16 = 1
)

var (
	// ErrMalformedTupleMessage classifies truncated, trailing, or impossible encodings.
	ErrMalformedTupleMessage = errors.New("malformed Crane tuple message")
	// ErrUnsupportedTupleSchema classifies an unknown tuple payload schema version.
	ErrUnsupportedTupleSchema = errors.New("unsupported Crane tuple schema")
	// ErrUnexpectedMessage classifies a payload decoded through the wrong message codec.
	ErrUnexpectedMessage = errors.New("unexpected Crane tuple message type")
	// ErrInvalidTupleMessage classifies a structurally complete payload with invalid semantics.
	ErrInvalidTupleMessage = errors.New("invalid Crane tuple message")
	// ErrTupleMessageTooLarge classifies a payload that cannot fit the compiled datagram bound.
	ErrTupleMessageTooLarge = errors.New("Crane tuple message too large")
)

// AssignmentSetIdentity binds tuple traffic to one exact complete committed set.
type AssignmentSetIdentity struct {
	// JobID is the job whose complete assignment set is installed.
	JobID model.JobID
	// Revision is the nonzero monotonically increasing assignment-set revision.
	Revision uint64
	// Digest is the canonical digest of the complete assignment set.
	Digest [32]byte
}

// TupleDelivery transfers one canonical tuple under exact producer and destination fences.
type TupleDelivery struct {
	// DeliveryID is the stable logical custody key and excludes transmission metadata.
	DeliveryID model.DeliveryID
	// Tuple is the owned canonical tuple payload named by DeliveryID.
	Tuple model.Tuple
	// Producer is the exact current assignment token of the sending task.
	Producer model.AssignmentToken
	// Destination is the exact current assignment token selected by the route.
	Destination model.AssignmentToken
	// Assignment identifies the complete assignment set containing both tokens.
	Assignment AssignmentSetIdentity
	// Coordinator is the leadership fence under which this delivery is authorized.
	Coordinator model.CoordinatorEpoch
}

// MessageType returns the sole authenticated wire type for TupleDelivery.
func (TupleDelivery) MessageType() wire.MessageType { return wire.MessageCraneTupleDelivery }

// TupleACKStatus is the closed durable custody state reported by an ACK.
type TupleACKStatus uint8

const (
	// TupleAccepted means durable Received custody and reservation have completed.
	TupleAccepted TupleACKStatus = iota + 1
	// TupleCompleted means the complete downstream tree or replicated sink is durable.
	TupleCompleted
)

// TupleACK acknowledges one delivery under the exact accepted destination fence.
type TupleACK struct {
	// DeliveryID is the stable custody key being acknowledged.
	DeliveryID model.DeliveryID
	// Destination is the exact destination token that accepted the delivery.
	Destination model.AssignmentToken
	// Assignment identifies the complete set under which custody was accepted.
	Assignment AssignmentSetIdentity
	// Coordinator is the coordinator fence copied from the accepted delivery.
	Coordinator model.CoordinatorEpoch
	// Status distinguishes durable custody from durable downstream completion.
	Status TupleACKStatus
}

// MessageType returns the sole authenticated wire type for TupleACK.
func (TupleACK) MessageType() wire.MessageType { return wire.MessageCraneTupleDeliveryAck }

// TupleNACKCode is the closed, allocation-free rejection reason enumeration.
type TupleNACKCode uint16

const (
	// TupleNACKNotReady reports that worker recovery or admission is incomplete.
	TupleNACKNotReady TupleNACKCode = iota + 1
	// TupleNACKUnknownAssignment reports an unknown job, task, or assignment set.
	TupleNACKUnknownAssignment
	// TupleNACKStaleCoordinator reports a fenced coordinator epoch.
	TupleNACKStaleCoordinator
	// TupleNACKStaleAssignment reports a stale token, attempt, worker epoch, or set.
	TupleNACKStaleAssignment
	// TupleNACKWrongDestination reports a route that does not target this task.
	TupleNACKWrongDestination
	// TupleNACKCapacityExceeded reports that durable custody cannot be reserved.
	TupleNACKCapacityExceeded
	// TupleNACKOverloaded reports bounded transient queue or replay pressure.
	TupleNACKOverloaded
)

// TupleNACK rejects one delivery without inventing replacement assignment tokens.
type TupleNACK struct {
	// DeliveryID is the stable custody key being rejected.
	DeliveryID model.DeliveryID
	// Destination is the exact rejected destination token copied from the delivery.
	Destination model.AssignmentToken
	// Assignment identifies the exact rejected complete assignment set.
	Assignment AssignmentSetIdentity
	// Coordinator is the exact rejected coordinator fence.
	Coordinator model.CoordinatorEpoch
	// Code is one bounded, authentication-safe rejection reason.
	Code TupleNACKCode
}

// MessageType returns the sole authenticated wire type for TupleNACK.
func (TupleNACK) MessageType() wire.MessageType { return wire.MessageCraneTupleDeliveryNack }

func (identity AssignmentSetIdentity) validate(job model.JobID) error {
	if err := identity.JobID.Validate(); err != nil {
		return fmt.Errorf("assignment job: %w", err)
	}
	if identity.JobID != job {
		return errors.New("assignment set job does not match delivery")
	}
	if identity.Revision == 0 {
		return errors.New("zero assignment-set revision")
	}
	if identity.Digest == ([32]byte{}) {
		return errors.New("zero assignment-set digest")
	}
	return nil
}

func validateDestinationEnvelope(delivery model.DeliveryID, destination model.AssignmentToken, assignment AssignmentSetIdentity, coordinator model.CoordinatorEpoch) error {
	if err := delivery.Validate(); err != nil {
		return fmt.Errorf("delivery identity: %w", err)
	}
	job := delivery.Tuple.JobID
	if err := destination.Validate(); err != nil {
		return fmt.Errorf("destination token: %w", err)
	}
	if destination.Task != delivery.DestinationTask {
		return errors.New("destination token does not match routed destination")
	}
	if destination.Task.JobID != job {
		return errors.New("destination token job does not match delivery")
	}
	if destination.SpecificationHash == ([32]byte{}) {
		return errors.New("zero destination specification hash")
	}
	if err := assignment.validate(job); err != nil {
		return err
	}
	if destination.AssignmentRevision != assignment.Revision {
		return errors.New("destination token does not match assignment-set revision")
	}
	if err := coordinator.Validate(); err != nil {
		return fmt.Errorf("coordinator epoch: %w", err)
	}
	return nil
}

func (delivery TupleDelivery) validate() error {
	if err := validateDestinationEnvelope(delivery.DeliveryID, delivery.Destination, delivery.Assignment, delivery.Coordinator); err != nil {
		return err
	}
	if err := delivery.Producer.Validate(); err != nil {
		return fmt.Errorf("producer token: %w", err)
	}
	if delivery.Producer.Task.JobID != delivery.DeliveryID.Tuple.JobID {
		return errors.New("producer token job does not match delivery")
	}
	if delivery.Producer.AssignmentRevision != delivery.Assignment.Revision {
		return errors.New("producer token does not match assignment-set revision")
	}
	if delivery.Producer.SpecificationHash == ([32]byte{}) || delivery.Producer.SpecificationHash != delivery.Destination.SpecificationHash {
		return errors.New("producer and destination specification hashes differ")
	}
	if err := delivery.Tuple.Validate(); err != nil {
		return fmt.Errorf("tuple: %w", err)
	}
	return nil
}

func (ack TupleACK) validate() error {
	if err := validateDestinationEnvelope(ack.DeliveryID, ack.Destination, ack.Assignment, ack.Coordinator); err != nil {
		return err
	}
	if ack.Status < TupleAccepted || ack.Status > TupleCompleted {
		return errors.New("unknown tuple ACK status")
	}
	return nil
}

func (nack TupleNACK) validate() error {
	if err := validateDestinationEnvelope(nack.DeliveryID, nack.Destination, nack.Assignment, nack.Coordinator); err != nil {
		return err
	}
	if nack.Code < TupleNACKNotReady || nack.Code > TupleNACKOverloaded {
		return errors.New("unknown tuple NACK code")
	}
	return nil
}
