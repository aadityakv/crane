package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
)

func applyDelivery(work *RecoveredWork, delivery DeliveryRecord, outboxes []OutboxRecord, processed bool, proven map[model.DeliveryID]struct{}) error {
	if uint64(len(delivery.Outputs)) > model.LimitsV1().MaxOperatorOutputs || uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("processed output/outbox count exceeds v1 bounds")
	}
	assignment, ok := findAssignment(work, delivery.ID.Tuple.JobID)
	if !ok {
		return errors.New("delivery references unknown assignment")
	}
	if err := validateDelivery(delivery, assignment, work.Fence); err != nil {
		return err
	}
	digest, err := deliveryDefinitionDigest(delivery)
	if err != nil {
		return err
	}
	delivery.definitionDigest = digest
	index := deliveryIndex(work.Deliveries, delivery.ID)
	if !processed {
		if delivery.State != Received || len(outboxes) != 0 || len(delivery.Outputs) != 0 || len(delivery.OutboxIDs) != 0 {
			return errors.New("invalid received delivery record")
		}
		if index >= 0 {
			if !equalDeliveryDefinition(work.Deliveries[index], delivery) {
				return model.ErrIdentityReuse
			}
			return nil
		}
		if len(work.Deliveries) >= MaxTransactionRecords {
			return ErrCapacity
		}
		work.Deliveries = append(work.Deliveries, delivery.Clone())
		return nil
	}
	if index >= 0 {
		if err := validateProcessedOutputs(work.Deliveries[index], delivery.Outputs, assignment); err != nil {
			return err
		}
	}
	if index >= 0 && (work.Deliveries[index].State == Processed || work.Deliveries[index].State == Completed) {
		prior := work.Deliveries[index]
		if !equalDeliveryDefinition(prior, delivery) || !equalTuples(prior.Outputs, delivery.Outputs) || len(prior.OutboxIDs) != len(outboxes) {
			return model.ErrIdentityReuse
		}
		for outboxIndexInRecord, outbox := range outboxes {
			stored := outboxIndex(work.Outboxes, prior.OutboxIDs[outboxIndexInRecord])
			if outbox.Completed || outbox.Accepted || outbox.RetryDeadlineUnixNano != 0 || stored < 0 || outbox.ID != prior.OutboxIDs[outboxIndexInRecord] || !equalOutboxDefinition(work.Outboxes[stored], outbox) {
				return model.ErrIdentityReuse
			}
		}
		return nil
	}
	if index < 0 || work.Deliveries[index].State != Received {
		return errors.New("processed delivery has no received predecessor")
	}
	if delivery.State != Processed || !equalDeliveryDefinition(work.Deliveries[index], delivery) {
		return errors.New("processed delivery identity changed")
	}
	expected, err := expectedProcessedOutboxes(delivery, assignment)
	if err != nil {
		return err
	}
	if len(outboxes) != len(expected) {
		return errors.New("processed outboxes are not the complete topology-derived set")
	}
	if !outboxesCanonical(outboxes) {
		return errors.New("processed outboxes are not in canonical identity order")
	}
	seen := make(map[model.DeliveryID]struct{}, len(outboxes))
	for _, outbox := range outboxes {
		if _, duplicate := seen[outbox.ID]; duplicate {
			return errors.New("duplicate outbox")
		}
		seen[outbox.ID] = struct{}{}
		if outbox.Completed || outbox.Accepted || outbox.RetryDeadlineUnixNano != 0 {
			return errors.New("new processed outbox has retry or completion state")
		}
		if err := validateOutbox(outbox, assignment, work.Fence, proven); err != nil {
			return err
		}
		if outbox.Producer != currentProducerToken(delivery, assignment) {
			return errors.New("outbox producer is not processed destination")
		}
		want, exists := expected[outbox.ID]
		if !exists || !equalOutboxDefinition(want, outbox) {
			return errors.New("processed outbox does not match topology-derived definition")
		}
		if outboxIndex(work.Outboxes, outbox.ID) >= 0 {
			return errors.New("outbox identity already exists")
		}
	}
	delivery.OutboxIDs = delivery.OutboxIDs[:0]
	for _, outbox := range outboxes {
		delivery.OutboxIDs = append(delivery.OutboxIDs, outbox.ID)
		work.Outboxes = append(work.Outboxes, outbox.Clone())
	}
	work.Deliveries[index] = delivery.Clone()
	return nil
}

func validateProcessedOutputs(delivery DeliveryRecord, outputs []model.Tuple, assignment InstalledAssignment) error {
	canonical, err := validateOutputTupleBounds(outputs)
	if err != nil {
		return err
	}
	stage, ok := findStage(assignment.Topology, delivery.Destination.Task.StageID)
	if !ok {
		return errors.New("processed delivery destination stage is absent")
	}
	expected, err := model.ExecuteOperator(stage.Operator, delivery.Tuple)
	if err != nil {
		return fmt.Errorf("execute installed operator: %w", err)
	}
	if uint64(len(expected)) > model.LimitsV1().MaxOperatorOutputs || len(expected) != len(canonical) {
		return errors.New("processed outputs do not match installed operator count")
	}
	for index := range expected {
		encoded, err := model.MarshalTuple(expected[index])
		if err != nil {
			return fmt.Errorf("installed operator output %d: %w", index, err)
		}
		if !bytes.Equal(encoded, canonical[index]) {
			return errors.New("processed outputs do not match installed operator bytes and order")
		}
	}
	return nil
}

func validateOutputTupleBounds(outputs []model.Tuple) ([][]byte, error) {
	if uint64(len(outputs)) > model.LimitsV1().MaxOperatorOutputs {
		return nil, errors.New("processed output count exceeds v1 bound")
	}
	canonical := make([][]byte, len(outputs))
	for index := range outputs {
		encoded, err := model.MarshalTuple(outputs[index])
		if err != nil {
			return nil, fmt.Errorf("processed output %d: %w", index, err)
		}
		canonical[index] = encoded
	}
	return canonical, nil
}

func applyCompleted(work *RecoveredWork, id model.DeliveryID) error {
	index := deliveryIndex(work.Deliveries, id)
	if index < 0 {
		return errors.New("completion references unknown delivery")
	}
	if work.Deliveries[index].State == Compacted || work.Deliveries[index].State == Completed {
		return nil
	}
	if work.Deliveries[index].State != Processed {
		return errors.New("delivery completion before processing")
	}
	for _, outboxID := range work.Deliveries[index].OutboxIDs {
		outbox := outboxIndex(work.Outboxes, outboxID)
		if outbox < 0 || !work.Outboxes[outbox].Completed {
			return errors.New("delivery completion before downstream outbox completion")
		}
	}
	work.Deliveries[index].State = Completed
	return nil
}

func applyOutboxAck(work *RecoveredWork, id model.DeliveryID) error {
	index := outboxIndex(work.Outboxes, id)
	if index < 0 {
		return errors.New("unknown outbox")
	}
	work.Outboxes[index].Completed = true
	return nil
}

type outboxRetryUpdate struct {
	ID               model.DeliveryID
	Accepted         bool
	AcceptTransition bool
	DeadlineUnixNano int64
}

func applyOutboxRetry(work *RecoveredWork, update outboxRetryUpdate) error {
	index := outboxIndex(work.Outboxes, update.ID)
	if index < 0 {
		return errors.New("retry references unknown outbox")
	}
	if update.DeadlineUnixNano == 0 {
		return errors.New("retry deadline is unset")
	}
	record := &work.Outboxes[index]
	if record.Completed {
		return errors.New("retry references completed outbox")
	}
	if update.AcceptTransition {
		if !update.Accepted {
			return errors.New("accepted transition regresses retry phase")
		}
		if record.Accepted {
			if record.RetryDeadlineUnixNano != update.DeadlineUnixNano {
				return model.ErrIdentityReuse
			}
			return nil
		}
		record.Accepted = true
		record.RetryDeadlineUnixNano = update.DeadlineUnixNano
		return nil
	}
	if update.Accepted != record.Accepted {
		return errors.New("dispatch retry phase mismatch")
	}
	if record.RetryDeadlineUnixNano == update.DeadlineUnixNano {
		return nil
	}
	record.RetryDeadlineUnixNano = update.DeadlineUnixNano
	return nil
}

// readoptedDeliveryAuthority reports whether one retained record published
// under a superseded fence may re-enter under the current fence: its
// assignment identity must still match the current installed assignment
// byte-exactly and its retained epoch must be strictly ordered before the
// current committed fence. Genuinely replaced assignments never re-adopt.
func readoptedDeliveryAuthority(record DeliveryRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) bool {
	if assignment.CoordinatorEpoch != fence || compareEpochOrder(record.CoordinatorEpoch, fence) > 0 {
		return false
	}
	if record.AssignmentRevision == assignment.Assignment.Revision {
		return record.AssignmentDigest == assignment.Assignment.Digest && compareEpochOrder(record.CoordinatorEpoch, fence) < 0
	}
	// A superseded assignment revision re-adopts only when the custody's own
	// destination task incarnation is unchanged in the current set (Task 24
	// defect #4 ruling: retained custody re-envelopes under the current
	// assignment; a replaced task never re-enters).
	return record.AssignmentRevision < assignment.Assignment.Revision && sameTaskIncarnation(assignment.Assignment, record.Destination)
}

// sameTaskIncarnation reports whether token's task is placed on the identical
// worker incarnation (worker, epoch, attempt, specification) in set; only the
// token's AssignmentRevision may differ.
func sameTaskIncarnation(set model.AssignmentSet, token model.AssignmentToken) bool {
	current, ok := findToken(set, token.Task)
	return ok && current.WorkerID == token.WorkerID && current.WorkerEpoch == token.WorkerEpoch && current.Attempt == token.Attempt && current.SpecificationHash == token.SpecificationHash
}

// equalDeliveryDefinitionModuloEnvelope compares the logical custody of two
// delivery definitions ignoring the assignment envelope (producer incarnation,
// revision, digest, coordinator epoch): identity, payload bytes, reservation,
// producer task and destination task incarnation must agree.
func equalDeliveryDefinitionModuloEnvelope(a, b DeliveryRecord) bool {
	if a.ID != b.ID || a.Producer.Task != b.Producer.Task || a.Reservation != b.Reservation ||
		a.Destination.Task != b.Destination.Task || a.Destination.WorkerID != b.Destination.WorkerID || a.Destination.WorkerEpoch != b.Destination.WorkerEpoch || a.Destination.Attempt != b.Destination.Attempt || a.Destination.SpecificationHash != b.Destination.SpecificationHash {
		return false
	}
	aa, err := model.MarshalTuple(a.Tuple)
	if err != nil {
		return false
	}
	bb, err := model.MarshalTuple(b.Tuple)
	return err == nil && bytes.Equal(aa, bb)
}

// equalDeliveryDefinitionModuloEpoch compares one delivery definition against
// another while ignoring only the coordinator-epoch branding.
func equalDeliveryDefinitionModuloEpoch(a, b DeliveryRecord) bool {
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.Reservation != b.Reservation {
		return false
	}
	aa, err := model.MarshalTuple(a.Tuple)
	if err != nil {
		return false
	}
	bb, err := model.MarshalTuple(b.Tuple)
	return err == nil && bytes.Equal(aa, bb)
}

func validateDelivery(record DeliveryRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch) error {
	if record.State < Received || record.State > Compacted || record.AssignmentRevision == 0 || record.AssignmentDigest == ([32]byte{}) {
		return errors.New("invalid delivery metadata")
	}
	readopted := record.CoordinatorEpoch != fence || record.AssignmentRevision != assignment.Assignment.Revision || record.AssignmentDigest != assignment.Assignment.Digest
	if readopted && !readoptedDeliveryAuthority(record, assignment, fence) {
		return errors.New("delivery assignment fence mismatch")
	}
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	if _, err := protocol.MarshalTupleDelivery(message); err != nil {
		return err
	}
	supersededRevision := readopted && record.AssignmentRevision != assignment.Assignment.Revision
	if supersededRevision {
		if _, ok := findToken(assignment.Assignment, record.Producer.Task); !ok || !sameTaskIncarnation(assignment.Assignment, record.Destination) {
			return errors.New("delivery token not in installed assignment")
		}
	} else if !containsToken(assignment.Assignment, record.Producer) || !containsToken(assignment.Assignment, record.Destination) {
		return errors.New("delivery token not in installed assignment")
	}
	if err := validateRoute(assignment.Topology, record.ID, record.Tuple, record.Producer.Task); err != nil {
		return err
	}
	if record.Destination.WorkerID == 0 {
		return errors.New("zero destination")
	}
	want, err := assignment.Topology.WorstCaseCustodyBytes(record.Destination.Task)
	if err != nil {
		return err
	}
	if record.Reservation != want {
		return errors.New("delivery reservation does not match topology worst case")
	}
	derived, exists, err := deriveDeliveryDefinition(assignment, fence, record.ID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("delivery definition does not match deterministic source path")
	}
	if supersededRevision {
		// The reconstruction derives under the current assignment; a
		// re-adopted retained record differs from it only by its envelope.
		if !equalDeliveryDefinitionModuloEnvelope(derived, record) {
			return errors.New("delivery definition does not match deterministic source path")
		}
	} else if readopted {
		// The reconstruction derives under the current fence; a re-adopted
		// retained record differs from it only by the epoch branding.
		if !equalDeliveryDefinitionModuloEpoch(derived, record) {
			return errors.New("delivery definition does not match deterministic source path")
		}
	} else if !equalDeliveryDefinition(derived, record) {
		return errors.New("delivery definition does not match deterministic source path")
	}
	return nil
}

// outboxProofObserver, when non-nil, is invoked once for every executed
// expensive outbox proof (TupleDelivery construction + marshal + assignment
// containment + deterministic route validation). Production leaves it nil; it
// exists as the test seam that pins the once-per-record proof contract.
var outboxProofObserver func(record OutboxRecord)

// validateOutbox enforces the per-commit structural invariants of one outbox
// (fence branding and retry-state consistency) and, unless the record's
// immutable definition is already in the proven set, the expensive proof:
// TupleDelivery construction + MarshalTupleDelivery, assignment containment,
// and the deterministic route. A successful proof is recorded in proven; a nil
// set forces the proof (recovery-time validation and Snapshot full checks).
func validateOutbox(record OutboxRecord, assignment InstalledAssignment, fence model.CoordinatorEpoch, proven map[model.DeliveryID]struct{}) error {
	if record.CoordinatorEpoch != fence {
		return errors.New("outbox fence mismatch")
	}
	if record.Accepted && record.RetryDeadlineUnixNano == 0 {
		return errors.New("accepted outbox has no retry deadline")
	}
	if _, ok := proven[record.ID]; ok {
		return nil
	}
	if outboxProofObserver != nil {
		outboxProofObserver(record)
	}
	delivery := DeliveryRecord{ID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, AssignmentRevision: record.AssignmentRevision, AssignmentDigest: record.AssignmentDigest, CoordinatorEpoch: record.CoordinatorEpoch, State: Received}
	message := protocol.TupleDelivery{DeliveryID: delivery.ID, Tuple: delivery.Tuple, Producer: delivery.Producer, Destination: delivery.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: delivery.ID.Tuple.JobID, Revision: delivery.AssignmentRevision, Digest: delivery.AssignmentDigest}, Coordinator: delivery.CoordinatorEpoch}
	if _, err := protocol.MarshalTupleDelivery(message); err != nil {
		return err
	}
	if delivery.AssignmentRevision != assignment.Assignment.Revision || delivery.AssignmentDigest != assignment.Assignment.Digest || !containsToken(assignment.Assignment, delivery.Producer) || !containsToken(assignment.Assignment, delivery.Destination) {
		return errors.New("outbox assignment mismatch")
	}
	if err := validateRoute(assignment.Topology, record.ID, record.Tuple, record.Producer.Task); err != nil {
		return err
	}
	if proven != nil {
		proven[record.ID] = struct{}{}
	}
	return nil
}

func validateRoute(topology model.ValidatedTopology, id model.DeliveryID, tuple model.Tuple, producer model.TaskID) error {
	var edge model.EdgeSpec
	found := false
	for _, candidate := range topology.Spec().Edges {
		if candidate.EdgeID == id.EdgeID {
			edge = candidate
			found = true
			break
		}
	}
	if !found || edge.SourceStageID != producer.StageID || edge.DestinationStageID != id.DestinationTask.StageID {
		return errors.New("delivery route does not match topology edge")
	}
	partitions, err := model.Route(topology, edge, id.Tuple, tuple)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		if partition == id.DestinationTask.Partition {
			return nil
		}
	}
	return errors.New("delivery destination partition does not match deterministic route")
}

func deliveryDefinitionDigest(record DeliveryRecord) ([32]byte, error) {
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return [32]byte{}, err
	}
	var reservation [8]byte
	binary.BigEndian.PutUint64(reservation[:], record.Reservation)
	return sha256.Sum256(append(encoded, reservation[:]...)), nil
}

func expectedProcessedOutboxes(delivery DeliveryRecord, assignment InstalledAssignment) (map[model.DeliveryID]OutboxRecord, error) {
	result := make(map[model.DeliveryID]OutboxRecord)
	producer := currentProducerToken(delivery, assignment)
	for outputIndex, tuple := range delivery.Outputs {
		for _, edge := range assignment.Topology.Spec().Edges {
			if edge.SourceStageID != delivery.Destination.Task.StageID {
				continue
			}
			if outputIndex > math.MaxUint16 {
				return nil, errors.New("output ordinal exceeds v1 identity")
			}
			child := model.DeriveChildTupleID(delivery.ID.Tuple, delivery.Destination.Task, edge.EdgeID, uint16(outputIndex))
			partitions, err := model.Route(assignment.Topology, edge, child, tuple)
			if err != nil {
				return nil, err
			}
			for _, partition := range partitions {
				task := model.TaskID{JobID: delivery.ID.Tuple.JobID, StageID: edge.DestinationStageID, Partition: partition}
				destination, ok := findToken(assignment.Assignment, task)
				if !ok {
					return nil, errors.New("derived outbox destination has no assignment token")
				}
				id := model.DeliveryID{Tuple: child, EdgeID: edge.EdgeID, DestinationTask: task}
				if _, duplicate := result[id]; duplicate {
					return nil, errors.New("topology derived duplicate outbox identity")
				}
				// Emissions are always branded with the CURRENT installed
				// assignment identity and fence: a re-adopted retained
				// delivery derives its outboxes under the fence it re-entered
				// through, never the superseded one it was published under.
				result[id] = OutboxRecord{ID: id, Tuple: cloneTuple(tuple), Producer: producer, Destination: destination, AssignmentRevision: assignment.Assignment.Revision, AssignmentDigest: assignment.Assignment.Digest, CoordinatorEpoch: assignment.CoordinatorEpoch}
			}
		}
	}
	if uint64(len(result)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("topology-derived outboxes exceed v1 bound")
	}
	return result, nil
}

// currentProducerToken returns the token a retained delivery's destination
// task carries in the current assignment — the same incarnation under the
// current revision — falling back to the retained token when absent.
func currentProducerToken(delivery DeliveryRecord, assignment InstalledAssignment) model.AssignmentToken {
	if current, ok := findToken(assignment.Assignment, delivery.Destination.Task); ok && sameTaskIncarnation(assignment.Assignment, delivery.Destination) {
		return current
	}
	return delivery.Destination
}

func deriveDeliveryDefinition(assignment InstalledAssignment, fence model.CoordinatorEpoch, target model.DeliveryID) (DeliveryRecord, bool, error) {
	if err := target.Validate(); err != nil {
		return DeliveryRecord{}, false, nil
	}
	if assignment.CoordinatorEpoch != fence {
		return DeliveryRecord{}, false, nil
	}
	source, ok := findToken(assignment.Assignment, target.Tuple.SourceTask)
	if !ok {
		return DeliveryRecord{}, false, nil
	}
	eof, err := model.SourceEOF(assignment.Topology, target.Tuple.SourceTask)
	if err != nil || target.Tuple.SourceSequence == 0 || target.Tuple.SourceSequence > eof {
		return DeliveryRecord{}, false, nil
	}
	initial, err := expectedSourceOutboxes(SourceCursor{Source: target.Tuple.SourceTask, NextSequence: target.Tuple.SourceSequence + 1, EOF: eof}, source, assignment)
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	queue := make([]DeliveryRecord, 0, len(initial))
	for _, outbox := range initial {
		delivery, err := deliveryFromOutbox(outbox, assignment.Topology)
		if err != nil {
			return DeliveryRecord{}, false, err
		}
		queue = append(queue, delivery)
	}
	seen := make(map[model.DeliveryID]struct{})
	for len(queue) != 0 {
		delivery := queue[0]
		queue = queue[1:]
		if _, duplicate := seen[delivery.ID]; duplicate {
			continue
		}
		seen[delivery.ID] = struct{}{}
		if delivery.ID == target {
			return delivery, true, nil
		}
		if uint64(len(seen)) >= model.LimitsV1().MaxDerivedDeliveries {
			break
		}
		stage, ok := findStage(assignment.Topology, delivery.Destination.Task.StageID)
		if !ok {
			return DeliveryRecord{}, false, errors.New("derived delivery stage is absent")
		}
		outputs, err := model.ExecuteOperator(stage.Operator, delivery.Tuple)
		if err != nil {
			return DeliveryRecord{}, false, err
		}
		delivery.State, delivery.Outputs = Processed, outputs
		next, err := expectedProcessedOutboxes(delivery, assignment)
		if err != nil {
			return DeliveryRecord{}, false, err
		}
		for _, outbox := range next {
			child, err := deliveryFromOutbox(outbox, assignment.Topology)
			if err != nil {
				return DeliveryRecord{}, false, err
			}
			queue = append(queue, child)
		}
	}
	return DeliveryRecord{}, false, nil
}

func deliveryFromOutbox(outbox OutboxRecord, topology model.ValidatedTopology) (DeliveryRecord, error) {
	reservation, err := topology.WorstCaseCustodyBytes(outbox.Destination.Task)
	if err != nil {
		return DeliveryRecord{}, err
	}
	record := DeliveryRecord{ID: outbox.ID, Tuple: cloneTuple(outbox.Tuple), Producer: outbox.Producer, Destination: outbox.Destination, AssignmentRevision: outbox.AssignmentRevision, AssignmentDigest: outbox.AssignmentDigest, CoordinatorEpoch: outbox.CoordinatorEpoch, State: Received, Reservation: reservation}
	record.definitionDigest, err = deliveryDefinitionDigest(record)
	return record, err
}

func findStage(topology model.ValidatedTopology, id uint16) (model.StageSpec, bool) {
	for _, stage := range topology.Spec().Stages {
		if stage.StageID == id {
			return stage, true
		}
	}
	return model.StageSpec{}, false
}

// Receive durably accepts exact custody or returns the prior duplicate state.
func (store *Store) Receive(record DeliveryRecord) (DeliveryState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrClosed
	}
	if store.failed {
		return 0, ErrUnavailable
	}
	if record.Destination.WorkerID != store.state.Identity.NodeID || record.Destination.WorkerEpoch != store.state.WorkerEpoch {
		return 0, errors.New("delivery destination is not this worker incarnation")
	}
	if state, found, err := probeDelivery(&store.work, record); found || err != nil {
		return state, err
	}
	payload, err := encodeDeliveryRecord(record, nil)
	if err != nil {
		return 0, err
	}
	tx := Transaction{Records: []Record{{Type: recordDelivery, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return 0, err
	}
	if err = store.commitWorkLocked(tx, prospective); err != nil {
		return 0, err
	}
	store.durable(BoundaryDeliveryReceived)
	return Received, nil
}

// ProbeDelivery non-mutatingly returns exact prior custody even when current
// admission authority has advanced. Unknown identities remain distinguishable
// from changed bytes under a known durable identity.
func (store *Store) ProbeDelivery(record DeliveryRecord) (DeliveryState, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, false, ErrClosed
	}
	if store.failed {
		return 0, false, ErrUnavailable
	}
	if record.Destination.WorkerID != store.state.Identity.NodeID || record.Destination.WorkerEpoch != store.state.WorkerEpoch {
		return 0, false, errors.New("delivery destination is not this worker incarnation")
	}
	return probeDelivery(&store.work, record)
}

func probeDelivery(work *RecoveredWork, record DeliveryRecord) (DeliveryState, bool, error) {
	if index := deliveryIndex(work.Deliveries, record.ID); index >= 0 {
		prior := work.Deliveries[index]
		if prior.State == Compacted {
			digest, err := deliveryDefinitionDigest(record)
			if err != nil {
				return 0, true, err
			}
			if digest != prior.definitionDigest {
				return 0, true, model.ErrIdentityReuse
			}
			return Compacted, true, nil
		}
		if !equalDeliveryDefinition(prior, record) {
			// A current-fence rebrand of one's own exact retained definition
			// re-adopts the retained custody (defect #5 ruling): the delivery
			// re-enters under the fence it was re-validated against.
			assignment, ok := findAssignment(work, record.ID.Tuple.JobID)
			if ok && record.CoordinatorEpoch == work.Fence &&
				compareEpochOrder(prior.CoordinatorEpoch, work.Fence) < 0 &&
				assignment.CoordinatorEpoch == work.Fence &&
				prior.AssignmentRevision == assignment.Assignment.Revision &&
				prior.AssignmentDigest == assignment.Assignment.Digest &&
				equalDeliveryDefinitionModuloEpoch(prior, record) {
				return prior.State, true, nil
			}
			// The current assignment's exact derivation of the same logical
			// custody re-delivered by a (possibly replaced) producer answers
			// from custody retained under a superseded revision when this
			// destination task's incarnation is unchanged (Task 24 defect #4
			// ruling: retained custody re-envelopes under the current
			// assignment).
			if ok && record.CoordinatorEpoch == work.Fence && assignment.CoordinatorEpoch == work.Fence &&
				prior.AssignmentRevision < assignment.Assignment.Revision &&
				record.AssignmentRevision == assignment.Assignment.Revision && record.AssignmentDigest == assignment.Assignment.Digest &&
				sameTaskIncarnation(assignment.Assignment, prior.Destination) {
				derived, exists, err := deriveDeliveryDefinition(assignment, work.Fence, record.ID)
				if err != nil {
					return 0, true, err
				}
				if exists && equalDeliveryDefinition(derived, record) && equalDeliveryDefinitionModuloEnvelope(derived, prior) {
					return prior.State, true, nil
				}
			}
			return 0, true, model.ErrIdentityReuse
		}
		return prior.State, true, nil
	}
	cursor := sourceIndex(work.Sources, record.ID.Tuple.SourceTask)
	if cursor < 0 || record.ID.Tuple.SourceSequence > work.Sources[cursor].Watermark {
		return 0, false, nil
	}
	assignment, ok := findAssignment(work, record.ID.Tuple.JobID)
	if !ok {
		return 0, true, model.ErrIdentityReuse
	}
	expected, exists, err := deriveDeliveryDefinition(assignment, assignment.CoordinatorEpoch, record.ID)
	if err != nil {
		return 0, true, err
	}
	if !exists {
		return 0, false, nil
	}
	if !equalCompactedLogicalDefinition(expected, record) {
		return 0, true, model.ErrIdentityReuse
	}
	if record.AssignmentRevision < expected.AssignmentRevision {
		// A committed checkpoint proves every upstream outbox completion was
		// already durable, so no correct sender depends on an ACK beyond this
		// causal-safe frontier. Once assignment replacement retires the old
		// tokens, fail closed instead of accepting unverifiable authority.
		return 0, true, ErrHistoricalAuthorityUnavailable
	}
	if !equalDeliveryDefinition(expected, record) {
		return 0, true, model.ErrIdentityReuse
	}
	return Compacted, true, nil
}

// MarkProcessed atomically persists deterministic outputs and every downstream outbox.
func (store *Store) MarkProcessed(id model.DeliveryID, outputs []model.Tuple, outboxes []OutboxRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	index := deliveryIndex(store.work.Deliveries, id)
	if index < 0 {
		return errors.New("unknown delivery")
	}
	if uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return errors.New("processed outbox count exceeds v1 bounds")
	}
	assignment, ok := findAssignment(&store.work, id.Tuple.JobID)
	if !ok {
		return errors.New("processed delivery references unknown assignment")
	}
	if _, err := validateOutputTupleBounds(outputs); err != nil {
		return err
	}
	stored := store.work.Deliveries[index]
	if stored.State == Processed || stored.State == Completed {
		if !equalTuples(stored.Outputs, outputs) || len(stored.OutboxIDs) != len(outboxes) {
			return model.ErrIdentityReuse
		}
		for outIndex, id := range stored.OutboxIDs {
			storedIndex := outboxIndex(store.work.Outboxes, id)
			candidate := outboxes[outIndex]
			if candidate.Completed || candidate.Accepted || candidate.RetryDeadlineUnixNano != 0 || storedIndex < 0 || !equalOutboxDefinition(store.work.Outboxes[storedIndex], candidate) {
				return model.ErrIdentityReuse
			}
		}
		return nil
	}
	if err := validateProcessedOutputs(stored, outputs, assignment); err != nil {
		return err
	}
	record := stored.Clone()
	if record.State != Received {
		return errors.New("delivery not received")
	}
	record.State = Processed
	record.Outputs = cloneTuples(outputs)
	payload, err := encodeDeliveryRecord(record, outboxes)
	if err != nil {
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordDeliveryProcessed, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	store.durable(BoundaryDeliveryProcessed)
	return nil
}

// MarkCompleted durably closes a processed delivery.
func (store *Store) MarkCompleted(id model.DeliveryID) error {
	payload, err := encodeDeliveryIDPayload(id)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordDeliveryCompleted, Payload: payload}}}, BoundaryDeliveryCompleted)
}

// MarkOutboxCompleted durably records one downstream completion.
func (store *Store) MarkOutboxCompleted(id model.DeliveryID) error {
	payload, err := encodeDeliveryIDPayload(id)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordOutboxAck, Payload: payload}}}, BoundaryOutboxCompleted)
}

// MarkOutboxDispatched durably records the retry deadline chosen from the
// injected clock at actual sender dispatch start.
func (store *Store) MarkOutboxDispatched(id model.DeliveryID, deadlineUnixNano int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	if store.failed {
		return ErrUnavailable
	}
	index := outboxIndex(store.work.Outboxes, id)
	if index < 0 {
		return errors.New("unknown outbox")
	}
	update := outboxRetryUpdate{ID: id, Accepted: store.work.Outboxes[index].Accepted, DeadlineUnixNano: deadlineUnixNano}
	payload, err := encodeOutboxRetry(update)
	if err != nil {
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordOutboxRetry, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err != nil {
		return err
	}
	if err := store.commitWorkLocked(tx, prospective); err != nil {
		return err
	}
	store.durable(BoundaryOutboxDispatched)
	return nil
}

// MarkOutboxAccepted durably enters the completion-wait retry phase.
func (store *Store) MarkOutboxAccepted(id model.DeliveryID, deadlineUnixNano int64) error {
	update := outboxRetryUpdate{ID: id, Accepted: true, AcceptTransition: true, DeadlineUnixNano: deadlineUnixNano}
	payload, err := encodeOutboxRetry(update)
	if err != nil {
		return err
	}
	return store.applyWorkTransaction(Transaction{Records: []Record{{Type: recordOutboxRetry, Payload: payload}}}, BoundaryOutboxAccepted)
}

func encodeDeliveryRecord(record DeliveryRecord, outboxes []OutboxRecord) ([]byte, error) {
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.u8(uint8(record.State))
	w.u64(record.Reservation)
	w.blob(encoded)
	if uint64(len(record.Outputs)) > model.LimitsV1().MaxOperatorOutputs || uint64(len(outboxes)) > model.LimitsV1().MaxDerivedDeliveries {
		return nil, errors.New("delivery collections exceed bounds")
	}
	w.u16(uint16(len(record.Outputs)))
	for _, tuple := range record.Outputs {
		b, e := model.MarshalTuple(tuple)
		if e != nil {
			return nil, e
		}
		w.blob(b)
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

func decodeDeliveryRecord(payload []byte) (DeliveryRecord, []OutboxRecord, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return DeliveryRecord{}, nil, err
	}
	state, err := r.u8()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	reservation, err := r.u64()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	message, err := protocol.UnmarshalTupleDelivery(encoded)
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	record := DeliveryRecord{ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination, AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest, CoordinatorEpoch: message.Coordinator, State: DeliveryState(state), Reservation: reservation}
	count, err := r.u16()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	if uint64(count) > model.LimitsV1().MaxOperatorOutputs || r.remaining() < int(count)*4+2 {
		return DeliveryRecord{}, nil, errors.New("delivery output collection exceeds bounds or remaining bytes")
	}
	record.Outputs = make([]model.Tuple, int(count))
	for i := range record.Outputs {
		b, e := r.blob(model.LimitsV1().MaxTuplePayloadBytes)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
		record.Outputs[i], e = model.UnmarshalTuple(b)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
	}
	outCount, err := r.u16()
	if err != nil {
		return DeliveryRecord{}, nil, err
	}
	if uint64(outCount) > model.LimitsV1().MaxDerivedDeliveries || r.remaining() < int(outCount)*4 {
		return DeliveryRecord{}, nil, errors.New("delivery outbox collection exceeds bounds or remaining bytes")
	}
	outboxes := make([]OutboxRecord, int(outCount))
	record.OutboxIDs = make([]model.DeliveryID, 0, int(outCount))
	for i := range outboxes {
		b, e := r.blob(MaxRecordPayloadBytes)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
		outboxes[i], e = decodeOutbox(b)
		if e != nil {
			return DeliveryRecord{}, nil, e
		}
		record.OutboxIDs = append(record.OutboxIDs, outboxes[i].ID)
	}
	if !r.done() {
		return DeliveryRecord{}, nil, errors.New("trailing delivery bytes")
	}
	return record, outboxes, nil
}

func encodeOutbox(record OutboxRecord) ([]byte, error) {
	message := protocol.TupleDelivery{DeliveryID: record.ID, Tuple: record.Tuple, Producer: record.Producer, Destination: record.Destination, Assignment: protocol.AssignmentSetIdentity{JobID: record.ID.Tuple.JobID, Revision: record.AssignmentRevision, Digest: record.AssignmentDigest}, Coordinator: record.CoordinatorEpoch}
	encoded, err := protocol.MarshalTupleDelivery(message)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(outboxRecordSchema)
	w.u8(boolByte(record.Completed))
	w.u8(boolByte(record.Accepted))
	w.u64(uint64(record.RetryDeadlineUnixNano))
	w.blob(encoded)
	return w.bytes(), nil
}

func decodeOutbox(payload []byte) (OutboxRecord, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil || schema != domainRecordSchema && schema != outboxRecordSchema {
		return OutboxRecord{}, errors.New("unsupported outbox schema")
	}
	complete, err := r.u8()
	if err != nil || complete > 1 {
		return OutboxRecord{}, errors.New("invalid outbox status")
	}
	var accepted uint8
	var deadline int64
	if schema == outboxRecordSchema {
		accepted, err = r.u8()
		if err != nil || accepted > 1 {
			return OutboxRecord{}, errors.New("invalid outbox retry phase")
		}
		raw, deadlineErr := r.u64()
		if deadlineErr != nil {
			return OutboxRecord{}, deadlineErr
		}
		deadline = int64(raw)
		if accepted == 1 && deadline == 0 {
			return OutboxRecord{}, errors.New("accepted outbox has unset retry deadline")
		}
	}
	encoded, err := r.blob(MaxRecordPayloadBytes)
	if err != nil || !r.done() {
		return OutboxRecord{}, errors.New("invalid outbox record")
	}
	message, err := protocol.UnmarshalTupleDelivery(encoded)
	if err != nil {
		return OutboxRecord{}, err
	}
	return OutboxRecord{ID: message.DeliveryID, Tuple: message.Tuple, Producer: message.Producer, Destination: message.Destination, AssignmentRevision: message.Assignment.Revision, AssignmentDigest: message.Assignment.Digest, CoordinatorEpoch: message.Coordinator, Completed: complete == 1, Accepted: accepted == 1, RetryDeadlineUnixNano: deadline}, nil
}

func encodeOutboxRetry(update outboxRetryUpdate) ([]byte, error) {
	if err := update.ID.Validate(); err != nil {
		return nil, err
	}
	if update.DeadlineUnixNano == 0 || update.AcceptTransition && !update.Accepted {
		return nil, errors.New("invalid outbox retry update")
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.u8(boolByte(update.Accepted))
	w.u8(boolByte(update.AcceptTransition))
	w.u64(uint64(update.DeadlineUnixNano))
	w.deliveryID(update.ID)
	return w.bytes(), nil
}

func decodeOutboxRetry(payload []byte) (outboxRetryUpdate, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return outboxRetryUpdate{}, err
	}
	accepted, err := r.u8()
	if err != nil || accepted > 1 {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry phase")
	}
	transition, err := r.u8()
	if err != nil || transition > 1 {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry transition")
	}
	deadline, err := r.u64()
	if err != nil {
		return outboxRetryUpdate{}, err
	}
	id, err := r.deliveryID()
	if err != nil || !r.done() {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry record")
	}
	update := outboxRetryUpdate{ID: id, Accepted: accepted == 1, AcceptTransition: transition == 1, DeadlineUnixNano: int64(deadline)}
	if update.DeadlineUnixNano == 0 || update.AcceptTransition && !update.Accepted {
		return outboxRetryUpdate{}, errors.New("invalid outbox retry update")
	}
	return update, nil
}

func encodeDeliveryIDPayload(id model.DeliveryID) ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.deliveryID(id)
	return w.bytes(), nil
}

func decodeDeliveryIDPayload(payload []byte) (model.DeliveryID, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return model.DeliveryID{}, err
	}
	id, err := r.deliveryID()
	if err != nil || !r.done() {
		return model.DeliveryID{}, errors.New("invalid delivery identity record")
	}
	return id, id.Validate()
}

func cloneTuple(v model.Tuple) model.Tuple {
	result := model.Tuple{Fields: make([]model.Field, len(v.Fields))}
	for i, f := range v.Fields {
		result.Fields[i] = f
		result.Fields[i].Value.Bytes = append([]byte(nil), f.Value.Bytes...)
	}
	return result
}

func cloneTuples(v []model.Tuple) []model.Tuple {
	result := make([]model.Tuple, len(v))
	for i := range v {
		result[i] = cloneTuple(v[i])
	}
	return result
}

func deliveryIndex(v []DeliveryRecord, id model.DeliveryID) int {
	for i := range v {
		if v[i].ID == id {
			return i
		}
	}
	return -1
}

func outboxIndex(v []OutboxRecord, id model.DeliveryID) int {
	for i := range v {
		if v[i].ID == id {
			return i
		}
	}
	return -1
}

func equalDeliveryDefinition(a, b DeliveryRecord) bool {
	if a.definitionDigest != ([32]byte{}) {
		digest := b.definitionDigest
		if digest == ([32]byte{}) {
			digest, _ = deliveryDefinitionDigest(b)
		}
		return digest == a.definitionDigest
	}
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.CoordinatorEpoch != b.CoordinatorEpoch || a.Reservation != b.Reservation {
		return false
	}
	aa, _ := model.MarshalTuple(a.Tuple)
	bb, _ := model.MarshalTuple(b.Tuple)
	return bytes.Equal(aa, bb)
}

func equalCompactedLogicalDefinition(a, b DeliveryRecord) bool {
	if a.ID != b.ID || a.Producer.Task != b.Producer.Task || a.Destination.Task != b.Destination.Task || a.Producer.SpecificationHash != b.Producer.SpecificationHash || a.Destination.SpecificationHash != b.Destination.SpecificationHash || a.Reservation != b.Reservation {
		return false
	}
	aa, err := model.MarshalTuple(a.Tuple)
	if err != nil {
		return false
	}
	bb, err := model.MarshalTuple(b.Tuple)
	return err == nil && bytes.Equal(aa, bb)
}

func equalTuples(a, b []model.Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		aa, _ := model.MarshalTuple(a[i])
		bb, _ := model.MarshalTuple(b[i])
		if !bytes.Equal(aa, bb) {
			return false
		}
	}
	return true
}

func equalOutboxDefinition(a, b OutboxRecord) bool {
	if a.ID != b.ID || a.Producer != b.Producer || a.Destination != b.Destination || a.AssignmentRevision != b.AssignmentRevision || a.AssignmentDigest != b.AssignmentDigest || a.CoordinatorEpoch != b.CoordinatorEpoch {
		return false
	}
	aa, _ := model.MarshalTuple(a.Tuple)
	bb, _ := model.MarshalTuple(b.Tuple)
	return bytes.Equal(aa, bb)
}

func tupleLess(a, b model.TupleID) bool {
	if c := bytes.Compare(a.JobID[:], b.JobID[:]); c != 0 {
		return c < 0
	}
	if a.SourceTask != b.SourceTask {
		return taskLess(a.SourceTask, b.SourceTask)
	}
	if a.SourceSequence != b.SourceSequence {
		return a.SourceSequence < b.SourceSequence
	}
	return bytes.Compare(a.PathDigest[:], b.PathDigest[:]) < 0
}

func deliveryIDLess(a, b model.DeliveryID) bool {
	if a.Tuple != b.Tuple {
		return tupleLess(a.Tuple, b.Tuple)
	}
	if a.EdgeID != b.EdgeID {
		return a.EdgeID < b.EdgeID
	}
	return taskLess(a.DestinationTask, b.DestinationTask)
}

func outboxesCanonical(outboxes []OutboxRecord) bool {
	for index := 1; index < len(outboxes); index++ {
		if !deliveryIDLess(outboxes[index-1].ID, outboxes[index].ID) {
			return false
		}
	}
	return true
}

func outboxIDsCanonical(ids []model.DeliveryID) bool {
	for index := 1; index < len(ids); index++ {
		if !deliveryIDLess(ids[index-1], ids[index]) {
			return false
		}
	}
	return true
}
