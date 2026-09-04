package store

import (
	"bytes"
	"errors"
	"math"

	"github.com/aadityakv/crane/internal/crane/model"
)

func applyResult(work *RecoveredWork, result StoredResult) error {
	assignment, ok := findAssignment(work, result.Record.TupleID.JobID)
	if !ok {
		return errors.New("result references unknown assignment")
	}
	if err := result.Provenance.Validate(result.Record); err != nil {
		return err
	}
	if result.Provenance.CoordinatorEpoch != work.Fence || result.Provenance.AssignmentRevision != assignment.Assignment.Revision || result.Provenance.AssignmentDigest != assignment.Assignment.Digest {
		return errors.New("result provenance assignment fence mismatch")
	}
	want, ok := findReplica(assignment.Assignment, result.Record.SinkTask)
	if !ok || want != result.Provenance.ReplicaSet {
		return errors.New("result replica set mismatch")
	}
	if result.canonical == nil {
		encoded, err := model.MarshalResultRecord(result.Record)
		if err != nil {
			return err
		}
		result.canonical = encoded
	}
	if err := ensureWorkIndexes(work); err != nil {
		return err
	}
	key := resultKey{SinkTask: result.Record.SinkTask, TupleID: result.Record.TupleID}
	priorResult := findResultNode(work.indexes.results, key)
	if priorResult != nil {
		if equalStoredResult(priorResult.value, result) {
			return nil
		}
		if !rebindableResultProvenance(priorResult.value, result) {
			return model.ErrIdentityReuse
		}
		// Copy-provenance rebind (Task 24 defect #4 ruling): the identical
		// logical record retained under a superseded envelope re-binds to the
		// current pair (validated above against the current fence, assignment
		// and replica set). The logical record and its byte accounting are
		// unchanged; the prospective tree is path-copied like every insert.
		result.Record.Value = append([]byte(nil), result.Record.Value...)
		work.indexes.results = replaceResultNode(work.indexes.results, key, result)
		return nil
	}
	entryBytes, err := resultArtifactEntryBytes(uint64(len(result.canonical)))
	if err != nil {
		return err
	}
	jobBytes := work.indexes.resultBytesByJob[result.Record.TupleID.JobID]
	if jobBytes > model.LimitsV1().MaxResultRecordsBytesPerJob || entryBytes > model.LimitsV1().MaxResultRecordsBytesPerJob-jobBytes {
		return ErrCapacity
	}
	if work.indexes.resultCount >= maxStoredResultCount() {
		return ErrCapacity
	}
	result.Record.Value = append([]byte(nil), result.Record.Value...)
	inserted, err := insertResultNode(work.indexes.results, &resultNode{key: key, value: result, height: 1})
	if err != nil {
		return err
	}
	work.indexes.results = inserted
	work.indexes.resultBytesByJob[result.Record.TupleID.JobID] = jobBytes + entryBytes
	work.indexes.resultCount++
	return nil
}

// UpsertResult idempotently persists one logical record and exact current-copy provenance.
func (store *Store) UpsertResult(record model.ResultRecord, provenance model.ResultCopyProvenance) error {
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return ErrClosed
	}
	if store.failed {
		store.mu.Unlock()
		return ErrUnavailable
	}
	if !provenanceTargets(provenance, store.state.Identity.NodeID, store.state.WorkerEpoch) {
		store.mu.Unlock()
		return errors.New("result provenance does not designate this worker incarnation")
	}
	payload, err := encodeStoredResult(StoredResult{Record: record, Provenance: provenance})
	if err != nil {
		store.mu.Unlock()
		return err
	}
	tx := Transaction{Records: []Record{{Type: recordResult, Payload: payload}}}
	prospective, err := store.reduceWorkLocked(tx)
	if err == nil {
		err = store.commitWorkLocked(tx, prospective)
	}
	if err == nil {
		store.durable(BoundaryResultUpserted)
	}
	store.mu.Unlock()
	return err
}

func encodeStoredResult(result StoredResult) ([]byte, error) {
	if err := result.Provenance.Validate(result.Record); err != nil {
		return nil, err
	}
	logical, err := model.MarshalResultRecord(result.Record)
	if err != nil {
		return nil, err
	}
	w := newRecordWriter()
	w.u16(domainRecordSchema)
	w.blob(logical)
	w.u64(result.Provenance.AssignmentRevision)
	w.fixed32(result.Provenance.AssignmentDigest)
	w.replica(result.Provenance.ReplicaSet)
	w.u8(uint8(result.Provenance.DestinationRole))
	w.epoch(result.Provenance.CoordinatorEpoch)
	return w.bytes(), nil
}

func decodeStoredResult(payload []byte) (StoredResult, error) {
	r := newRecordReader(payload)
	if err := r.schema(); err != nil {
		return StoredResult{}, err
	}
	logical, err := r.blob(MaxRecordPayloadBytes)
	if err != nil {
		return StoredResult{}, err
	}
	record, err := model.UnmarshalResultRecord(logical)
	if err != nil {
		return StoredResult{}, err
	}
	var p model.ResultCopyProvenance
	if p.AssignmentRevision, err = r.u64(); err != nil {
		return StoredResult{}, err
	}
	if p.AssignmentDigest, err = r.fixed32(); err != nil {
		return StoredResult{}, err
	}
	if p.ReplicaSet, err = r.replica(); err != nil {
		return StoredResult{}, err
	}
	role, err := r.u8()
	p.DestinationRole = model.ResultReplicaRole(role)
	if err != nil {
		return StoredResult{}, err
	}
	if p.CoordinatorEpoch, err = r.epoch(); err != nil || !r.done() {
		return StoredResult{}, errors.New("invalid result record")
	}
	return StoredResult{Record: record, Provenance: p, canonical: logical}, p.Validate(record)
}

// rebindableResultProvenance reports whether an incoming result may re-bind
// the copy provenance of a retained prior copy: the logical record must be
// byte-identical and the prior provenance strictly historical against the
// incoming (already current-validated) one — a lower assignment revision, or
// the same revision/replica set/role under a coordinator epoch ordered
// strictly before. Any other difference is an identity reuse.
func rebindableResultProvenance(prior, incoming StoredResult) bool {
	priorBytes := prior.canonical
	if priorBytes == nil {
		priorBytes, _ = model.MarshalResultRecord(prior.Record)
	}
	incomingBytes := incoming.canonical
	if incomingBytes == nil {
		incomingBytes, _ = model.MarshalResultRecord(incoming.Record)
	}
	if len(priorBytes) == 0 || !bytes.Equal(priorBytes, incomingBytes) {
		return false
	}
	return ResultProvenanceOrderedBefore(prior.Provenance, incoming.Provenance)
}

// ResultProvenanceOrderedBefore reports whether prior is a strictly superseded
// copy envelope of current: a lower assignment revision, or the identical
// revision, digest, replica set and role under a coordinator epoch ordered
// strictly before current's.
func ResultProvenanceOrderedBefore(prior, current model.ResultCopyProvenance) bool {
	if prior.AssignmentRevision < current.AssignmentRevision {
		return true
	}
	return prior.AssignmentRevision == current.AssignmentRevision && prior.AssignmentDigest == current.AssignmentDigest &&
		prior.ReplicaSet == current.ReplicaSet && prior.DestinationRole == current.DestinationRole &&
		compareEpochOrder(prior.CoordinatorEpoch, current.CoordinatorEpoch) < 0
}

// replaceResultNode returns a path-copied tree in which the node holding key
// carries value; shape and heights are unchanged. The key must exist.
func replaceResultNode(root *resultNode, key resultKey, value StoredResult) *resultNode {
	if root == nil {
		return nil
	}
	copyRoot := *root
	switch comparison := compareResultKey(key, root.key); {
	case comparison < 0:
		copyRoot.left = replaceResultNode(root.left, key, value)
	case comparison > 0:
		copyRoot.right = replaceResultNode(root.right, key, value)
	default:
		copyRoot.value = value
	}
	return &copyRoot
}

func compareEpochOrder(a, b model.CoordinatorEpoch) int {
	if a.Term < b.Term {
		return -1
	}
	if a.Term > b.Term {
		return 1
	}
	if a.BeginIndex < b.BeginIndex {
		return -1
	}
	if a.BeginIndex > b.BeginIndex {
		return 1
	}
	return 0
}

func ensureWorkIndexes(work *RecoveredWork) error {
	if work.indexes != nil {
		return nil
	}
	indexes := &workIndexes{resultBytesByJob: make(map[model.JobID]uint64)}
	for index := range work.Results {
		result := work.Results[index]
		if result.canonical == nil {
			encoded, err := model.MarshalResultRecord(result.Record)
			if err != nil {
				return err
			}
			result.canonical = encoded
		}
		key := resultKey{SinkTask: result.Record.SinkTask, TupleID: result.Record.TupleID}
		if findResultNode(indexes.results, key) != nil {
			return model.ErrIdentityReuse
		}
		if indexes.resultCount >= maxStoredResultCount() {
			return ErrCapacity
		}
		inserted, err := insertResultNode(indexes.results, &resultNode{key: key, value: result, height: 1})
		if err != nil {
			return err
		}
		indexes.results = inserted
		indexes.resultCount++
		entryBytes, err := resultArtifactEntryBytes(uint64(len(result.canonical)))
		if err != nil {
			return err
		}
		prior := indexes.resultBytesByJob[result.Record.TupleID.JobID]
		if prior > math.MaxUint64-entryBytes {
			return ErrCapacity
		}
		indexes.resultBytesByJob[result.Record.TupleID.JobID] = prior + entryBytes
	}
	work.Results = nil
	work.indexes = indexes
	return nil
}

func cloneWorkIndexes(indexes *workIndexes) *workIndexes {
	if indexes == nil {
		return nil
	}
	result := &workIndexes{results: indexes.results, resultBytesByJob: make(map[model.JobID]uint64, len(indexes.resultBytesByJob)), resultCount: indexes.resultCount}
	for job, total := range indexes.resultBytesByJob {
		result.resultBytesByJob[job] = total
	}
	return result
}

func compareResultKey(a, b resultKey) int {
	if a.SinkTask != b.SinkTask {
		if taskLess(a.SinkTask, b.SinkTask) {
			return -1
		}
		return 1
	}
	if a.TupleID == b.TupleID {
		return 0
	}
	if tupleLess(a.TupleID, b.TupleID) {
		return -1
	}
	return 1
}

func findResultNode(root *resultNode, key resultKey) *resultNode {
	for root != nil {
		switch comparison := compareResultKey(key, root.key); {
		case comparison < 0:
			root = root.left
		case comparison > 0:
			root = root.right
		default:
			return root
		}
	}
	return nil
}

func insertResultNode(root, inserted *resultNode) (*resultNode, error) {
	if root == nil {
		return inserted, nil
	}
	copyRoot := *root
	comparison := compareResultKey(inserted.key, root.key)
	if comparison == 0 {
		return nil, model.ErrIdentityReuse
	}
	var err error
	if comparison < 0 {
		copyRoot.left, err = insertResultNode(root.left, inserted)
	} else {
		copyRoot.right, err = insertResultNode(root.right, inserted)
	}
	if err != nil {
		return nil, err
	}
	return rebalanceResultNode(&copyRoot)
}

func rebalanceResultNode(node *resultNode) (*resultNode, error) {
	if err := updateResultNodeHeight(node); err != nil {
		return nil, err
	}
	balance := int(resultNodeHeight(node.left)) - int(resultNodeHeight(node.right))
	if balance > 1 {
		if resultNodeHeight(node.left.left) < resultNodeHeight(node.left.right) {
			rotated, err := rotateResultNodeLeft(node.left)
			if err != nil {
				return nil, err
			}
			node.left = rotated
		}
		return rotateResultNodeRight(node)
	}
	if balance < -1 {
		if resultNodeHeight(node.right.right) < resultNodeHeight(node.right.left) {
			rotated, err := rotateResultNodeRight(node.right)
			if err != nil {
				return nil, err
			}
			node.right = rotated
		}
		return rotateResultNodeLeft(node)
	}
	return node, nil
}

func rotateResultNodeRight(root *resultNode) (*resultNode, error) {
	if root == nil || root.left == nil {
		return nil, errors.New("invalid result index right rotation")
	}
	newRoot, moved := *root.left, root.left.right
	oldRoot := *root
	oldRoot.left = moved
	if err := updateResultNodeHeight(&oldRoot); err != nil {
		return nil, err
	}
	newRoot.right = &oldRoot
	if err := updateResultNodeHeight(&newRoot); err != nil {
		return nil, err
	}
	return &newRoot, nil
}

func rotateResultNodeLeft(root *resultNode) (*resultNode, error) {
	if root == nil || root.right == nil {
		return nil, errors.New("invalid result index left rotation")
	}
	newRoot, moved := *root.right, root.right.left
	oldRoot := *root
	oldRoot.right = moved
	if err := updateResultNodeHeight(&oldRoot); err != nil {
		return nil, err
	}
	newRoot.left = &oldRoot
	if err := updateResultNodeHeight(&newRoot); err != nil {
		return nil, err
	}
	return &newRoot, nil
}

func resultNodeHeight(node *resultNode) uint16 {
	if node == nil {
		return 0
	}
	return node.height
}

func updateResultNodeHeight(node *resultNode) error {
	height := resultNodeHeight(node.left)
	if right := resultNodeHeight(node.right); right > height {
		height = right
	}
	if height == math.MaxUint16 {
		return ErrCapacity
	}
	node.height = height + 1
	return nil
}

func maxStoredResultCount() uint64 {
	jobs := model.LimitsV1().MaxRetainedJobs
	if jobs > math.MaxUint64/model.ResultArtifactMaxRecordCountV1 {
		return math.MaxUint64
	}
	return jobs * model.ResultArtifactMaxRecordCountV1
}

const resultArtifactEntryPrefixBytes uint64 = 4

func resultArtifactEntryBytes(logicalBytes uint64) (uint64, error) {
	entryBytes, ok := checkedAdd(logicalBytes, resultArtifactEntryPrefixBytes)
	if !ok || entryBytes < model.ResultArtifactMinRecordBytesV1 || entryBytes > model.ResultArtifactMaxRecordBytesV1 {
		return 0, errors.New("result logical bytes outside artifact entry bounds")
	}
	return entryBytes, nil
}

func appendOwnedResults(indexes *workIndexes, destination *[]StoredResult) {
	if indexes == nil {
		return
	}
	stack := make([]*resultNode, 0, resultNodeHeight(indexes.results))
	current := indexes.results
	for current != nil || len(stack) != 0 {
		for current != nil {
			stack = append(stack, current)
			current = current.left
		}
		current = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		owned := current.value
		owned.Record.Value = append([]byte(nil), owned.Record.Value...)
		owned.canonical = nil
		*destination = append(*destination, owned)
		current = current.right
	}
}

func visitResults(work RecoveredWork, visit func(StoredResult) bool) bool {
	if work.indexes == nil {
		for _, result := range work.Results {
			if !visit(result) {
				return false
			}
		}
		return true
	}
	stack := make([]*resultNode, 0, resultNodeHeight(work.indexes.results))
	current := work.indexes.results
	for current != nil || len(stack) != 0 {
		for current != nil {
			stack, current = append(stack, current), current.left
		}
		current = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !visit(current.value) {
			return false
		}
		current = current.right
	}
	return true
}

func equalStoredResult(a, b StoredResult) bool {
	aa := a.canonical
	if aa == nil {
		aa, _ = model.MarshalResultRecord(a.Record)
	}
	bb := b.canonical
	if bb == nil {
		bb, _ = model.MarshalResultRecord(b.Record)
	}
	return bytes.Equal(aa, bb) && a.Provenance == b.Provenance
}
