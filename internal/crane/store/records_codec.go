package store

import (
	"encoding/binary"
	"errors"

	"github.com/aadityakv/crane/internal/crane/model"
)

func encodeUint64Payload(v uint64) []byte {
	w := newRecordWriter()
	w.u16(eventAckRecordSchema)
	w.u64(v)
	return w.bytes()
}

func decodeUint64Payload(payload []byte) (uint64, error) {
	r := newRecordReader(payload)
	schema, err := r.u16()
	if err != nil {
		return 0, err
	}
	if schema != domainRecordSchema && schema != eventAckRecordSchema {
		return 0, errors.New("unsupported event acknowledgement schema")
	}
	v, err := r.u64()
	if err != nil || !r.done() {
		return 0, errors.New("invalid uint64 record")
	}
	return v, nil
}

func recordPayloadSchema(payload []byte) uint16 {
	if len(payload) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(payload[:2])
}

type recordWriter struct{ data []byte }

func newRecordWriter() *recordWriter { return &recordWriter{data: make([]byte, 0, 256)} }

func (w *recordWriter) bytes() []byte { return append([]byte(nil), w.data...) }

func (w *recordWriter) u8(v uint8) { w.data = append(w.data, v) }

func (w *recordWriter) u16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	w.data = append(w.data, b[:]...)
}

func (w *recordWriter) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	w.data = append(w.data, b[:]...)
}

func (w *recordWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.data = append(w.data, b[:]...)
}

func (w *recordWriter) fixed16(v [16]byte) { w.data = append(w.data, v[:]...) }

func (w *recordWriter) fixed32(v [32]byte) { w.data = append(w.data, v[:]...) }

func (w *recordWriter) blob(v []byte) { w.u32(uint32(len(v))); w.data = append(w.data, v...) }

func (w *recordWriter) job(v model.JobID) { w.fixed16([16]byte(v)) }

func (w *recordWriter) task(v model.TaskID) { w.job(v.JobID); w.u16(v.StageID); w.u16(v.Partition) }

func (w *recordWriter) tupleID(v model.TupleID) {
	w.job(v.JobID)
	w.task(v.SourceTask)
	w.u64(v.SourceSequence)
	w.fixed32(v.PathDigest)
}

func (w *recordWriter) deliveryID(v model.DeliveryID) {
	w.tupleID(v.Tuple)
	w.u16(v.EdgeID)
	w.task(v.DestinationTask)
}

func (w *recordWriter) epoch(v model.CoordinatorEpoch) {
	w.u64(v.Term)
	w.u64(v.BeginIndex)
	w.u16(v.Coordinator)
	w.fixed16(v.Nonce)
}

func (w *recordWriter) token(v model.AssignmentToken) {
	w.task(v.Task)
	w.u16(v.WorkerID)
	w.fixed16([16]byte(v.WorkerEpoch))
	w.u64(v.Attempt)
	w.fixed32(v.SpecificationHash)
	w.u64(v.AssignmentRevision)
}

func (w *recordWriter) replica(v model.ResultReplicaSet) {
	w.task(v.SinkTask)
	w.u16(v.PrimaryNodeID)
	w.u16(v.SecondaryNodeID)
	w.fixed16([16]byte(v.PrimaryEpoch))
	w.fixed16([16]byte(v.SecondaryEpoch))
}

func (w *recordWriter) completion(v model.CompletionReport) {
	w.job(v.JobID)
	w.u64(v.JobControlRevision)
	w.u64(v.AssignmentRevision)
	w.task(v.Source)
	w.token(v.Token)
	w.epoch(v.Epoch)
	w.u64(v.ExpectedCheckpointRevision)
	w.u64(v.Prior)
	w.u64(v.New)
	w.u64(v.EOF)
	w.u64(v.WorkerTransactionID)
	w.fixed32(v.Digest)
}

func (w *recordWriter) failure(v model.JobFailureReport) {
	w.job(v.JobID)
	w.u64(v.JobControlRevision)
	w.u64(v.AssignmentRevision)
	w.token(v.Task)
	w.epoch(v.Epoch)
	w.u64(v.TransactionID)
	w.u16(uint16(v.Code))
	w.fixed32(v.DetailDigest)
}

func (w *recordWriter) repairDefinition(v model.RepairResultPartitionDefinition) {
	w.fixed16(v.RepairID)
	w.epoch(v.CoordinatorEpoch)
	w.job(v.JobID)
	w.u64(v.AssignmentRevision)
	w.fixed32(v.AssignmentDigest)
	w.u16(v.SourceNodeID)
	w.fixed16([16]byte(v.SourceWorkerEpoch))
	w.u16(v.DestinationNodeID)
	w.fixed16([16]byte(v.DestinationWorkerEpoch))
	w.task(v.SinkTask)
	w.fixed32(v.SpecificationHash)
	w.u16(uint16(len(v.Checkpoints)))
	for _, c := range v.Checkpoints {
		w.task(c.Source)
		w.u64(c.Watermark)
	}
	w.fixed32(v.CheckpointDigest)
	w.fixed32(v.InventoryQueryDigest)
	w.u64(v.ExpectedRecordCount)
	w.u64(v.ExpectedTotalBytes)
	w.fixed32(v.ExpectedContentDigest)
}

type recordReader struct {
	data   []byte
	offset int
}

func newRecordReader(v []byte) *recordReader { return &recordReader{data: v} }

func (r *recordReader) remaining() int { return len(r.data) - r.offset }

func (r *recordReader) done() bool { return r.offset == len(r.data) }

func (r *recordReader) take(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, errors.New("truncated domain record")
	}
	v := r.data[r.offset : r.offset+n]
	r.offset += n
	return v, nil
}

func (r *recordReader) u8() (uint8, error) {
	b, e := r.take(1)
	if e != nil {
		return 0, e
	}
	return b[0], nil
}

func (r *recordReader) u16() (uint16, error) {
	b, e := r.take(2)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *recordReader) u32() (uint32, error) {
	b, e := r.take(4)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *recordReader) u64() (uint64, error) {
	b, e := r.take(8)
	if e != nil {
		return 0, e
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *recordReader) fixed16() ([16]byte, error) {
	b, e := r.take(16)
	var v [16]byte
	copy(v[:], b)
	return v, e
}

func (r *recordReader) fixed32() ([32]byte, error) {
	b, e := r.take(32)
	var v [32]byte
	copy(v[:], b)
	return v, e
}

func (r *recordReader) blob(max uint64) ([]byte, error) {
	n, e := r.u32()
	if e != nil {
		return nil, e
	}
	if uint64(n) > max {
		return nil, errors.New("domain blob exceeds bound")
	}
	b, e := r.take(int(n))
	if e != nil {
		return nil, e
	}
	return append([]byte(nil), b...), nil
}

func (r *recordReader) schema() error {
	v, e := r.u16()
	if e != nil || v != domainRecordSchema {
		return errors.New("unsupported domain record schema")
	}
	return nil
}

func (r *recordReader) job() (model.JobID, error) { v, e := r.fixed16(); return model.JobID(v), e }

func (r *recordReader) task() (model.TaskID, error) {
	job, e := r.job()
	if e != nil {
		return model.TaskID{}, e
	}
	stage, e := r.u16()
	if e != nil {
		return model.TaskID{}, e
	}
	partition, e := r.u16()
	return model.TaskID{JobID: job, StageID: stage, Partition: partition}, e
}

func (r *recordReader) tupleID() (model.TupleID, error) {
	job, e := r.job()
	if e != nil {
		return model.TupleID{}, e
	}
	source, e := r.task()
	if e != nil {
		return model.TupleID{}, e
	}
	seq, e := r.u64()
	if e != nil {
		return model.TupleID{}, e
	}
	digest, e := r.fixed32()
	return model.TupleID{JobID: job, SourceTask: source, SourceSequence: seq, PathDigest: digest}, e
}

func (r *recordReader) deliveryID() (model.DeliveryID, error) {
	tuple, e := r.tupleID()
	if e != nil {
		return model.DeliveryID{}, e
	}
	edge, e := r.u16()
	if e != nil {
		return model.DeliveryID{}, e
	}
	task, e := r.task()
	return model.DeliveryID{Tuple: tuple, EdgeID: edge, DestinationTask: task}, e
}

func (r *recordReader) epoch() (model.CoordinatorEpoch, error) {
	term, e := r.u64()
	if e != nil {
		return model.CoordinatorEpoch{}, e
	}
	index, e := r.u64()
	if e != nil {
		return model.CoordinatorEpoch{}, e
	}
	node, e := r.u16()
	if e != nil {
		return model.CoordinatorEpoch{}, e
	}
	nonce, e := r.fixed16()
	return model.CoordinatorEpoch{Term: term, BeginIndex: index, Coordinator: node, Nonce: nonce}, e
}

func (r *recordReader) token() (model.AssignmentToken, error) {
	task, e := r.task()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	node, e := r.u16()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	epoch, e := r.fixed16()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	attempt, e := r.u64()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	hash, e := r.fixed32()
	if e != nil {
		return model.AssignmentToken{}, e
	}
	revision, e := r.u64()
	return model.AssignmentToken{Task: task, WorkerID: node, WorkerEpoch: model.WorkerEpoch(epoch), Attempt: attempt, SpecificationHash: hash, AssignmentRevision: revision}, e
}

func (r *recordReader) replica() (model.ResultReplicaSet, error) {
	task, e := r.task()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	p, e := r.u16()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	s, e := r.u16()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	pe, e := r.fixed16()
	if e != nil {
		return model.ResultReplicaSet{}, e
	}
	se, e := r.fixed16()
	return model.ResultReplicaSet{SinkTask: task, PrimaryNodeID: p, SecondaryNodeID: s, PrimaryEpoch: model.WorkerEpoch(pe), SecondaryEpoch: model.WorkerEpoch(se)}, e
}

func (r *recordReader) completion() (model.CompletionReport, error) {
	var v model.CompletionReport
	var e error
	if v.JobID, e = r.job(); e != nil {
		return v, e
	}
	if v.JobControlRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.AssignmentRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.Source, e = r.task(); e != nil {
		return v, e
	}
	if v.Token, e = r.token(); e != nil {
		return v, e
	}
	if v.Epoch, e = r.epoch(); e != nil {
		return v, e
	}
	if v.ExpectedCheckpointRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.Prior, e = r.u64(); e != nil {
		return v, e
	}
	if v.New, e = r.u64(); e != nil {
		return v, e
	}
	if v.EOF, e = r.u64(); e != nil {
		return v, e
	}
	if v.WorkerTransactionID, e = r.u64(); e != nil {
		return v, e
	}
	v.Digest, e = r.fixed32()
	return v, e
}

func (r *recordReader) failure() (model.JobFailureReport, error) {
	var v model.JobFailureReport
	var e error
	if v.JobID, e = r.job(); e != nil {
		return v, e
	}
	if v.JobControlRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.AssignmentRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.Task, e = r.token(); e != nil {
		return v, e
	}
	if v.Epoch, e = r.epoch(); e != nil {
		return v, e
	}
	if v.TransactionID, e = r.u64(); e != nil {
		return v, e
	}
	code, e := r.u16()
	if e != nil {
		return v, e
	}
	v.Code = model.FailureCode(code)
	v.DetailDigest, e = r.fixed32()
	return v, e
}

func (r *recordReader) repairDefinition() (model.RepairResultPartitionDefinition, error) {
	var v model.RepairResultPartitionDefinition
	var e error
	if v.RepairID, e = r.fixed16(); e != nil {
		return v, e
	}
	if v.CoordinatorEpoch, e = r.epoch(); e != nil {
		return v, e
	}
	if v.JobID, e = r.job(); e != nil {
		return v, e
	}
	if v.AssignmentRevision, e = r.u64(); e != nil {
		return v, e
	}
	if v.AssignmentDigest, e = r.fixed32(); e != nil {
		return v, e
	}
	if v.SourceNodeID, e = r.u16(); e != nil {
		return v, e
	}
	source, e := r.fixed16()
	if e != nil {
		return v, e
	}
	v.SourceWorkerEpoch = model.WorkerEpoch(source)
	if v.DestinationNodeID, e = r.u16(); e != nil {
		return v, e
	}
	destination, e := r.fixed16()
	if e != nil {
		return v, e
	}
	v.DestinationWorkerEpoch = model.WorkerEpoch(destination)
	if v.SinkTask, e = r.task(); e != nil {
		return v, e
	}
	if v.SpecificationHash, e = r.fixed32(); e != nil {
		return v, e
	}
	count, e := r.u16()
	if e != nil {
		return v, e
	}
	if count > model.WorkerControlMaxCheckpointsV1 {
		return v, errors.New("repair checkpoints exceed bound")
	}
	if r.remaining() < int(count)*28+112 {
		return v, errors.New("repair checkpoints exceed remaining bytes")
	}
	v.Checkpoints = make([]model.SourceCheckpoint, int(count))
	for i := range v.Checkpoints {
		if v.Checkpoints[i].Source, e = r.task(); e != nil {
			return v, e
		}
		if v.Checkpoints[i].Watermark, e = r.u64(); e != nil {
			return v, e
		}
	}
	if v.CheckpointDigest, e = r.fixed32(); e != nil {
		return v, e
	}
	if v.InventoryQueryDigest, e = r.fixed32(); e != nil {
		return v, e
	}
	if v.ExpectedRecordCount, e = r.u64(); e != nil {
		return v, e
	}
	if v.ExpectedTotalBytes, e = r.u64(); e != nil {
		return v, e
	}
	v.ExpectedContentDigest, e = r.fixed32()
	return v, e
}

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
