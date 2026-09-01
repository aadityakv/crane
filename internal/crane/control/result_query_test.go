// Package control owns Crane's public +6 control-plane logic. This file
// tests the memory-bounded global result-page query engine.
package control

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/crane/worker"
)

// queryTopology is one source feeding the requested number of collect
// partitions.
func queryTopology(sinkPartitions uint16) model.TopologySpec {
	return model.TopologySpec{
		SchemaVersion: 1, Name: "task-20-query", RegistryFingerprint: model.RegistryFingerprint(),
		Stages: []model.StageSpec{
			{StageID: 1, Name: "numbers", Role: model.Source, Parallelism: 1, Operator: model.OperatorSpec{
				Name: "range", Version: 1,
				Settings: []model.Setting{{Key: "end_exclusive", Value: "4"}, {Key: "start", Value: "0"}},
			}},
			{StageID: 2, Name: "sink", Role: model.Sink, Parallelism: sinkPartitions, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
		},
		Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.Shuffle}},
	}
}

func queryCommandID(domain string, parts ...[]byte) state.InternalCommandID {
	var id state.InternalCommandID
	copy(id[:], domain)
	position := len(domain)
	for _, part := range parts {
		for _, value := range part {
			if position >= len(id) {
				break
			}
			id[position] ^= value
			position++
		}
	}
	if id == (state.InternalCommandID{}) {
		id[0] = 1
	}
	return id
}

// seedMachine applies one command directly and requires success.
func seedMachine(t *testing.T, machine *state.Machine, index uint64, command any) {
	t.Helper()
	encoded, err := state.MarshalCommand(command)
	if err != nil {
		t.Fatalf("marshal seed command: %v", err)
	}
	resultBytes, err := machine.Apply(index, 1, encoded)
	if err != nil {
		t.Fatalf("apply seed command: %v", err)
	}
	result, err := state.UnmarshalCommandResult(resultBytes)
	if err != nil {
		t.Fatalf("decode seed result: %v", err)
	}
	if result.Code != state.ResultSuccess {
		t.Fatalf("seed command rejected: %#v", result)
	}
}

// querySeed selects one fixture's sink count, sealed-partition count,
// terminal success, and per-partition record builder.
type querySeed struct {
	sinkPartitions uint16
	sealPartitions int
	succeed        bool
	recordsFor     func(sink model.TaskID, partition uint16, partitions uint16) []model.ResultRecord
}

// interleavedRecords assigns every partitions-th source sequence to one
// partition so merged partitions interleave in global TupleID order.
func interleavedRecords(t *testing.T, job model.JobID, topology model.ValidatedTopology) func(model.TaskID, uint16, uint16) []model.ResultRecord {
	t.Helper()
	source := model.TaskID{JobID: job, StageID: 1, Partition: 0}
	return func(sink model.TaskID, partition, partitions uint16) []model.ResultRecord {
		records := make([]model.ResultRecord, 0)
		for sequence := uint64(1); ; sequence++ {
			tuple, exists, err := model.SourceTuple(topology, source, sequence)
			if err != nil {
				t.Fatal(err)
			}
			if !exists {
				break
			}
			if sequence%uint64(partitions) != uint64(partition) {
				continue
			}
			encoded, err := model.MarshalTuple(tuple)
			if err != nil {
				t.Fatal(err)
			}
			record, err := model.NewResultRecord(model.DeriveSourceTupleID(job, source, sequence), sink, topology.Digest(), encoded)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
		return records
	}
}

// queryFixture is one job driven through the replicated lifecycle with
// sealed two-copy manifests.
type queryFixture struct {
	machine    *state.Machine
	job        model.JobID
	topology   model.ValidatedTopology
	assignment model.AssignmentSet
	records    map[model.TaskID][]model.ResultRecord
	artifacts  map[model.TaskID]protocol.ResultArtifact
	digest     [32]byte
}

// seedQueryFixture drives one job through the replicated lifecycle sealing
// the first seed.sealPartitions partitions and optionally succeeding.
func seedQueryFixture(t *testing.T, seed querySeed) queryFixture {
	t.Helper()
	machine := state.NewMachine()
	view := machine.View()
	nonce := [16]byte{0x44, byte(view.CoordinatorRevision + 1)}
	begin, err := state.NewBeginCoordinatorEpoch(queryCommandID("begin", nonce[:]), view.CoordinatorRevision, 1, nonce)
	if err != nil {
		t.Fatal(err)
	}
	seedMachine(t, machine, 1, begin)
	epoch := machine.View().CoordinatorEpoch
	for _, node := range []uint16{2, 3} {
		record := state.WorkerRecord{
			NodeID: node, Epoch: model.WorkerEpoch{byte(node)}, State: state.WorkerEligible, Revision: 1, Slots: 8,
			ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint(),
		}
		register, err := state.NewRegisterWorker(queryCommandID("register", []byte{byte(node), byte(node >> 8)}), 0, record, epoch)
		if err != nil {
			t.Fatal(err)
		}
		seedMachine(t, machine, uint64(node), register)
	}
	topology, err := model.ValidateTopology(queryTopology(seed.sinkPartitions))
	if err != nil {
		t.Fatal(err)
	}
	submit, err := state.NewSubmitJob(model.ClientRequestID{ClientID: model.ClientID{0x71}, Sequence: 1}, topology.Spec(), epoch)
	if err != nil {
		t.Fatal(err)
	}
	seedMachine(t, machine, 4, submit)
	job := submit.JobID()
	placements := []model.WorkerPlacement{{NodeID: 2, WorkerEpoch: model.WorkerEpoch{2}, SlotCapacity: 8}, {NodeID: 3, WorkerEpoch: model.WorkerEpoch{3}, SlotCapacity: 8}}
	assignment, err := model.BuildAssignmentSet(job, topology.Digest(), 1, topology, placements)
	if err != nil {
		t.Fatal(err)
	}
	var sources []model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task.StageID == 1 {
			sources = append(sources, token)
		}
	}
	for _, token := range sources {
		eof, err := model.SourceEOF(topology, token.Task)
		if err != nil {
			t.Fatal(err)
		}
		recordEOF, err := state.NewRecordSourceEOF(queryCommandID("eof", []byte{byte(token.Task.Partition)}), 0, token.Task, eof, epoch)
		if err != nil {
			t.Fatal(err)
		}
		seedMachine(t, machine, 5, recordEOF)
	}
	install, err := state.NewInstallAssignments(queryCommandID("install", job[:]), 1, assignment, epoch)
	if err != nil {
		t.Fatal(err)
	}
	seedMachine(t, machine, 6, install)
	run, err := state.NewTransitionJob(queryCommandID("run", job[:]), 2, job, state.JobDeploying, state.JobRunning, epoch)
	if err != nil {
		t.Fatal(err)
	}
	seedMachine(t, machine, 7, run)
	record := machine.View().Jobs[0]
	transaction := uint64(1)
	for _, token := range sources {
		eof := record.SourceEOFs[token.Task].EOF
		report := model.CompletionReport{
			JobID: job, JobControlRevision: record.JobControlRevision, AssignmentRevision: assignment.Revision,
			Source: token.Task, Token: token, Epoch: epoch,
			ExpectedCheckpointRevision: 0, Prior: 0, New: eof, EOF: eof, WorkerTransactionID: transaction,
		}
		report.Digest = model.CompletionReportDigest(report)
		advance, err := state.NewAdvanceCheckpoint(queryCommandID("advance", []byte{byte(token.Task.Partition)}), 0, report, epoch)
		if err != nil {
			t.Fatal(err)
		}
		seedMachine(t, machine, 8, advance)
		transaction++
	}
	drain, err := state.NewTransitionJob(queryCommandID("drain", job[:]), 3, job, state.JobRunning, state.JobDraining, epoch)
	if err != nil {
		t.Fatal(err)
	}
	seedMachine(t, machine, 9, drain)

	recordsFor := seed.recordsFor
	if recordsFor == nil {
		recordsFor = interleavedRecords(t, job, topology)
	}
	records := make(map[model.TaskID][]model.ResultRecord)
	artifacts := make(map[model.TaskID]protocol.ResultArtifact)
	manifests := make([]state.ResultManifest, 0)
	index := uint64(10)
	for position, replica := range assignment.ResultReplicas {
		list := recordsFor(replica.SinkTask, replica.SinkTask.Partition, seed.sinkPartitions)
		artifact, _, err := worker.SealResultPartition(job, replica.SinkTask, topology.Digest(), list)
		if err != nil {
			t.Fatal(err)
		}
		records[replica.SinkTask] = list
		artifacts[replica.SinkTask] = artifact
		if position >= seed.sealPartitions {
			continue
		}
		manifest := state.ResultManifest{
			JobID: job, SinkTask: replica.SinkTask, ManifestRevision: 1, SpecificationHash: topology.Digest(),
			RecordCount: artifact.RecordCount, TotalBytes: artifact.TotalLength, Checksum: artifact.Checksum, Replicas: replica,
		}
		seal, err := state.NewSealManifest(queryCommandID("seal", job[:], []byte{byte(replica.SinkTask.Partition)}), 0, manifest, epoch)
		if err != nil {
			t.Fatal(err)
		}
		seedMachine(t, machine, index, seal)
		index++
		manifests = append(manifests, manifest)
	}
	if seed.succeed && len(manifests) == len(assignment.ResultReplicas) {
		succeeded, err := state.NewTransitionJob(queryCommandID("succeed", job[:]), 4, job, state.JobDraining, state.JobSucceeded, epoch)
		if err != nil {
			t.Fatal(err)
		}
		seedMachine(t, machine, index, succeeded)
	}
	return queryFixture{machine: machine, job: job, topology: topology, assignment: assignment, records: records, artifacts: artifacts, digest: ResultManifestSetDigest(manifests)}
}

// fakeFetcher serves sealed artifact streams from memory with per-sink
// failing replicas.
type fakeFetcher struct {
	streams  map[[32]byte][]byte
	failNode map[model.TaskID]uint16
	requests []protocol.ResultFetchRequest
}

func newFakeFetcher(fixture queryFixture) *fakeFetcher {
	streams := make(map[[32]byte][]byte)
	for sink, list := range fixture.records {
		_, stream, err := worker.SealResultPartition(fixture.job, sink, fixture.topology.Digest(), list)
		if err != nil {
			panic(err)
		}
		streams[fixture.artifacts[sink].Checksum] = stream
	}
	return &fakeFetcher{streams: streams, failNode: make(map[model.TaskID]uint16)}
}

func (fetcher *fakeFetcher) OpenPartition(_ context.Context, request protocol.ResultFetchRequest) (RecordStream, error) {
	fetcher.requests = append(fetcher.requests, request)
	if fetcher.failNode[request.Artifact.SinkTask] == request.ReplicaNodeID {
		return nil, errors.New("injected replica failure")
	}
	stream, ok := fetcher.streams[request.Artifact.Checksum]
	if !ok {
		return nil, errors.New("no sealed copy")
	}
	return &sliceRecordStream{data: append([]byte(nil), stream...), offset: 0}, nil
}

type sliceRecordStream struct {
	data   []byte
	offset int
}

func (stream *sliceRecordStream) Next(context.Context) (model.ResultRecord, error) {
	if stream.offset >= len(stream.data) {
		return model.ResultRecord{}, io.EOF
	}
	if len(stream.data)-stream.offset < 4 {
		return model.ResultRecord{}, errors.New("truncated entry prefix")
	}
	length := int(stream.data[stream.offset])<<24 | int(stream.data[stream.offset+1])<<16 | int(stream.data[stream.offset+2])<<8 | int(stream.data[stream.offset+3])
	if length <= 0 || length > len(stream.data)-stream.offset-4 {
		return model.ResultRecord{}, errors.New("entry length outside bounds")
	}
	record, err := model.UnmarshalResultRecord(stream.data[stream.offset+4 : stream.offset+4+length])
	if err != nil {
		return model.ResultRecord{}, err
	}
	stream.offset += 4 + length
	return record, nil
}

func (stream *sliceRecordStream) Close() error { return nil }

var _ ResultFetcher = (*fakeFetcher)(nil)

func pageRequest(fixture queryFixture, pageBytes uint32) protocol.ResultPageRequest {
	return protocol.ResultPageRequest{JobID: fixture.job, ManifestDigest: fixture.digest, PageBytes: pageBytes}
}

func TestPageMergesPartitionsInGlobalTupleOrder(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fetcher := newFakeFetcher(fixture)
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}
	request := pageRequest(fixture, protocol.MaxResultPageBytes)
	response, err := engine.Page(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.ValidateResultPageResponseCorrelation(request, response); err != nil {
		t.Fatalf("correlation: %v", err)
	}
	want := allGlobalRecords(t, fixture)
	if len(response.Records) != len(want) || !response.End {
		t.Fatalf("records=%d end=%t want=%d", len(response.Records), response.End, len(want))
	}
	for index := range want {
		if response.Records[index].TupleID != want[index].TupleID || !bytes.Equal(response.Records[index].Value, want[index].Value) {
			t.Fatalf("record %d=%+v want=%+v", index, response.Records[index].TupleID, want[index].TupleID)
		}
	}
	if response.Records[0].TupleID.SourceSequence != 1 {
		t.Fatalf("global order wrong: first=%+v", response.Records[0].TupleID)
	}
}

func TestPageIsStatelessAcrossCursorPagesWithoutSplittingRecords(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fetcher := newFakeFetcher(fixture)
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}

	firstEntry, err := protocol.EncodedResultPageRecordBytes(firstGlobalRecord(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	request := pageRequest(fixture, uint32(len(firstEntry)))
	collected := make([]model.ResultRecord, 0)
	pages := 0
	for {
		response, err := engine.Page(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := protocol.ValidateResultPageResponseCorrelation(request, response); err != nil {
			t.Fatalf("correlation: %v", err)
		}
		collected = append(collected, response.Records...)
		pages++
		if response.End {
			break
		}
		if len(response.Records) != 1 {
			t.Fatalf("page split or dropped records: %d", len(response.Records))
		}
		request = protocol.ResultPageRequest{JobID: request.JobID, ManifestDigest: request.ManifestDigest, HasLastTuple: true, Last: response.NextLast, PageBytes: request.PageBytes}
	}
	want := allGlobalRecords(t, fixture)
	if pages < 2 || len(collected) != len(want) {
		t.Fatalf("pages=%d collected=%d want=%d", pages, len(collected), len(want))
	}
	for index := range want {
		if collected[index].TupleID != want[index].TupleID {
			t.Fatalf("global order broke at %d: %+v want %+v", index, collected[index].TupleID, want[index].TupleID)
		}
	}
}

func TestPageReturnsPageLimitTooSmallWithoutAdvancement(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 1, sealPartitions: 1, succeed: true})
	fetcher := newFakeFetcher(fixture)
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}
	first := firstGlobalRecord(t, fixture)
	entry, err := protocol.EncodedResultPageRecordBytes(first)
	if err != nil {
		t.Fatal(err)
	}
	request := pageRequest(fixture, uint32(len(entry)-1))
	_, err = engine.Page(context.Background(), request)
	var tooSmall PageLimitTooSmallError
	if !errors.As(err, &tooSmall) {
		t.Fatalf("err=%v want PageLimitTooSmallError", err)
	}
	if tooSmall.RequiredBytes != uint32(len(entry)) {
		t.Fatalf("required=%d want=%d", tooSmall.RequiredBytes, len(entry))
	}
	// The same cursor still serves the record under a sufficient budget.
	response, err := engine.Page(context.Background(), pageRequest(fixture, uint32(len(entry))))
	if err != nil || len(response.Records) != 1 || response.Records[0].TupleID != first.TupleID {
		t.Fatalf("advancement after rejection: err=%v records=%d", err, len(response.Records))
	}
}

func TestPageRejectsCrossPartitionDuplicateAsCorruption(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	// Serve the first partition's records under the second partition's
	// committed artifact identity: the same TupleID reaching two partitions
	// is corruption even with identical bytes.
	second := fixture.assignment.ResultReplicas[1].SinkTask
	committed := fixture.artifacts[second]
	duplicated := make([]model.ResultRecord, 0)
	for _, record := range fixture.records[fixture.assignment.ResultReplicas[0].SinkTask] {
		clone, err := model.NewResultRecord(record.TupleID, second, record.SpecificationHash, record.Value)
		if err != nil {
			t.Fatal(err)
		}
		duplicated = append(duplicated, clone)
	}
	_, duplicatedStream, err := worker.SealResultPartition(fixture.job, second, fixture.topology.Digest(), duplicated)
	if err != nil {
		t.Fatal(err)
	}
	fetcher := newFakeFetcher(fixture)
	fetcher.streams[committed.Checksum] = duplicatedStream
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}
	_, err = engine.Page(context.Background(), pageRequest(fixture, protocol.MaxResultPageBytes))
	if !errors.Is(err, ErrCorruptResultSet) {
		t.Fatalf("err=%v want ErrCorruptResultSet", err)
	}
}

func TestPageFallsBackToTheOtherCopyWhenOneReplicaFails(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fetcher := newFakeFetcher(fixture)
	failing := fixture.assignment.ResultReplicas[0]
	fetcher.failNode[failing.SinkTask] = failing.PrimaryNodeID
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}
	response, err := engine.Page(context.Background(), pageRequest(fixture, protocol.MaxResultPageBytes))
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) == 0 {
		t.Fatal("fallback served no records")
	}
	servedSecondary := false
	for _, request := range fetcher.requests {
		if request.Artifact.SinkTask == failing.SinkTask && request.ReplicaNodeID == failing.SecondaryNodeID {
			servedSecondary = true
		}
	}
	if !servedSecondary {
		t.Fatalf("secondary copy never served: %+v", fetcher.requests)
	}
}

func TestPageRejectsForeignBindingsAndIncompleteManifests(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fetcher := newFakeFetcher(fixture)
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}

	foreign := pageRequest(fixture, protocol.MaxResultPageBytes)
	foreign.ManifestDigest[0] ^= 1
	if _, err := engine.Page(context.Background(), foreign); err == nil {
		t.Fatal("foreign manifest digest accepted")
	}

	selectorless := pageRequest(fixture, protocol.MaxResultPageBytes)
	selectorless.Last = firstGlobalRecord(t, fixture).TupleID
	if _, err := engine.Page(context.Background(), selectorless); err == nil {
		t.Fatal("cursor value without selector accepted")
	}
	foreignJob := pageRequest(fixture, protocol.MaxResultPageBytes)
	foreignJob.HasLastTuple = true
	foreignJob.Last = firstGlobalRecord(t, fixture).TupleID
	foreignJob.Last.JobID[0] ^= 1
	if _, err := engine.Page(context.Background(), foreignJob); err == nil {
		t.Fatal("foreign-job cursor accepted")
	}
	zero := pageRequest(fixture, 0)
	if _, err := engine.Page(context.Background(), zero); err == nil {
		t.Fatal("zero page budget accepted")
	}
	unknown := pageRequest(fixture, protocol.MaxResultPageBytes)
	unknown.JobID[0] ^= 1
	if _, err := engine.Page(context.Background(), unknown); err == nil {
		t.Fatal("unknown job accepted")
	}
	if len(fetcher.requests) != 0 {
		t.Fatalf("rejected bindings opened partitions: %+v", fetcher.requests)
	}

	// An incomplete manifest set never opens any partition.
	incomplete := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 1, succeed: false})
	incompleteFetcher := newFakeFetcher(incomplete)
	incompleteEngine := &QueryEngine{Machine: incomplete.machine, Fetcher: incompleteFetcher}
	if _, err := incompleteEngine.Page(context.Background(), pageRequest(incomplete, protocol.MaxResultPageBytes)); err == nil {
		t.Fatal("incomplete manifest set accepted")
	}
	if len(incompleteFetcher.requests) != 0 {
		t.Fatalf("incomplete set opened partitions: %+v", incompleteFetcher.requests)
	}
}

func TestPageServesEmptyArtifactsAndTerminates(t *testing.T) {
	fixture := seedQueryFixture(t, querySeed{sinkPartitions: 1, sealPartitions: 1, succeed: true, recordsFor: func(model.TaskID, uint16, uint16) []model.ResultRecord {
		return nil
	}})
	fetcher := newFakeFetcher(fixture)
	engine := &QueryEngine{Machine: fixture.machine, Fetcher: fetcher}
	response, err := engine.Page(context.Background(), pageRequest(fixture, 1024))
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.ValidateResultPageResponseCorrelation(pageRequest(fixture, 1024), response); err != nil {
		t.Fatalf("correlation: %v", err)
	}
	if len(response.Records) != 0 || !response.End || response.NextHasLastTuple || response.NextLast != (model.TupleID{}) {
		t.Fatalf("empty page=%+v", response)
	}
}

func firstGlobalRecord(t *testing.T, fixture queryFixture) model.ResultRecord {
	t.Helper()
	records := allGlobalRecords(t, fixture)
	if len(records) == 0 {
		t.Fatal("fixture has no records")
	}
	return records[0]
}

func allGlobalRecords(t *testing.T, fixture queryFixture) []model.ResultRecord {
	t.Helper()
	records := make([]model.ResultRecord, 0)
	for _, replica := range fixture.assignment.ResultReplicas {
		records = append(records, fixture.records[replica.SinkTask]...)
	}
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && tupleLessQuery(records[j].TupleID, records[j-1].TupleID); j-- {
			records[j-1], records[j] = records[j], records[j-1]
		}
	}
	return records
}
