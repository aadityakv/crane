package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

func TestWorkerMessageTableValidInvalidGoldenTruncationAndOwnership(t *testing.T) {
	fixture := workerFixture(t)
	cases := []struct {
		name    string
		typeID  wire.MessageType
		value   WorkerMessage
		invalid WorkerMessage
		golden  string
	}{
		{"handshake", 200, fixture.handshake, WorkerHandshake{}, "a38c814079b890cd1308edd083f925cc49e36c681417e658736534a68bafecd5"},
		{"handshake_ack", 201, fixture.handshakeAck, WorkerHandshakeAck{}, "ba425b5ec24e43d64dc8c38014366b0ae289f901b3ac9ff065c926df5ba757a6"},
		{"fence_request", 202, fixture.fence, FenceRequest{}, "188908a1e60279df9bdf19b5f3f559c2a3749a3d5637c769a1f4947060cb236a"},
		{"fence_response", 203, fixture.fenceResponse, FenceResponse{}, "d4b8e6ec7790fa83a009b8944bd2c12889059da4e9bd27ed68ed1ae4f92db417"},
		{"register_request", 204, fixture.register, WorkerRegisterRequest{}, "ddb07f64b38d124f70dfb3b78e187f15d36b40a1f1719b75c1a2b277cf6c167e"},
		{"register_response", 205, fixture.registerResponse, WorkerRegisterResponse{}, "754be824632090c1ccdc0a08ed2ff6d599c6c461d1f7027618687c691e41a4ad"},
		{"assignment_install", 206, fixture.install, AssignmentSetInstall{}, "16e239886543b95509ee672c8226a31fb619ca8a37d81889b9967ee625c2eb6c"},
		{"assignment_ack", 207, fixture.installAck, AssignmentSetInstallAck{}, "6f805e4bea27a0895c250671132928b832197be2d5141b01c29d4edac2a18b4f"},
		{"status_request", 208, fixture.statusRequest, WorkerStatusRequest{MaxEvents: MaxWorkerStatusEvents + 1}, "b0c2e4970553038c5add24e0bda5a5b6036696ea9fac5587f70f1c6171ba876e"},
		{"status_report", 209, fixture.status, WorkerStatus{Events: []model.WorkerEvent{fixture.event, fixture.event}}, "5cff2c13ab9417d46f2f0e6eee187e6cbbf5cb7da5425f7044f8e4cbe60c884c"},
		{"checkpoint_notice", 210, fixture.checkpoint, CheckpointNotice{}, "8e0f715b2cc1d3a88baba6309ad2be26b93c1245aff75d7ac5849403dfff91e8"},
		{"checkpoint_ack", 211, fixture.checkpointAck, CheckpointAck{}, "8875adccca99f4cd0eb1c258547790cbd9cca1ffd6795d6bec457b1ca26ef979"},
		{"record_chunk", 212, fixture.recordChunk, ResultRecordChunk{}, "20e79f134f010b00c1750f948469376b9d471f05beed105fa392ebceb55f3e16"},
		{"record_ack", 213, fixture.recordAck, ResultRecordAck{}, "cf72ea4689552e93104e77c3d1a615d796314da1b4072ae98826273ee39e86b5"},
		{"artifact_chunk", 214, fixture.artifactChunk, ResultArtifactChunk{}, "0c1856850cfff0ffad59f41c806d75d10d8930f261ddbbc44b30424b803e8eed"},
		{"artifact_ack", 215, fixture.artifactAck, ResultArtifactAck{}, "a5cf440ba9aa7f45847ceb92902c9a848333d03cd7063f768e9328bddb7f118f"},
		{"fetch_request", 216, fixture.fetch, ResultFetchRequest{}, "8800e24d5fff485a9abb7e750d758795b46f40cf098e6401d100bcab7ea6176f"},
		{"fetch_chunk", 217, fixture.fetchChunk, ResultFetchChunk{}, "d1c467fc77a6928b0599afa68b0cc1bed0e983dad2b615f621297a50a07745d1"},
		{"worker_error", 218, fixture.workerError, WorkerError{}, "661b53eb100ab237f3e379be80d7c1caf3d235f028a1c5c3cfc97d782c73c881"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.value.MessageType(); got != test.typeID {
				t.Fatalf("MessageType = %d, want %d", got, test.typeID)
			}
			encoded, err := MarshalWorkerMessage(test.value)
			if err != nil {
				t.Fatalf("MarshalWorkerMessage: %v", err)
			}
			if len(encoded) > MaxWorkerControlPayloadBytes {
				t.Fatalf("payload length = %d, max %d", len(encoded), MaxWorkerControlPayloadBytes)
			}
			if len(encoded) < 4 || binary.BigEndian.Uint16(encoded[:2]) != WorkerControlSchemaVersion || binary.BigEndian.Uint16(encoded[2:4]) != uint16(test.typeID) {
				t.Fatalf("non-golden prefix %x", encoded)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != test.golden {
				t.Fatalf("golden SHA-256 = %s, want %s", got, test.golden)
			}
			encodedAgain, err := MarshalWorkerMessage(test.value)
			if err != nil || !bytes.Equal(encoded, encodedAgain) {
				t.Fatalf("encoding is not deterministic: %x / %x / %v", encoded, encodedAgain, err)
			}
			decoded, err := UnmarshalWorkerMessage(test.typeID, encoded)
			if err != nil {
				t.Fatalf("UnmarshalWorkerMessage: %v", err)
			}
			if !reflect.DeepEqual(decoded, test.value) {
				t.Fatalf("round trip mismatch\n got: %#v\nwant: %#v", decoded, test.value)
			}
			for cut := 0; cut < len(encoded); cut++ {
				if _, err := UnmarshalWorkerMessage(test.typeID, encoded[:cut]); err == nil {
					t.Fatalf("accepted truncation at %d/%d", cut, len(encoded))
				}
			}
			trailing := append(append([]byte(nil), encoded...), 0)
			if _, err := UnmarshalWorkerMessage(test.typeID, trailing); err == nil {
				t.Fatal("accepted trailing byte")
			}
			if _, err := MarshalWorkerMessage(test.invalid); err == nil {
				t.Fatalf("accepted invalid %#v", test.invalid)
			}
			if len(encoded) > 4 {
				owned, err := UnmarshalWorkerMessage(test.typeID, encoded)
				if err != nil {
					t.Fatal(err)
				}
				encoded[4] ^= 0xff
				ownedBytes, err := MarshalWorkerMessage(owned)
				if err != nil || bytes.Equal(encoded, ownedBytes) {
					t.Fatalf("decode did not own values: %v", err)
				}
			}
		})
	}
	if _, err := UnmarshalWorkerMessage(wire.MessageCraneWorkerReserved, []byte{0, 1, 0, 219}); !errors.Is(err, ErrUnexpectedWorkerMessage) {
		t.Fatalf("reserved 219 error = %v, want ErrUnexpectedWorkerMessage", err)
	}
}

func TestWorkerHandshakeRequiresExactCompiledFingerprints(t *testing.T) {
	valid := workerFixture(t).handshake
	for _, mutate := range []func(*WorkerHandshake){
		func(message *WorkerHandshake) { message.ConsensusFingerprint = [32]byte{} },
		func(message *WorkerHandshake) { message.ConsensusFingerprint[0] ^= 1 },
		func(message *WorkerHandshake) { message.RegistryFingerprint = [32]byte{} },
		func(message *WorkerHandshake) { message.RegistryFingerprint[0] ^= 1 },
	} {
		invalid := valid
		mutate(&invalid)
		if _, err := MarshalWorkerMessage(invalid); err == nil {
			t.Fatalf("accepted fingerprint mismatch %#v", invalid)
		}
	}
}

func TestAssignmentInstallOwnsCompleteCrossReferencedSetAndTopology(t *testing.T) {
	fixture := workerFixture(t)
	encoded, err := MarshalWorkerMessage(fixture.install)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := model.ValidateTopology(fixture.install.Specification)
	if err != nil {
		t.Fatal(err)
	}
	canonicalTopology := validated.CanonicalBytes()
	if declared := binary.BigEndian.Uint64(encoded[4:12]); declared != uint64(len(canonicalTopology)-8) || !bytes.Equal(encoded[4:4+len(canonicalTopology)], canonicalTopology) {
		t.Fatalf("assignment install did not embed the one canonical topology length: declared=%d canonical=%d", declared, len(canonicalTopology)-8)
	}
	decodedMessage, err := UnmarshalWorkerMessage(wire.MessageCraneAssignmentSetInstall, encoded)
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodedMessage.(AssignmentSetInstall)
	fixture.install.Assignment.Tasks[0].Attempt++
	fixture.install.Specification.Stages[0].Name = "changed"
	if decoded.Assignment.Tasks[0].Attempt == fixture.install.Assignment.Tasks[0].Attempt || decoded.Specification.Stages[0].Name == "changed" {
		t.Fatal("decoded install aliases caller-owned slices")
	}
	bad := decoded
	bad.JobControlRevision = 0
	if _, err := MarshalWorkerMessage(bad); err == nil {
		t.Fatal("accepted zero JobControlRevision")
	}
	bad = decoded
	bad.SchedulingState = 0
	if _, err := MarshalWorkerMessage(bad); err == nil {
		t.Fatal("accepted implicit scheduling state")
	}
	bad = decoded
	bad.SpecificationDigest[0] ^= 1
	if _, err := MarshalWorkerMessage(bad); err == nil {
		t.Fatal("accepted mismatched specification digest")
	}
}

func TestCheckpointAckRepeatsExactCommittedAndAssignmentCorrelation(t *testing.T) {
	ack := workerFixture(t).checkpointAck
	for _, mutate := range []func(*CheckpointAck){
		func(v *CheckpointAck) { v.JobControlRevision = 0 },
		func(v *CheckpointAck) { v.AssignmentRevision = 0 },
		func(v *CheckpointAck) { v.AssignmentDigest = [32]byte{} },
	} {
		invalid := ack
		mutate(&invalid)
		if _, err := MarshalWorkerMessage(invalid); err == nil {
			t.Fatalf("accepted incomplete checkpoint correlation: %#v", invalid)
		}
	}
}

func TestAssignmentInstallWorstLegalShapeFitsAuthenticatedFrameAndPreflightsDeclaredLength(t *testing.T) {
	install := worstLegalAssignmentInstall(t)
	consensus := model.LimitsV1()
	if uint64(len(install.Specification.Stages)) != consensus.MaxStages || uint64(len(install.Specification.Edges)) != consensus.MaxEdges || uint64(len(install.Assignment.Tasks)) != consensus.MaxTasksPerJob || uint64(len(install.Assignment.ResultReplicas)) != consensus.MaxTasksPerStage {
		t.Fatal("maximum proof fixture misses a structural/count maximum")
	}
	if len(install.Specification.Name) != int(consensus.MaxIdentifierBytes) {
		t.Fatal("maximum proof fixture job name is not maximal")
	}
	largestTransform := largestLegalTransformOperator(t)
	for index, stage := range install.Specification.Stages {
		if len(stage.Name) != int(consensus.MaxIdentifierBytes) {
			t.Fatalf("stage %d name is not maximal", index)
		}
		if stage.Role == model.Transform && !reflect.DeepEqual(stage.Operator, largestTransform) {
			t.Fatalf("stage %d does not use the largest legal transform/settings encoding: %#v", index, stage.Operator)
		}
	}
	source := install.Specification.Stages[0]
	if source.Role != model.Source || source.Parallelism != uint16(consensus.MaxTasksPerStage) || source.Operator.Name != "range" || !reflect.DeepEqual(source.Operator.Settings, []model.Setting{{Key: "end_exclusive", Value: "-9223372036853775808"}, {Key: "start", Value: "-9223372036854775808"}}) {
		t.Fatalf("source does not maximize legal range/settings/task dimensions: %#v", source)
	}
	sink := install.Specification.Stages[len(install.Specification.Stages)-1]
	if sink.Role != model.Sink || sink.Parallelism != uint16(consensus.MaxTasksPerStage) || sink.Operator.Name != "collect" || len(sink.Operator.Settings) != 0 {
		t.Fatalf("sink does not maximize legal replica/task dimensions: %#v", sink)
	}
	for index, edge := range install.Specification.Edges {
		if edge.Routing != model.FieldHash || edge.Field != "value" {
			t.Fatalf("edge %d omits the largest legal routing-field encoding: %#v", index, edge)
		}
	}
	overlongField := install.Specification
	overlongField.Edges = append([]model.EdgeSpec(nil), overlongField.Edges...)
	overlongField.Edges[0].Field = strings.Repeat("v", int(consensus.MaxIdentifierBytes))
	if _, err := model.ValidateTopology(overlongField); err == nil {
		t.Fatal("consensus unexpectedly permits a longer field-hash routing field than value")
	}
	tasksPerWorker := make(map[uint16]int)
	for _, token := range install.Assignment.Tasks {
		tasksPerWorker[token.WorkerID]++
	}
	if len(tasksPerWorker) != 4 {
		t.Fatalf("maximum set does not use the four required max-slot workers: %v", tasksPerWorker)
	}
	for node, count := range tasksPerWorker {
		if count != int(consensus.MaxWorkerSlots) {
			t.Fatalf("worker %d owns %d tasks, want max %d", node, count, consensus.MaxWorkerSlots)
		}
	}
	payload, err := MarshalWorkerMessage(install)
	if err != nil {
		t.Fatal(err)
	}
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.LimitsV1().MaxWorkerControlFrameBytes)
	frame, err := wire.Encode(wire.Header{Version: wire.Version1, Message: wire.MessageCraneAssignmentSetInstall, ClusterID: [16]byte{1}, SenderID: 1, RequestID: wire.RequestID{1}, TimestampMillis: 1, Codec: wire.CodecBinary}, payload, wire.NewHMACAuthenticator([]byte("worker-control-max-frame-test-key")), limits)
	if err != nil {
		t.Fatalf("encode authenticated maximum install: %v", err)
	}
	if uint64(len(frame)) > model.LimitsV1().MaxWorkerControlFrameBytes {
		t.Fatalf("authenticated install = %d, max %d", len(frame), model.LimitsV1().MaxWorkerControlFrameBytes)
	}
	if len(frame) != 113932 {
		t.Fatalf("maximum structured authenticated install = %d, want independently accounted 113932", len(frame))
	}
	if want, err := model.CompleteAssignmentSetInstallBytes(uint64(len(mustValidatedTopology(t, install.Specification).CanonicalBytes())), uint64(len(install.Assignment.Tasks)), uint64(len(install.Assignment.ResultReplicas))); err != nil || want != uint64(len(frame)) {
		t.Fatalf("model/real frame accounting = %d/%d/%v", want, len(frame), err)
	}
	t.Logf("actual 64-stage/256-edge/1,024-task/256-replica authenticated install: %d bytes", len(frame))
	if _, err := UnmarshalAssignmentSetInstall(payload); err != nil {
		t.Fatalf("typed install decoder: %v", err)
	}
	decodeSentinel := errors.New("decode reached")
	exact := make([]byte, 4+model.LimitsV1().MaxTopologyBytes)
	binary.BigEndian.PutUint16(exact[:2], WorkerControlSchemaVersion)
	binary.BigEndian.PutUint16(exact[2:4], uint16(wire.MessageCraneAssignmentSetInstall))
	binary.BigEndian.PutUint64(exact[4:12], model.LimitsV1().MaxTopologyBytes-8)
	called := false
	if _, err := unmarshalAssignmentSetInstallWith(exact, func([]byte) (model.ValidatedTopology, error) {
		called = true
		return model.ValidatedTopology{}, decodeSentinel
	}); !called || err == nil || !strings.Contains(err.Error(), decodeSentinel.Error()) {
		t.Fatalf("exact topology boundary did not reach decoder: err=%v called=%v", err, called)
	}
	malformed := make([]byte, 12)
	binary.BigEndian.PutUint16(malformed[:2], WorkerControlSchemaVersion)
	binary.BigEndian.PutUint16(malformed[2:4], uint16(wire.MessageCraneAssignmentSetInstall))
	binary.BigEndian.PutUint64(malformed[4:12], model.LimitsV1().MaxTopologyBytes-8+1)
	called = false
	if _, err := unmarshalAssignmentSetInstallWith(malformed, func([]byte) (model.ValidatedTopology, error) {
		called = true
		return model.ValidatedTopology{}, nil
	}); err == nil || called {
		t.Fatalf("oversized declared topology was not rejected before allocation/decode: err=%v called=%v", err, called)
	}
}

func worstLegalAssignmentInstall(t *testing.T) AssignmentSetInstall {
	t.Helper()
	job := model.JobID{0x71}
	limits := model.LimitsV1()
	stages := make([]model.StageSpec, limits.MaxStages)
	for index := range stages {
		role, operator := model.Transform, largestLegalTransformOperator(t)
		parallelism := uint16(1)
		if index == 0 {
			role, operator, parallelism = model.Source, model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "-9223372036853775808"}, {Key: "start", Value: "-9223372036854775808"}}}, 256
		} else if index == 1 {
			parallelism = 256
		} else if index == 2 {
			parallelism = 196
		}
		if index == len(stages)-1 {
			role, operator, parallelism = model.Sink, model.OperatorSpec{Name: "collect", Version: 1}, 256
		}
		name := fmt.Sprintf("stage-%03d", index+1)
		name += strings.Repeat("x", int(limits.MaxIdentifierBytes)-len(name))
		stages[index] = model.StageSpec{StageID: uint16(index + 1), Name: name, Role: role, Parallelism: parallelism, Operator: operator}
	}
	edges := make([]model.EdgeSpec, 0, limits.MaxEdges)
	for index := 0; index < len(stages)-1; index++ {
		edges = append(edges, model.EdgeSpec{EdgeID: uint16(len(edges) + 1), SourceStageID: uint16(index + 1), DestinationStageID: uint16(index + 2), Routing: model.FieldHash, Field: "value"})
	}
	for uint64(len(edges)) < limits.MaxEdges {
		edges = append(edges, model.EdgeSpec{EdgeID: uint16(len(edges) + 1), SourceStageID: 1, DestinationStageID: uint16(len(stages)), Routing: model.FieldHash, Field: "value"})
	}
	spec := model.TopologySpec{SchemaVersion: 1, Name: string(bytes.Repeat([]byte{'z'}, 64)), Stages: stages, Edges: edges, RegistryFingerprint: model.RegistryFingerprint()}
	validated, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	workers := make([]model.WorkerPlacement, 4)
	for index := range workers {
		workers[index] = model.WorkerPlacement{NodeID: uint16(index + 1), WorkerEpoch: model.WorkerEpoch{byte(index + 1)}, SlotCapacity: 256}
	}
	set, err := model.BuildAssignmentSet(job, validated.Digest(), 1, validated, workers)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Tasks) != 1024 || len(set.ResultReplicas) != 256 {
		t.Fatalf("worst assignment shape = %d tasks/%d replicas", len(set.Tasks), len(set.ResultReplicas))
	}
	return AssignmentSetInstall{Assignment: set, Specification: spec, SpecificationDigest: validated.Digest(), JobControlRevision: 1, SchedulingState: model.Closed, CoordinatorEpoch: model.CoordinatorEpoch{Term: 1, BeginIndex: 1, Coordinator: 1, Nonce: [16]byte{1}}}
}

func largestLegalTransformOperator(t *testing.T) model.OperatorSpec {
	t.Helper()
	var largest model.OperatorSpec
	largestBytes := -1
	for _, descriptor := range model.RegistryV1().Operators {
		if descriptor.Role != model.OperatorRoleTransform {
			continue
		}
		operator := model.OperatorSpec{Name: descriptor.Name, Version: descriptor.Version}
		encodedBytes := 2 + len(descriptor.Name) + 2 + 2
		for _, setting := range descriptor.Settings {
			if !setting.Required || setting.Type != model.SettingTypeInt64 {
				t.Fatalf("unexpected optional/non-int transform setting in v1 registry: %#v", setting)
			}
			value := "-9223372036854775808"
			operator.Settings = append(operator.Settings, model.Setting{Key: setting.Name, Value: value})
			encodedBytes += 2 + len(setting.Name) + 2 + len(value)
		}
		if encodedBytes > largestBytes {
			largest, largestBytes = operator, encodedBytes
		}
	}
	if largestBytes < 0 {
		t.Fatal("v1 registry has no transform operator")
	}
	return largest
}

func mustValidatedTopology(t *testing.T, spec model.TopologySpec) model.ValidatedTopology {
	t.Helper()
	validated, err := model.ValidateTopology(spec)
	if err != nil {
		t.Fatal(err)
	}
	return validated
}

func TestWorkerStatusGlobalCursorInventoryAndRepairRules(t *testing.T) {
	fixture := workerFixture(t)
	status := fixture.status
	status.AfterTransactionID = status.Events[0].TransactionID
	if _, err := MarshalWorkerMessage(status); err == nil {
		t.Fatal("accepted event that was not after echoed page cursor")
	}
	status = fixture.status
	duplicate := fixture.event
	duplicate.TransactionID = status.Events[0].TransactionID
	status.Events = append(status.Events, duplicate)
	if _, err := MarshalWorkerMessage(status); err == nil {
		t.Fatal("accepted non-increasing global WorkerEpoch transaction IDs")
	}
	status = fixture.status
	status.LastTransactionID--
	if _, err := MarshalWorkerMessage(status); err == nil {
		t.Fatal("accepted LastTransactionID inconsistent with page")
	}
	request := fixture.statusRequest
	request.Repair = &fixture.grant
	if _, err := MarshalWorkerMessage(request); err == nil {
		t.Fatal("accepted both inventory and repair in one request")
	}
	query := *fixture.statusRequest.Inventory
	query.Checkpoints = append(query.Checkpoints, query.Checkpoints[0])
	request = fixture.statusRequest
	request.Inventory = &query
	if _, err := MarshalWorkerMessage(request); err == nil {
		t.Fatal("accepted unsorted/duplicate inventory checkpoint vector")
	}
}

func TestWorkerStatusAcceptsExactly256GloballyOrderedOwnedEvents(t *testing.T) {
	fixture := workerFixture(t)
	status := fixture.status
	status.Events = make([]model.WorkerEvent, MaxWorkerStatusEvents)
	for index := range status.Events {
		event := fixture.event
		report := *fixture.event.Completion
		report.WorkerTransactionID = uint64(index + 10)
		report.Digest = model.CompletionReportDigest(report)
		event.TransactionID = report.WorkerTransactionID
		event.Completion = &report
		status.Events[index] = event
	}
	status.LastTransactionID = status.Events[len(status.Events)-1].TransactionID
	status.StoreTransactionID = status.LastTransactionID
	status.HasMore = true
	encoded, err := MarshalWorkerMessage(status)
	if err != nil {
		t.Fatalf("marshal 256-event page: %v", err)
	}
	decoded, err := UnmarshalWorkerMessage(wire.MessageCraneWorkerStatusReport, encoded)
	if err != nil || len(decoded.(WorkerStatus).Events) != MaxWorkerStatusEvents {
		t.Fatalf("decode 256-event page: len=%d err=%v", len(decoded.(WorkerStatus).Events), err)
	}
	status.Events = append(status.Events, status.Events[len(status.Events)-1])
	if _, err := MarshalWorkerMessage(status); err == nil {
		t.Fatal("accepted 257 events")
	}
}

func TestInventoryAcceptsZeroWatermarksAtExact256EntryBound(t *testing.T) {
	fixture := workerFixture(t)
	query := *fixture.statusRequest.Inventory
	query.Checkpoints = make([]SourceCheckpoint, MaxInventoryCheckpoints)
	for index := range query.Checkpoints {
		query.Checkpoints[index] = SourceCheckpoint{Source: model.TaskID{JobID: query.JobID, StageID: 1, Partition: uint16(index)}}
	}
	query.CheckpointDigest = CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = InventoryQueryDigest(query)
	request := fixture.statusRequest
	request.Inventory = &query
	if _, err := MarshalWorkerMessage(request); err != nil {
		t.Fatalf("marshal 256-entry zero-watermark vector: %v", err)
	}
	query.Checkpoints = append(query.Checkpoints, SourceCheckpoint{Source: model.TaskID{JobID: query.JobID, StageID: 1, Partition: 256}})
	query.CheckpointDigest = CheckpointVectorDigest(query.Checkpoints)
	query.QueryDigest = InventoryQueryDigest(query)
	if _, err := MarshalWorkerMessage(WorkerStatusRequest{CoordinatorEpoch: request.CoordinatorEpoch, MaxEvents: 1, Inventory: &query}); err == nil {
		t.Fatal("accepted 257 checkpoint entries")
	}
}

func TestBilateralRepairUsesOneInstructionWithDistinctEndpointRoles(t *testing.T) {
	fixture := workerFixture(t)
	source := fixture.grant
	destination := source
	destination.Role = RepairDestination
	for _, grant := range []RepairGrant{source, destination} {
		request := WorkerStatusRequest{CoordinatorEpoch: grant.Instruction.CoordinatorEpoch, MaxEvents: 1, Repair: &grant}
		encoded, err := MarshalWorkerMessage(request)
		if err != nil {
			t.Fatalf("marshal role %d: %v", grant.Role, err)
		}
		decoded, err := UnmarshalWorkerMessage(wire.MessageCraneWorkerStatusRequest, encoded)
		if err != nil {
			t.Fatalf("decode role %d: %v", grant.Role, err)
		}
		got := decoded.(WorkerStatusRequest).Repair
		if got.Role != grant.Role || got.Instruction.RepairID != source.Instruction.RepairID || got.Instruction.InstructionDigest != source.Instruction.InstructionDigest {
			t.Fatalf("bilateral grant changed shared instruction: %#v", got)
		}
	}
}

func TestResultRepairStatusUsesTypedErrorsOnlyForFailedState(t *testing.T) {
	fixture := workerFixture(t)
	base := fixture.status
	base.Inventory = nil
	base.Repair = &ResultRepairStatus{Instruction: fixture.grant.Instruction, RepairID: fixture.grant.Instruction.RepairID, InstructionDigest: fixture.grant.Instruction.InstructionDigest, Role: RepairDestination, State: RepairPending, ContentDigest: model.EmptyResultInventoryDigest(fixture.grant.Instruction.InstructionDigest)}
	if _, err := MarshalWorkerMessage(base); err != nil {
		t.Fatalf("pending repair status: %v", err)
	}
	failed := *base.Repair
	failed.State = RepairFailed
	base.Repair = &failed
	if _, err := MarshalWorkerMessage(base); err == nil {
		t.Fatal("accepted failed repair without typed error")
	}
	failed.ErrorCode = WorkerErrorUnavailable
	base.Repair = &failed
	if _, err := MarshalWorkerMessage(base); err != nil {
		t.Fatalf("failed repair with typed error: %v", err)
	}
	failed.State = RepairComplete
	base.Repair = &failed
	if _, err := MarshalWorkerMessage(base); err == nil {
		t.Fatal("accepted completed repair carrying an error")
	}
}

func TestEmptyResultArtifactHasCanonicalZeroLengthTransfer(t *testing.T) {
	fixture := workerFixture(t)
	emptyChecksum := sha256.Sum256(nil)
	artifact := fixture.artifactChunk.Artifact
	artifact.RecordCount = 0
	artifact.TotalLength = 0
	artifact.Checksum = emptyChecksum
	transfer := fixture.artifactChunk.Transfer
	transfer.TotalLength = 0
	transfer.Checksum = emptyChecksum
	transfer.Offset = 0
	transfer.Data = nil
	transfer.Final = true
	messages := []WorkerMessage{
		ResultArtifactChunk{Transfer: transfer, Artifact: artifact, DestinationNodeID: fixture.artifactChunk.DestinationNodeID, DestinationWorkerEpoch: fixture.artifactChunk.DestinationWorkerEpoch, CoordinatorEpoch: fixture.artifactChunk.CoordinatorEpoch},
		ResultArtifactAck{TransferID: transfer.TransferID, NodeID: fixture.artifactAck.NodeID, WorkerEpoch: fixture.artifactAck.WorkerEpoch, Artifact: artifact, NextOffset: 0, Complete: true, CoordinatorEpoch: fixture.artifactAck.CoordinatorEpoch},
		ResultFetchRequest{Artifact: artifact, ReplicaNodeID: fixture.fetch.ReplicaNodeID, ReplicaWorkerEpoch: fixture.fetch.ReplicaWorkerEpoch, Offset: 0, CoordinatorEpoch: fixture.fetch.CoordinatorEpoch},
		ResultFetchChunk{Transfer: transfer, Artifact: artifact, SourceNodeID: fixture.fetchChunk.SourceNodeID, SourceWorkerEpoch: fixture.fetchChunk.SourceWorkerEpoch, CoordinatorEpoch: fixture.fetchChunk.CoordinatorEpoch},
	}
	for _, message := range messages {
		encoded, err := MarshalWorkerMessage(message)
		if err != nil {
			t.Fatalf("marshal empty artifact message %d: %v", message.MessageType(), err)
		}
		if _, err := UnmarshalWorkerMessage(message.MessageType(), encoded); err != nil {
			t.Fatalf("decode empty artifact message %d: %v", message.MessageType(), err)
		}
	}
}

func TestTransferBoundsOffsetsChecksumsRepairAndReplicaOwnership(t *testing.T) {
	fixture := workerFixture(t)
	resumedFinal := fixture.recordChunk
	stream, err := model.MarshalResultRecord(resumedFinal.Record)
	if err != nil {
		t.Fatal(err)
	}
	resumedFinal.Transfer.Offset = uint64(len(stream) - 2)
	resumedFinal.Transfer.Data = append([]byte(nil), stream[len(stream)-2:]...)
	if _, err := MarshalWorkerMessage(resumedFinal); err != nil {
		t.Fatalf("rejected exact resumed final record chunk: %v", err)
	}
	tooLarge := fixture.recordChunk
	tooLarge.Transfer.Data = make([]byte, MaxTransferChunkBytes+1)
	if _, err := MarshalWorkerMessage(tooLarge); err == nil {
		t.Fatal("accepted 256-KiB chunk plus one")
	}
	tooLarge = fixture.recordChunk
	tooLarge.Transfer.TotalLength = MaxTransferTotalBytes + 1
	if _, err := MarshalWorkerMessage(tooLarge); err == nil {
		t.Fatal("accepted 64-MiB total plus one")
	}
	gap := fixture.recordChunk
	gap.Transfer.Offset = gap.Transfer.TotalLength
	if _, err := MarshalWorkerMessage(gap); err == nil {
		t.Fatal("accepted transfer offset beyond exact chunk range")
	}
	wrongReplica := fixture.recordChunk
	wrongReplica.DestinationNodeID = wrongReplica.Provenance.ReplicaSet.PrimaryNodeID
	wrongReplica.DestinationWorkerEpoch = wrongReplica.Provenance.ReplicaSet.PrimaryEpoch
	if _, err := MarshalWorkerMessage(wrongReplica); err == nil {
		t.Fatal("accepted secondary role mapped to primary destination")
	}
	brokenRepair := fixture.recordChunk
	brokenRepair.RepairInstructionDigest = [32]byte{}
	if _, err := MarshalWorkerMessage(brokenRepair); err == nil {
		t.Fatal("accepted repair chunk without instruction digest")
	}
	original := fixture.recordChunk.Record.Checksum
	changed := fixture.recordChunk
	changed.Provenance.AssignmentRevision++
	if changed.Record.Checksum != original {
		t.Fatal("copy provenance changed immutable logical checksum")
	}
}

func TestWorkerDecoderRejectsCompleteFrameBudgetPlusOneBeforeFields(t *testing.T) {
	encoded := make([]byte, MaxWorkerControlPayloadBytes+1)
	if _, err := UnmarshalWorkerMessage(wire.MessageCraneWorkerHandshake, encoded); !errors.Is(err, ErrWorkerMessageTooLarge) {
		t.Fatalf("payload max + 1 error = %v, want ErrWorkerMessageTooLarge", err)
	}
}

func FuzzUnmarshalWorkerMessage(f *testing.F) {
	fixture := workerFixtureForFuzz()
	for _, message := range fixture {
		encoded, err := MarshalWorkerMessage(message)
		if err == nil {
			f.Add(uint16(message.MessageType()), encoded)
		}
	}
	f.Fuzz(func(t *testing.T, typeID uint16, encoded []byte) {
		if len(encoded) > MaxWorkerControlPayloadBytes+1 {
			encoded = encoded[:MaxWorkerControlPayloadBytes+1]
		}
		_, _ = UnmarshalWorkerMessage(wire.MessageType(typeID), encoded)
	})
}

type workerMessageFixture struct {
	handshake        WorkerHandshake
	handshakeAck     WorkerHandshakeAck
	fence            FenceRequest
	fenceResponse    FenceResponse
	register         WorkerRegisterRequest
	registerResponse WorkerRegisterResponse
	install          AssignmentSetInstall
	installAck       AssignmentSetInstallAck
	statusRequest    WorkerStatusRequest
	status           WorkerStatus
	event            model.WorkerEvent
	checkpoint       CheckpointNotice
	checkpointAck    CheckpointAck
	recordChunk      ResultRecordChunk
	recordAck        ResultRecordAck
	artifactChunk    ResultArtifactChunk
	artifactAck      ResultArtifactAck
	fetch            ResultFetchRequest
	fetchChunk       ResultFetchChunk
	workerError      WorkerError
	grant            RepairGrant
}

func workerFixture(t *testing.T) workerMessageFixture {
	t.Helper()
	fixture, err := makeWorkerFixture()
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func workerFixtureForFuzz() []WorkerMessage {
	fixture, err := makeWorkerFixture()
	if err != nil {
		return nil
	}
	return []WorkerMessage{fixture.handshake, fixture.handshakeAck, fixture.fence, fixture.fenceResponse, fixture.register, fixture.registerResponse, fixture.install, fixture.installAck, fixture.statusRequest, fixture.status, fixture.checkpoint, fixture.checkpointAck, fixture.recordChunk, fixture.recordAck, fixture.artifactChunk, fixture.artifactAck, fixture.fetch, fixture.fetchChunk, fixture.workerError}
}

func makeWorkerFixture() (workerMessageFixture, error) {
	job := model.JobID{1}
	workerEpoch := model.WorkerEpoch{2}
	destinationEpoch := model.WorkerEpoch{3}
	epoch := model.CoordinatorEpoch{Term: 4, BeginIndex: 5, Coordinator: 1, Nonce: [16]byte{6}}
	spec := model.TopologySpec{SchemaVersion: 1, Name: "job", RegistryFingerprint: model.RegistryFingerprint(), Stages: []model.StageSpec{
		{StageID: 1, Name: "source", Role: model.Source, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "2"}, {Key: "start", Value: "1"}}}},
		{StageID: 2, Name: "sink", Role: model.Sink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
	}, Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.Shuffle}}}
	validated, err := model.ValidateTopology(spec)
	if err != nil {
		return workerMessageFixture{}, err
	}
	assignment, err := model.BuildAssignmentSet(job, validated.Digest(), 7, validated, []model.WorkerPlacement{{NodeID: 1, WorkerEpoch: workerEpoch, SlotCapacity: 2}, {NodeID: 2, WorkerEpoch: destinationEpoch, SlotCapacity: 2}})
	if err != nil {
		return workerMessageFixture{}, err
	}
	source := model.TaskID{JobID: job, StageID: 1}
	sink := model.TaskID{JobID: job, StageID: 2}
	var sourceToken model.AssignmentToken
	for _, token := range assignment.Tasks {
		if token.Task == source {
			sourceToken = token
		}
	}
	report := model.CompletionReport{JobID: job, JobControlRevision: 8, AssignmentRevision: 7, Source: source, Token: sourceToken, Epoch: epoch, Prior: 0, New: 1, EOF: 1, WorkerTransactionID: 9}
	report.Digest = model.CompletionReportDigest(report)
	event := model.WorkerEvent{WorkerID: sourceToken.WorkerID, WorkerEpoch: sourceToken.WorkerEpoch, TransactionID: 9, Kind: model.WorkerEventCompletion, Completion: &report}
	checkpoints := []SourceCheckpoint{{Source: source, Watermark: 1}}
	query := ResultInventoryQuery{JobID: job, SinkTask: sink, SpecificationHash: validated.Digest(), AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, Checkpoints: checkpoints}
	query.CheckpointDigest = CheckpointVectorDigest(checkpoints)
	query.QueryDigest = InventoryQueryDigest(query)
	instruction := RepairResultPartition{CoordinatorEpoch: epoch, JobID: job, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SourceNodeID: 1, SourceWorkerEpoch: workerEpoch, DestinationNodeID: 2, DestinationWorkerEpoch: destinationEpoch, SinkTask: sink, SpecificationHash: validated.Digest(), Checkpoints: checkpoints, CheckpointDigest: query.CheckpointDigest, InventoryQueryDigest: query.QueryDigest, ExpectedRecordCount: 1, ExpectedTotalBytes: 2, ExpectedContentDigest: sha256.Sum256([]byte("records"))}
	instruction.RepairID = DeriveRepairID(instruction)
	instruction.InstructionDigest = RepairInstructionDigest(instruction)
	grant := RepairGrant{Instruction: instruction, Role: RepairSource}
	tupleID := model.DeriveSourceTupleID(job, source, 1)
	tupleBytes, err := model.MarshalTuple(model.Tuple{})
	if err != nil {
		return workerMessageFixture{}, err
	}
	record, err := model.NewResultRecord(tupleID, sink, validated.Digest(), tupleBytes)
	if err != nil {
		return workerMessageFixture{}, err
	}
	replica := assignment.ResultReplicas[0]
	provenance := model.ResultCopyProvenance{AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, ReplicaSet: replica, DestinationRole: model.SecondaryReplica, CoordinatorEpoch: epoch}
	recordBytes, err := model.MarshalResultRecord(record)
	if err != nil {
		return workerMessageFixture{}, err
	}
	recordTransfer := TransferChunk{TransferID: TransferID{10}, JobID: job, TotalLength: uint64(len(recordBytes)), Checksum: sha256.Sum256(recordBytes), Offset: 0, Data: recordBytes, Final: true}
	artifactBytes := []byte{1, 2}
	artifactTransfer := TransferChunk{TransferID: TransferID{11}, JobID: job, TotalLength: uint64(len(artifactBytes)), Checksum: sha256.Sum256(artifactBytes), Offset: 0, Data: artifactBytes, Final: true}
	artifact := ResultArtifact{JobID: job, SinkTask: sink, SpecificationHash: validated.Digest(), RecordCount: 1, TotalLength: uint64(len(artifactBytes)), Checksum: artifactTransfer.Checksum}
	return workerMessageFixture{
		handshake:    WorkerHandshake{NodeID: 1, WorkerEpoch: workerEpoch, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()},
		handshakeAck: WorkerHandshakeAck{NodeID: 1, WorkerEpoch: workerEpoch, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()},
		fence:        FenceRequest{CoordinatorEpoch: epoch}, fenceResponse: FenceResponse{NodeID: 1, WorkerEpoch: workerEpoch, CoordinatorEpoch: epoch},
		register:         WorkerRegisterRequest{NodeID: 1, WorkerEpoch: workerEpoch, SlotCapacity: 2, CoordinatorEpoch: epoch, ConsensusFingerprint: model.ConsensusFingerprint(), RegistryFingerprint: model.RegistryFingerprint()},
		registerResponse: WorkerRegisterResponse{NodeID: 1, WorkerEpoch: workerEpoch, WorkerRevision: 1, CoordinatorEpoch: epoch, Accepted: true},
		install:          AssignmentSetInstall{Assignment: assignment, Specification: spec, SpecificationDigest: validated.Digest(), JobControlRevision: 8, SchedulingState: model.Running, CoordinatorEpoch: epoch},
		installAck:       AssignmentSetInstallAck{NodeID: 1, WorkerEpoch: workerEpoch, JobID: job, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, JobControlRevision: 8, SchedulingState: model.Running, CoordinatorEpoch: epoch},
		statusRequest:    WorkerStatusRequest{CoordinatorEpoch: epoch, AfterTransactionID: 8, MaxEvents: 1, Inventory: &query},
		status:           WorkerStatus{NodeID: sourceToken.WorkerID, WorkerEpoch: sourceToken.WorkerEpoch, CoordinatorEpoch: epoch, StoreTransactionID: 10, AfterTransactionID: 8, Assignments: []InstalledAssignmentStatus{{JobID: job, JobControlRevision: 8, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, SpecificationDigest: validated.Digest(), SchedulingState: model.Running}}, Events: []model.WorkerEvent{event}, LastTransactionID: 9, HasMore: false, Inventory: &ResultInventorySummary{QueryDigest: query.QueryDigest, RecordCount: 1, TotalBytes: 2, ContentDigest: instruction.ExpectedContentDigest}}, event: event,
		checkpoint:    CheckpointNotice{Notice: model.CheckpointNotice{JobID: job, Source: source, Watermark: 1, RaftIndex: 11, Epoch: epoch}, JobControlRevision: 8, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest},
		checkpointAck: CheckpointAck{NodeID: 1, WorkerEpoch: workerEpoch, JobID: job, Source: source, Watermark: 1, RaftIndex: 11, JobControlRevision: 8, AssignmentRevision: assignment.Revision, AssignmentDigest: assignment.Digest, CoordinatorEpoch: epoch},
		recordChunk:   ResultRecordChunk{Transfer: recordTransfer, Record: record, Provenance: provenance, DestinationNodeID: replica.SecondaryNodeID, DestinationWorkerEpoch: replica.SecondaryEpoch, RepairID: instruction.RepairID, RepairInstructionDigest: instruction.InstructionDigest},
		recordAck:     ResultRecordAck{TransferID: recordTransfer.TransferID, NodeID: 2, WorkerEpoch: destinationEpoch, RepairID: instruction.RepairID, RepairInstructionDigest: instruction.InstructionDigest, NextOffset: uint64(len(recordBytes)), TotalLength: uint64(len(recordBytes)), Checksum: recordTransfer.Checksum, Complete: true, CoordinatorEpoch: epoch},
		artifactChunk: ResultArtifactChunk{Transfer: artifactTransfer, Artifact: artifact, DestinationNodeID: 2, DestinationWorkerEpoch: destinationEpoch, CoordinatorEpoch: epoch},
		artifactAck:   ResultArtifactAck{TransferID: artifactTransfer.TransferID, NodeID: 2, WorkerEpoch: destinationEpoch, Artifact: artifact, NextOffset: uint64(len(artifactBytes)), Complete: true, CoordinatorEpoch: epoch},
		fetch:         ResultFetchRequest{Artifact: artifact, ReplicaNodeID: 2, ReplicaWorkerEpoch: destinationEpoch, Offset: 0, CoordinatorEpoch: epoch},
		fetchChunk:    ResultFetchChunk{Transfer: artifactTransfer, Artifact: artifact, SourceNodeID: 2, SourceWorkerEpoch: destinationEpoch, CoordinatorEpoch: epoch},
		workerError:   WorkerError{NodeID: 1, WorkerEpoch: workerEpoch, CoordinatorEpoch: epoch, RelatedMessage: wire.MessageCraneWorkerStatusRequest, Code: WorkerErrorStaleEpoch, Detail: []byte("stale")}, grant: grant,
	}, nil
}
