package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/crane/state"
	"github.com/aaditya/cs425mp3/internal/wire"
)

// submitRequestFor builds one canonical digest-bound public submit request.
func submitRequestFor(t *testing.T, client byte, sequence uint64, topology model.TopologySpec) protocol.SubmitRequest {
	t.Helper()
	request := model.ClientRequestID{ClientID: model.ClientID{client}, Sequence: sequence}
	digest, err := protocol.SubmitCommandDigest(request, topology)
	if err != nil {
		t.Fatalf("submit digest: %v", err)
	}
	return protocol.SubmitRequest{Request: request, Topology: topology, Digest: digest}
}

// cancelRequestFor builds one canonical digest-bound public cancel request.
func cancelRequestFor(t *testing.T, client byte, sequence uint64, job model.JobID, expectedRevision uint64) protocol.CancelRequest {
	t.Helper()
	request := model.ClientRequestID{ClientID: model.ClientID{client}, Sequence: sequence}
	digest, err := protocol.CancelCommandDigest(request, job, expectedRevision)
	if err != nil {
		t.Fatalf("cancel digest: %v", err)
	}
	return protocol.CancelRequest{Request: request, JobID: job, ExpectedJobControlRevision: expectedRevision, Digest: digest}
}

func TestSubmitProposesCanonicalClientCommandAndWakes(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	epoch := fixture.seedEpochAndOpenGate()
	fixture.start()

	request := submitRequestFor(t, 0x61, 1, queryTopology(1))
	response := fixture.exchange(request)
	submitResponse, ok := response.(protocol.SubmitResponse)
	if !ok {
		t.Fatalf("submit response = %#v", response)
	}
	if err := protocol.ValidateSubmitResponseCorrelation(request, submitResponse); err != nil {
		t.Fatalf("submit correlation: %v", err)
	}
	if submitResponse.State != protocol.JobPending || submitResponse.JobControlRevision != 1 {
		t.Fatalf("submit response lifecycle = %#v", submitResponse)
	}
	proposals := fixture.raft.capturedProposals()
	if len(proposals) != 1 {
		t.Fatalf("captured %d proposals, want 1", len(proposals))
	}
	decoded, err := state.UnmarshalCommand(proposals[0])
	if err != nil {
		t.Fatalf("decode proposal: %v", err)
	}
	command, ok := decoded.(state.SubmitJob)
	if !ok {
		t.Fatalf("proposal = %T", decoded)
	}
	if command.Envelope.CoordinatorEpoch != epoch || command.Envelope.Client == nil || command.Envelope.Client.Request != request.Request || command.Envelope.Client.Digest != request.Digest {
		t.Fatalf("proposed envelope = %#v", command.Envelope)
	}
	if fixture.raft.barrierCount() == 0 {
		t.Fatal("submit skipped the leader barrier")
	}
	if fixture.wakes.Load() != 1 {
		t.Fatalf("coordinator wakes = %d, want 1", fixture.wakes.Load())
	}
	view := fixture.machine.View()
	if len(view.Jobs) != 1 || view.Jobs[0].JobID != submitResponse.JobID {
		t.Fatalf("machine jobs = %#v", view.Jobs)
	}
}

func TestSubmitRetryReturnsReplicatedCachedResult(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	request := submitRequestFor(t, 0x62, 1, queryTopology(1))
	first, ok := fixture.exchange(request).(protocol.SubmitResponse)
	if !ok {
		t.Fatal("first submit rejected")
	}
	// A lost response is retried unchanged: the replicated dedup history, not
	// any service-local sequence state, must answer with the identical result.
	second, ok := fixture.exchange(request).(protocol.SubmitResponse)
	if !ok {
		t.Fatal("retried submit rejected")
	}
	if first != second {
		t.Fatalf("cached retry result diverged: %#v vs %#v", first, second)
	}
	if got := len(fixture.raft.capturedProposals()); got != 2 {
		t.Fatalf("proposals = %d, want 2 (dedup is replicated, never local)", got)
	}
	if len(fixture.machine.View().Jobs) != 1 {
		t.Fatal("retry created a second job")
	}
	// The next sequence still succeeds, so no local sequence advanced early.
	next := submitRequestFor(t, 0x62, 2, queryTopology(2))
	if _, ok := fixture.exchange(next).(protocol.SubmitResponse); !ok {
		t.Fatal("subsequent sequence rejected after retry")
	}
}

func TestSubmitRejectionMatrixMapsTypedErrors(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	accepted := submitRequestFor(t, 0x63, 2, queryTopology(1))
	if _, ok := fixture.exchange(submitRequestFor(t, 0x63, 1, queryTopology(1))).(protocol.SubmitResponse); !ok {
		t.Fatal("seed submit sequence 1 rejected")
	}
	if _, ok := fixture.exchange(accepted).(protocol.SubmitResponse); !ok {
		t.Fatal("seed submit sequence 2 rejected")
	}
	wakesAfterSeeds := fixture.wakes.Load()

	cases := []struct {
		name      string
		request   protocol.SubmitRequest
		code      protocol.ControlErrorCode
		retryable bool
	}{
		{"StaleSequence", submitRequestFor(t, 0x63, 1, queryTopology(1)), protocol.ControlErrorStaleRequest, false},
		{"SkippedSequence", submitRequestFor(t, 0x63, 9, queryTopology(1)), protocol.ControlErrorSkippedRequest, false},
		{"IdentityReuse", submitRequestFor(t, 0x63, 2, queryTopology(2)), protocol.ControlErrorIdentityReuse, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			controlError := requireControlError(t, fixture.exchange(testCase.request), testCase.code)
			if controlError.Retryable != testCase.retryable {
				t.Fatalf("retryable = %v, want %v", controlError.Retryable, testCase.retryable)
			}
			if err := protocol.ValidateSubmitErrorCorrelation(testCase.request, controlError); err != nil {
				t.Fatalf("error correlation: %v", err)
			}
		})
	}
	if fixture.wakes.Load() != wakesAfterSeeds {
		t.Fatal("rejections woke the coordinator")
	}
}

// worstLegalControlTopology builds the maximally structured legal v1 topology:
// maximum stages/edges, maximal identifiers, the registry's largest transform,
// and maximal parallelism accepted by validation.
func worstLegalControlTopology(t *testing.T) model.TopologySpec {
	t.Helper()
	limits := model.LimitsV1()
	var largest model.OperatorSpec
	largestBytes := -1
	for _, descriptor := range model.RegistryV1().Operators {
		if descriptor.Role != model.OperatorRoleTransform {
			continue
		}
		operator := model.OperatorSpec{Name: descriptor.Name, Version: descriptor.Version}
		encodedBytes := len(descriptor.Name)
		for _, setting := range descriptor.Settings {
			value := "-9223372036854775808"
			operator.Settings = append(operator.Settings, model.Setting{Key: setting.Name, Value: value})
			encodedBytes += len(setting.Name) + len(value)
		}
		if encodedBytes > largestBytes {
			largest, largestBytes = operator, encodedBytes
		}
	}
	if largestBytes < 0 {
		t.Fatal("v1 registry has no transform operator")
	}
	stages := make([]model.StageSpec, limits.MaxStages)
	for index := range stages {
		role, operator := model.Transform, largest
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
	return model.TopologySpec{SchemaVersion: 1, Name: string(bytes.Repeat([]byte{'z'}, int(limits.MaxIdentifierBytes))), Stages: stages, Edges: edges, RegistryFingerprint: model.RegistryFingerprint()}
}

func TestSubmitMaximalTopologyBoundsAndOverBoundRejectionBeforePropose(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	topology := worstLegalControlTopology(t)
	request := submitRequestFor(t, 0x64, 1, topology)
	payload, err := protocol.MarshalControlMessage(request)
	if err != nil {
		t.Fatalf("marshal maximal submit request: %v", err)
	}
	if len(payload) > protocol.MaxControlPayloadBytes {
		t.Fatalf("maximal submit payload = %d, exceeds +6 payload bound %d", len(payload), protocol.MaxControlPayloadBytes)
	}
	view := fixture.machine.View()
	command, err := state.NewSubmitJob(request.Request, topology, view.CoordinatorEpoch)
	if err != nil {
		t.Fatalf("build maximal submit command: %v", err)
	}
	encoded, err := state.MarshalCommand(command)
	if err != nil {
		t.Fatalf("marshal maximal submit command: %v", err)
	}
	if uint64(len(encoded)) > config.MaxRaftCommandBytes {
		t.Fatalf("maximal submit command = %d, exceeds Raft command bound %d", len(encoded), config.MaxRaftCommandBytes)
	}

	response := fixture.exchange(request)
	submitResponse, ok := response.(protocol.SubmitResponse)
	if !ok {
		t.Fatalf("maximal submit rejected: %#v", response)
	}
	if err := protocol.ValidateSubmitResponseCorrelation(request, submitResponse); err != nil {
		t.Fatalf("maximal submit correlation: %v", err)
	}
	proposalsBefore := len(fixture.raft.capturedProposals())

	// An over-bound canonical command must be rejected before any Propose.
	fixture.service.maxCommand.Store(64)
	overBound := submitRequestFor(t, 0x64, 2, queryTopology(1))
	controlError := requireControlError(t, fixture.exchange(overBound), protocol.ControlErrorInvalidRequest)
	if controlError.RelatedMessage != wire.MessageCraneSubmitRequest {
		t.Fatalf("over-bound rejection binding = %#v", controlError)
	}
	if got := len(fixture.raft.capturedProposals()); got != proposalsBefore {
		t.Fatalf("over-bound request reached Propose: %d -> %d", proposalsBefore, got)
	}
}

func TestCancelProposesExactRevisionAndCorrelates(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	submit := submitRequestFor(t, 0x65, 1, queryTopology(1))
	submitResponse, ok := fixture.exchange(submit).(protocol.SubmitResponse)
	if !ok {
		t.Fatal("seed submit rejected")
	}
	request := cancelRequestFor(t, 0x65, 2, submitResponse.JobID, submitResponse.JobControlRevision)
	response := fixture.exchange(request)
	cancelResponse, ok := response.(protocol.CancelResponse)
	if !ok {
		t.Fatalf("cancel response = %#v", response)
	}
	if err := protocol.ValidateCancelResponseCorrelation(request, cancelResponse); err != nil {
		t.Fatalf("cancel correlation: %v", err)
	}
	if cancelResponse.State != protocol.JobCanceled || cancelResponse.JobControlRevision != submitResponse.JobControlRevision+1 {
		t.Fatalf("cancel lifecycle = %#v", cancelResponse)
	}
	view := fixture.machine.View()
	if len(view.Jobs) != 1 || view.Jobs[0].Lifecycle != state.JobCanceled {
		t.Fatalf("machine lifecycle = %#v", view.Jobs)
	}
	if fixture.wakes.Load() != 2 {
		t.Fatalf("coordinator wakes = %d, want 2", fixture.wakes.Load())
	}

	// The exact durable cancel retried after an ambiguous response replays the
	// identical replicated result.
	retried, ok := fixture.exchange(request).(protocol.CancelResponse)
	if !ok || retried != cancelResponse {
		t.Fatalf("cancel retry = %#v, want %#v", retried, cancelResponse)
	}
}

func TestCancelRejectionMatrixMapsTypedErrors(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()

	submit := submitRequestFor(t, 0x66, 1, queryTopology(1))
	submitResponse, ok := fixture.exchange(submit).(protocol.SubmitResponse)
	if !ok {
		t.Fatal("seed submit rejected")
	}

	revisionMismatch := cancelRequestFor(t, 0x66, 2, submitResponse.JobID, 7)
	controlError := requireControlError(t, fixture.exchange(revisionMismatch), protocol.ControlErrorRevisionMismatch)
	if err := protocol.ValidateCancelErrorCorrelation(revisionMismatch, controlError); err != nil {
		t.Fatalf("revision-mismatch correlation: %v", err)
	}

	unknownJob := cancelRequestFor(t, 0x66, 3, model.JobID{0xEE, 0x01}, 1)
	controlError = requireControlError(t, fixture.exchange(unknownJob), protocol.ControlErrorNotFound)
	if err := protocol.ValidateCancelErrorCorrelation(unknownJob, controlError); err != nil {
		t.Fatalf("not-found correlation: %v", err)
	}

	stale := cancelRequestFor(t, 0x66, 1, submitResponse.JobID, 1)
	requireControlError(t, fixture.exchange(stale), protocol.ControlErrorStaleRequest)
}

func TestStatusBarrierThenAtomicViewBindsProgress(t *testing.T) {
	seeded := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fixture := newServiceFixture(t, seeded.machine)
	if err := fixture.gate.Open(seeded.machine.View().CoordinatorEpoch); err != nil {
		t.Fatal(err)
	}
	fixture.start()

	response := fixture.exchange(statusRequest(seeded.job))
	status, ok := response.(protocol.StatusResponse)
	if !ok {
		t.Fatalf("status response = %#v", response)
	}
	if fixture.raft.barrierCount() != 1 {
		t.Fatalf("status barriers = %d, want exactly 1 before the view", fixture.raft.barrierCount())
	}
	view := seeded.machine.View()
	record := view.Jobs[0]
	if status.JobID != seeded.job || status.AppliedIndex != view.AppliedIndex || status.State != protocol.JobSucceeded {
		t.Fatalf("status identity = %#v", status)
	}
	if status.TopologyDigest != record.TopologyDigest || status.JobControlRevision != record.JobControlRevision {
		t.Fatalf("status binding = %#v", status)
	}
	if !status.HasAssignment || status.AssignmentRevision != record.Assignment.Revision || status.AssignmentDigest != record.Assignment.Digest {
		t.Fatalf("status assignment = %#v", status)
	}
	if status.SourceTaskCount != 1 || status.CompletedSourceTasks != 1 || status.ResultPartitionCount != 2 || status.ManifestCount != 2 {
		t.Fatalf("status progress = %#v", status)
	}
	if !status.HasManifestSet || status.ManifestSetDigest != seeded.digest {
		t.Fatalf("status manifest binding = %#v, want digest %x", status, seeded.digest)
	}
	if status.HasFailure {
		t.Fatalf("succeeded status carries failure: %#v", status)
	}

	partial := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 0})
	partialFixture := newServiceFixture(t, partial.machine)
	if err := partialFixture.gate.Open(partial.machine.View().CoordinatorEpoch); err != nil {
		t.Fatal(err)
	}
	partialFixture.start()
	partialStatus, ok := partialFixture.exchange(statusRequest(partial.job)).(protocol.StatusResponse)
	if !ok {
		t.Fatal("partial status rejected")
	}
	if partialStatus.State != protocol.JobDraining || partialStatus.ManifestCount != 0 || partialStatus.HasManifestSet {
		t.Fatalf("partial status = %#v", partialStatus)
	}
}

func TestStatusUnknownJobReturnsNotFoundAfterBarrier(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	fixture.seedEpochAndOpenGate()
	fixture.start()
	request := statusRequest(model.JobID{0x41})
	controlError := requireControlError(t, fixture.exchange(request), protocol.ControlErrorNotFound)
	if err := protocol.ValidateStatusErrorCorrelation(request, controlError); err != nil {
		t.Fatalf("status error correlation: %v", err)
	}
	if fixture.raft.barrierCount() != 1 {
		t.Fatalf("status barriers = %d", fixture.raft.barrierCount())
	}
}

func TestResultPageBindsCommittedManifestSetLinearizably(t *testing.T) {
	seeded := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fixture := newServiceFixture(t, seeded.machine)
	fixture.fetcher = newFakeFetcher(seeded)
	fixture.buildService()
	if err := fixture.gate.Open(seeded.machine.View().CoordinatorEpoch); err != nil {
		t.Fatal(err)
	}
	fixture.start()

	request := pageRequest(seeded, protocol.MaxResultPageBytes)
	response := fixture.exchange(request)
	page, ok := response.(protocol.ResultPageResponse)
	if !ok {
		t.Fatalf("result page response = %#v", response)
	}
	if err := protocol.ValidateResultPageResponseCorrelation(request, page); err != nil {
		t.Fatalf("result page correlation: %v", err)
	}
	if !page.End || len(page.Records) == 0 {
		t.Fatalf("result page shape = end %v records %d", page.End, len(page.Records))
	}
	if fixture.raft.barrierCount() != 1 {
		t.Fatalf("result page barriers = %d, want exactly 1", fixture.raft.barrierCount())
	}
	expected := 0
	for _, list := range seeded.records {
		expected += len(list)
	}
	if len(page.Records) != expected {
		t.Fatalf("result records = %d, want %d", len(page.Records), expected)
	}
}

func TestResultPageMapsTypedErrors(t *testing.T) {
	seeded := seedQueryFixture(t, querySeed{sinkPartitions: 2, sealPartitions: 2, succeed: true})
	fixture := newServiceFixture(t, seeded.machine)
	fixture.fetcher = newFakeFetcher(seeded)
	fixture.buildService()
	if err := fixture.gate.Open(seeded.machine.View().CoordinatorEpoch); err != nil {
		t.Fatal(err)
	}
	fixture.start()

	t.Run("PageLimitTooSmallCarriesRequiredBytesWithoutAdvancement", func(t *testing.T) {
		request := pageRequest(seeded, 1)
		controlError := requireControlError(t, fixture.exchange(request), protocol.ControlErrorPageLimitTooSmall)
		if err := protocol.ValidateResultPageErrorCorrelation(request, controlError); err != nil {
			t.Fatalf("page error correlation: %v", err)
		}
		if controlError.RequiredBytes <= request.PageBytes || controlError.RequiredBytes > uint32(protocol.MaxEncodedResultRecordBytes) {
			t.Fatalf("required bytes = %d", controlError.RequiredBytes)
		}
	})

	t.Run("ForeignManifestDigestIsUnavailable", func(t *testing.T) {
		request := pageRequest(seeded, protocol.MaxResultPageBytes)
		request.ManifestDigest = [32]byte{0xAB}
		controlError := requireControlError(t, fixture.exchange(request), protocol.ControlErrorResultUnavailable)
		if !controlError.Retryable {
			t.Fatal("unavailable result must be retryable")
		}
	})

	t.Run("UnknownJobIsNotFound", func(t *testing.T) {
		request := pageRequest(seeded, protocol.MaxResultPageBytes)
		request.JobID = model.JobID{0x59}
		request.HasLastTuple = false
		request.Last = model.TupleID{}
		requireControlError(t, fixture.exchange(request), protocol.ControlErrorNotFound)
	})
}

// gateClosingFetcher closes the shared admission gate before streaming, so the
// read observes leadership/gate loss mid-request.
type gateClosingFetcher struct {
	inner ResultFetcher
	close func()
}

func (fetcher *gateClosingFetcher) OpenPartition(ctx context.Context, request protocol.ResultFetchRequest) (RecordStream, error) {
	fetcher.close()
	return fetcher.inner.OpenPartition(ctx, request)
}

func TestResultReadsAbortOnGateLossInsteadOfServingStale(t *testing.T) {
	seeded := seedQueryFixture(t, querySeed{sinkPartitions: 1, sealPartitions: 1, succeed: true})
	fixture := newServiceFixture(t, seeded.machine)
	gate := fixture.gate
	fixture.fetcher = &gateClosingFetcher{inner: newFakeFetcher(seeded), close: func() {
		if err := gate.CloseAndWait(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
			panic(err)
		}
	}}
	fixture.buildService()
	if err := fixture.gate.Open(seeded.machine.View().CoordinatorEpoch); err != nil {
		t.Fatal(err)
	}
	fixture.start()

	request := pageRequest(seeded, protocol.MaxResultPageBytes)
	controlError := requireControlError(t, fixture.exchange(request), protocol.ControlErrorStarting)
	if !controlError.Retryable {
		t.Fatal("gate-loss abort must be retryable")
	}
}

// TestClientRejectionMapsResultTooLargeToTypedTerminalError pins the mapping
// of the consumed ResultResultTooLarge machine outcome: both mutations answer
// a request-bound, non-retryable ControlErrorResultTooLarge instead of the
// former nil (a silent drop the client resumed forever).
func TestClientRejectionMapsResultTooLargeToTypedTerminalError(t *testing.T) {
	fixture := newServiceFixture(t, state.NewMachine())
	result := state.CommandResult{Code: state.ResultResultTooLarge}

	submit := submitRequestFor(t, 0x71, 1, queryTopology(1))
	controlError := requireControlError(t, fixture.service.clientRejection(submit, result), protocol.ControlErrorResultTooLarge)
	if controlError.Retryable {
		t.Fatal("ResultTooLarge submit rejection must be terminal")
	}
	if err := protocol.ValidateSubmitErrorCorrelation(submit, controlError); err != nil {
		t.Fatalf("submit correlation: %v", err)
	}

	cancel := cancelRequestFor(t, 0x71, 2, model.JobID{0x71}, 1)
	controlError = requireControlError(t, fixture.service.clientRejection(cancel, result), protocol.ControlErrorResultTooLarge)
	if controlError.Retryable {
		t.Fatal("ResultTooLarge cancel rejection must be terminal")
	}
	if err := protocol.ValidateCancelErrorCorrelation(cancel, controlError); err != nil {
		t.Fatalf("cancel correlation: %v", err)
	}
	if _, err := protocol.MarshalControlMessage(controlError); err != nil {
		t.Fatalf("ResultTooLarge is not encodable on the public matrix: %v", err)
	}
}
