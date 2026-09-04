package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/wire"
)

func TestControlMessageTableExactIDsIndependentGoldensTruncationTrailingAndOwnership(t *testing.T) {
	fixture := controlFixture(t)
	cases := []struct {
		name    string
		typeID  wire.MessageType
		message ControlMessage
		invalid ControlMessage
		golden  func(controlMessageFixture) []byte
	}{
		{"submit_request", 240, fixture.submitRequest, SubmitRequest{}, goldenSubmitRequest},
		{"submit_response", 241, fixture.submitResponse, SubmitResponse{State: JobPending}, goldenSubmitResponse},
		{"cancel_request", 242, fixture.cancelRequest, CancelRequest{Request: fixture.request, JobID: fixture.job, ExpectedJobControlRevision: 9}, goldenCancelRequest},
		{"cancel_response", 243, fixture.cancelResponse, CancelResponse{Request: fixture.request, Digest: fixture.cancelDigest, JobID: fixture.job, JobControlRevision: 10, State: JobPending}, goldenCancelResponse},
		{"status_request", 244, fixture.statusRequest, StatusRequest{}, goldenStatusRequest},
		{"status_response", 245, fixture.statusResponse, StatusResponse{JobID: fixture.job, AppliedIndex: 1, TopologyDigest: fixture.topologyDigest, JobControlRevision: 1, State: JobRunning, HasAssignment: true}, goldenStatusResponse},
		{"result_page_request", 246, fixture.pageRequest, ResultPageRequest{JobID: fixture.job, ManifestDigest: fixture.manifestDigest, PageBytes: 0}, goldenResultPageRequest},
		{"result_page_response", 247, fixture.pageResponse, invalidReorderedPage(fixture.pageResponse), goldenResultPageResponse},
		{"leader_redirect", 248, fixture.redirect, LeaderRedirect{Endpoints: []string{"node-1.example.test:8006", "node-1.example.test:8006"}}, goldenLeaderRedirect},
		{"control_error", 249, fixture.controlError, ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, Code: ControlErrorPageLimitTooSmall}, goldenControlError},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.message.MessageType(); got != test.typeID {
				t.Fatalf("MessageType() = %d, want %d", got, test.typeID)
			}
			encoded, err := MarshalControlMessage(test.message)
			if err != nil {
				t.Fatalf("MarshalControlMessage: %v", err)
			}
			want := test.golden(fixture)
			if !bytes.Equal(encoded, want) {
				t.Fatalf("independent golden mismatch\n got: %x\nwant: %x", encoded, want)
			}
			if len(encoded) > MaxControlPayloadBytes {
				t.Fatalf("payload = %d, max %d", len(encoded), MaxControlPayloadBytes)
			}
			if binary.BigEndian.Uint16(encoded[:2]) != ControlSchemaVersion || binary.BigEndian.Uint16(encoded[2:4]) != uint16(test.typeID) {
				t.Fatalf("wrong schema/type prefix: %x", encoded[:4])
			}
			decoded, err := UnmarshalControlMessage(test.typeID, encoded)
			if err != nil {
				t.Fatalf("UnmarshalControlMessage: %v", err)
			}
			if !reflect.DeepEqual(decoded, test.message) {
				t.Fatalf("round trip mismatch\n got: %#v\nwant: %#v", decoded, test.message)
			}
			for cut := 0; cut < len(encoded); cut++ {
				if _, err := UnmarshalControlMessage(test.typeID, encoded[:cut]); err == nil {
					t.Fatalf("accepted truncation at %d/%d", cut, len(encoded))
				}
			}
			trailing := append(append([]byte(nil), encoded...), 0)
			if _, err := UnmarshalControlMessage(test.typeID, trailing); err == nil {
				t.Fatal("accepted trailing byte")
			}
			if _, err := MarshalControlMessage(test.invalid); err == nil {
				t.Fatalf("accepted invalid message %#v", test.invalid)
			}
		})
	}

	for _, unknown := range []wire.MessageType{239, 250, wire.MessageCraneWorkerReserved} {
		if _, err := UnmarshalControlMessage(unknown, []byte{0, 1, 0, byte(unknown)}); !errors.Is(err, ErrUnexpectedControlMessage) {
			t.Fatalf("unknown type %d error = %v", unknown, err)
		}
	}
	encoded, err := MarshalControlMessage(fixture.statusRequest)
	if err != nil {
		t.Fatal(err)
	}
	wrongSchema := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(wrongSchema[:2], ControlSchemaVersion+1)
	if _, err := UnmarshalControlMessage(fixture.statusRequest.MessageType(), wrongSchema); !errors.Is(err, ErrUnsupportedControlSchema) {
		t.Fatalf("schema mismatch error = %v", err)
	}
	wrongType := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(wrongType[2:4], uint16(wire.MessageCraneSubmitRequest))
	if _, err := UnmarshalControlMessage(fixture.statusRequest.MessageType(), wrongType); !errors.Is(err, ErrUnexpectedControlMessage) {
		t.Fatalf("payload/header type mismatch error = %v", err)
	}

	pageBytes, err := MarshalControlMessage(fixture.pageResponse)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalControlMessage(wire.MessageCraneResultPageResponse, pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	owned := decoded.(ResultPageResponse)
	pageBytes[len(pageBytes)-2] ^= 0xff
	fixture.pageResponse.Records[0].Value[0] ^= 0xff
	if bytes.Equal(owned.Records[0].Value, fixture.pageResponse.Records[0].Value) {
		t.Fatal("decoded result records alias the caller or encoded buffer")
	}
	redirectBytes, err := MarshalControlMessage(fixture.redirect)
	if err != nil {
		t.Fatal(err)
	}
	redirectMessage, err := UnmarshalControlMessage(wire.MessageCraneLeaderRedirect, redirectBytes)
	if err != nil {
		t.Fatal(err)
	}
	ownedRedirect := redirectMessage.(LeaderRedirect)
	fixture.redirect.Endpoints[0] = "changed.example.test:1"
	redirectBytes[8] ^= 0xff
	if ownedRedirect.Endpoints[0] != "node-1.example.test:8006" {
		t.Fatal("decoded redirect endpoint is not owned")
	}
}

func TestTypedControlCodecWrappersUseExactMessageAssociations(t *testing.T) {
	fixture := controlFixture(t)
	tests := []struct {
		name string
		run  func() error
	}{
		{"submit request", func() error {
			b, e := MarshalSubmitRequest(fixture.submitRequest)
			if e != nil {
				return e
			}
			_, e = UnmarshalSubmitRequest(b)
			return e
		}},
		{"submit response", func() error {
			b, e := MarshalSubmitResponse(fixture.submitResponse)
			if e != nil {
				return e
			}
			_, e = UnmarshalSubmitResponse(b)
			return e
		}},
		{"cancel request", func() error {
			b, e := MarshalCancelRequest(fixture.cancelRequest)
			if e != nil {
				return e
			}
			_, e = UnmarshalCancelRequest(b)
			return e
		}},
		{"cancel response", func() error {
			b, e := MarshalCancelResponse(fixture.cancelResponse)
			if e != nil {
				return e
			}
			_, e = UnmarshalCancelResponse(b)
			return e
		}},
		{"status request", func() error {
			b, e := MarshalStatusRequest(fixture.statusRequest)
			if e != nil {
				return e
			}
			_, e = UnmarshalStatusRequest(b)
			return e
		}},
		{"status response", func() error {
			b, e := MarshalStatusResponse(fixture.statusResponse)
			if e != nil {
				return e
			}
			_, e = UnmarshalStatusResponse(b)
			return e
		}},
		{"page request", func() error {
			b, e := MarshalResultPageRequest(fixture.pageRequest)
			if e != nil {
				return e
			}
			_, e = UnmarshalResultPageRequest(b)
			return e
		}},
		{"page response", func() error {
			b, e := MarshalResultPageResponse(fixture.pageResponse)
			if e != nil {
				return e
			}
			_, e = UnmarshalResultPageResponse(b)
			return e
		}},
		{"redirect", func() error {
			b, e := MarshalLeaderRedirect(fixture.redirect)
			if e != nil {
				return e
			}
			_, e = UnmarshalLeaderRedirect(b)
			return e
		}},
		{"control error", func() error {
			b, e := MarshalControlError(fixture.controlError)
			if e != nil {
				return e
			}
			_, e = UnmarshalControlError(b)
			return e
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSubmitAndCancelCommandDigestsBindEveryCanonicalDefiningByte(t *testing.T) {
	fixture := controlFixture(t)
	wantSubmit := independentSubmitDigest(t, fixture.request, fixture.topology)
	gotSubmit, err := SubmitCommandDigest(fixture.request, fixture.topology)
	if err != nil || gotSubmit != wantSubmit {
		t.Fatalf("SubmitCommandDigest = %x,%v want %x", gotSubmit, err, wantSubmit)
	}
	wantCancel := independentCancelDigest(fixture.request, fixture.job, 9)
	gotCancel, err := CancelCommandDigest(fixture.request, fixture.job, 9)
	if err != nil || gotCancel != wantCancel {
		t.Fatalf("CancelCommandDigest = %x,%v want %x", gotCancel, err, wantCancel)
	}

	for name, mutate := range map[string]func(*SubmitRequest){
		"client":   func(v *SubmitRequest) { v.Request.ClientID[0]++ },
		"sequence": func(v *SubmitRequest) { v.Request.Sequence++ },
		"topology": func(v *SubmitRequest) { v.Topology.Name = "changed" },
		"digest":   func(v *SubmitRequest) { v.Digest[0]++ },
	} {
		t.Run("submit "+name, func(t *testing.T) {
			changed := fixture.submitRequest
			mutate(&changed)
			if _, err := MarshalSubmitRequest(changed); err == nil {
				t.Fatal("accepted submit request whose exact digest no longer matches")
			}
		})
	}
	for name, mutate := range map[string]func(*CancelRequest){
		"client":   func(v *CancelRequest) { v.Request.ClientID[0]++ },
		"sequence": func(v *CancelRequest) { v.Request.Sequence++ },
		"job":      func(v *CancelRequest) { v.JobID[0]++ },
		"revision": func(v *CancelRequest) { v.ExpectedJobControlRevision++ },
		"digest":   func(v *CancelRequest) { v.Digest[0]++ },
	} {
		t.Run("cancel "+name, func(t *testing.T) {
			changed := fixture.cancelRequest
			mutate(&changed)
			if _, err := MarshalCancelRequest(changed); err == nil {
				t.Fatal("accepted cancel request whose exact digest no longer matches")
			}
		})
	}
}

func TestMutationResponsesCorrelateExactDurableIdentityAndDigest(t *testing.T) {
	fixture := controlFixture(t)
	if err := ValidateSubmitResponseCorrelation(fixture.submitRequest, fixture.submitResponse); err != nil {
		t.Fatalf("valid submit correlation: %v", err)
	}
	if err := ValidateCancelResponseCorrelation(fixture.cancelRequest, fixture.cancelResponse); err != nil {
		t.Fatalf("valid cancel correlation: %v", err)
	}
	for name, mutate := range map[string]func(*SubmitResponse){
		"request": func(v *SubmitResponse) { v.Request.Sequence++ },
		"digest":  func(v *SubmitResponse) { v.Digest[0]++ },
		"job":     func(v *SubmitResponse) { v.JobID[0]++ },
	} {
		t.Run("submit "+name, func(t *testing.T) {
			changed := fixture.submitResponse
			mutate(&changed)
			if err := ValidateSubmitResponseCorrelation(fixture.submitRequest, changed); err == nil {
				t.Fatal("accepted submit response for another durable command")
			}
		})
	}
	for name, mutate := range map[string]func(*CancelResponse){
		"request": func(v *CancelResponse) { v.Request.Sequence++ },
		"digest":  func(v *CancelResponse) { v.Digest[0]++ },
		"job":     func(v *CancelResponse) { v.JobID[0]++ },
	} {
		t.Run("cancel "+name, func(t *testing.T) {
			changed := fixture.cancelResponse
			mutate(&changed)
			if err := ValidateCancelResponseCorrelation(fixture.cancelRequest, changed); err == nil {
				t.Fatal("accepted cancel response for another durable command")
			}
		})
	}
}

func TestStatusResponseBindsConcreteReplicatedAndResultIdentities(t *testing.T) {
	fixture := controlFixture(t)
	valid := fixture.statusResponse
	mutations := map[string]func(*StatusResponse){
		"zero applied index":         func(v *StatusResponse) { v.AppliedIndex = 0 },
		"zero topology digest":       func(v *StatusResponse) { v.TopologyDigest = [32]byte{} },
		"zero revision":              func(v *StatusResponse) { v.JobControlRevision = 0 },
		"unknown state":              func(v *StatusResponse) { v.State = 99 },
		"partial assignment":         func(v *StatusResponse) { v.AssignmentDigest = [32]byte{} },
		"assignment without flag":    func(v *StatusResponse) { v.HasAssignment = false },
		"completed sources too high": func(v *StatusResponse) { v.CompletedSourceTasks = v.SourceTaskCount + 1 },
		"manifest digest missing":    func(v *StatusResponse) { v.ManifestSetDigest = [32]byte{} },
		"manifest without flag":      func(v *StatusResponse) { v.HasManifestSet = false },
		"failure outside failed": func(v *StatusResponse) {
			v.HasFailure = true
			v.FailureCode = model.FailureOperator
			v.FailureDetailDigest = [32]byte{1}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if _, err := MarshalStatusResponse(changed); err == nil {
				t.Fatalf("accepted contradictory status: %#v", changed)
			}
		})
	}
	failed := valid
	failed.State = JobFailed
	failed.HasManifestSet = false
	failed.ManifestSetDigest = [32]byte{}
	failed.ManifestCount = 0
	failed.HasFailure = true
	failed.FailureCode = model.FailureStorage
	failed.FailureDetailDigest = [32]byte{0x55}
	if _, err := MarshalStatusResponse(failed); err != nil {
		t.Fatalf("rejected concrete failed status: %v", err)
	}
	succeeded := valid
	succeeded.State = JobSucceeded
	succeeded.CompletedSourceTasks = succeeded.SourceTaskCount
	if _, err := MarshalStatusResponse(succeeded); err != nil {
		t.Fatalf("rejected complete succeeded status: %v", err)
	}
}

func TestResultPageGlobalCursorManifestBindingOrderingNoSplitAndEndSemantics(t *testing.T) {
	fixture := controlFixture(t)
	valid := fixture.pageResponse
	entryBytes := 0
	for _, record := range valid.Records {
		encoded, err := EncodedResultPageRecordBytes(record)
		if err != nil {
			t.Fatal(err)
		}
		entryBytes += len(encoded)
	}
	if entryBytes != int(valid.PageBytes) {
		t.Fatalf("fixture page entries = %d, PageBytes = %d", entryBytes, valid.PageBytes)
	}

	mutations := map[string]func(*ResultPageResponse){
		"foreign request cursor": func(v *ResultPageResponse) { v.RequestLast.JobID[0]++ },
		"record at cursor":       func(v *ResultPageResponse) { v.Records[0].TupleID = v.RequestLast },
		"reordered records":      func(v *ResultPageResponse) { v.Records[0], v.Records[1] = v.Records[1], v.Records[0] },
		"cross-job record": func(v *ResultPageResponse) {
			v.Records[0].TupleID.JobID[0]++
			v.Records[0].SinkTask.JobID = v.Records[0].TupleID.JobID
		},
		"page byte overflow":  func(v *ResultPageResponse) { v.PageBytes-- },
		"missing next flag":   func(v *ResultPageResponse) { v.NextHasLastTuple = false },
		"wrong next tuple":    func(v *ResultPageResponse) { v.NextLast = v.Records[0].TupleID },
		"zero next with flag": func(v *ResultPageResponse) { v.NextLast = model.TupleID{} },
	}
	if err := ValidateResultPageResponseCorrelation(fixture.pageRequest, valid); err != nil {
		t.Fatalf("valid page correlation: %v", err)
	}
	for name, mutate := range map[string]func(*ResultPageResponse){
		"job":      func(v *ResultPageResponse) { v.JobID[0]++ },
		"manifest": func(v *ResultPageResponse) { v.ManifestDigest[0]++ },
		"selector": func(v *ResultPageResponse) { v.RequestHasLastTuple = false; v.RequestLast = model.TupleID{} },
		"cursor":   func(v *ResultPageResponse) { v.RequestLast.SourceSequence++ },
		"limit":    func(v *ResultPageResponse) { v.PageBytes++ },
	} {
		t.Run("correlation "+name, func(t *testing.T) {
			changed := clonePage(valid)
			mutate(&changed)
			if err := ValidateResultPageResponseCorrelation(fixture.pageRequest, changed); err == nil {
				t.Fatal("accepted response for another request binding")
			}
		})
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := clonePage(valid)
			mutate(&changed)
			if _, err := MarshalResultPageResponse(changed); err == nil {
				t.Fatalf("accepted invalid page: %#v", changed)
			}
		})
	}

	empty := ResultPageResponse{
		JobID: fixture.job, ManifestDigest: fixture.manifestDigest,
		RequestHasLastTuple: true, RequestLast: fixture.pageResponse.NextLast,
		PageBytes: 1, NextHasLastTuple: true, NextLast: fixture.pageResponse.NextLast, End: true,
	}
	if _, err := MarshalResultPageResponse(empty); err != nil {
		t.Fatalf("rejected empty terminal page preserving cursor: %v", err)
	}
	contradictions := []ResultPageResponse{
		func() ResultPageResponse { v := empty; v.End = false; return v }(),
		func() ResultPageResponse { v := empty; v.NextLast = fixture.pageRequest.Last; return v }(),
		func() ResultPageResponse { v := empty; v.NextHasLastTuple = false; return v }(),
	}
	for _, invalid := range contradictions {
		if _, err := MarshalResultPageResponse(invalid); err == nil {
			t.Fatalf("accepted contradictory empty page: %#v", invalid)
		}
	}
	firstEmpty := ResultPageResponse{JobID: fixture.job, ManifestDigest: fixture.manifestDigest, PageBytes: 1, End: true}
	if _, err := MarshalResultPageResponse(firstEmpty); err != nil {
		t.Fatalf("rejected empty first terminal page: %v", err)
	}
}

func TestResultPageLimitsAndPageLimitTooSmallRepeatBindingWithoutAdvancement(t *testing.T) {
	fixture := controlFixture(t)
	for _, pageBytes := range []uint32{1, MaxResultPageBytes} {
		request := fixture.pageRequest
		request.PageBytes = pageBytes
		if _, err := MarshalResultPageRequest(request); err != nil {
			t.Fatalf("page limit %d: %v", pageBytes, err)
		}
	}
	for _, pageBytes := range []uint32{0, MaxResultPageBytes + 1} {
		request := fixture.pageRequest
		request.PageBytes = pageBytes
		if _, err := MarshalResultPageRequest(request); err == nil {
			t.Fatalf("accepted page limit %d", pageBytes)
		}
	}

	firstEntry, err := EncodedResultPageRecordBytes(fixture.pageResponse.Records[0])
	if err != nil {
		t.Fatal(err)
	}
	request := fixture.pageRequest
	request.PageBytes = uint32(len(firstEntry) - 1)
	pageError := ControlError{
		RelatedMessage: wire.MessageCraneResultPageRequest,
		Code:           ControlErrorPageLimitTooSmall,
		HasResultPage:  true,
		ResultPage:     request,
		RequiredBytes:  uint32(len(firstEntry)),
		Detail:         []byte("first complete record does not fit"),
	}
	encoded, err := MarshalControlError(pageError)
	if err != nil {
		t.Fatalf("PageLimitTooSmall: %v", err)
	}
	decoded, err := UnmarshalControlError(encoded)
	if err != nil || !reflect.DeepEqual(decoded, pageError) {
		t.Fatalf("PageLimitTooSmall round trip = %#v,%v", decoded, err)
	}
	for name, mutate := range map[string]func(*ControlError){
		"missing binding":  func(v *ControlError) { v.HasResultPage = false; v.ResultPage = ResultPageRequest{} },
		"wrong relation":   func(v *ControlError) { v.RelatedMessage = wire.MessageCraneStatusRequest },
		"zero required":    func(v *ControlError) { v.RequiredBytes = 0 },
		"not larger":       func(v *ControlError) { v.RequiredBytes = v.ResultPage.PageBytes },
		"above record max": func(v *ControlError) { v.RequiredBytes = MaxEncodedResultRecordBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := pageError
			mutate(&changed)
			if _, err := MarshalControlError(changed); err == nil {
				t.Fatalf("accepted invalid PageLimitTooSmall: %#v", changed)
			}
		})
	}
	other := fixture.controlError
	other.Code = ControlErrorResultUnavailable
	other.RequiredBytes = 1
	if _, err := MarshalControlError(other); err == nil {
		t.Fatal("accepted RequiredBytes on a non-page-limit error")
	}
}

func TestControlErrorBindingSelectorsAreExclusiveAndMatchRelatedRequest(t *testing.T) {
	fixture := controlFixture(t)
	mutation := ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, Code: ControlErrorCapacityExhausted, HasClientRequest: true, ClientRequest: fixture.request, ClientDigest: fixture.submitDigest, Detail: []byte("capacity")}
	status := ControlError{RelatedMessage: wire.MessageCraneStatusRequest, Code: ControlErrorNotFound, HasStatusRequest: true, StatusJobID: fixture.job, Detail: []byte("missing")}
	unboundMutation := ControlError{RelatedMessage: wire.MessageCraneCancelRequest, Code: ControlErrorMalformed, Detail: []byte("predecode")}
	boundPage := ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, Code: ControlErrorStarting, Retryable: true, HasResultPage: true, ResultPage: fixture.pageRequest}
	for _, valid := range []ControlError{mutation, status, fixture.controlError, unboundMutation, boundPage} {
		if _, err := MarshalControlError(valid); err != nil {
			t.Fatalf("valid selector %#v: %v", valid, err)
		}
	}
	invalid := []ControlError{
		func() ControlError {
			v := mutation
			v.HasResultPage = true
			v.ResultPage = fixture.pageRequest
			return v
		}(),
		func() ControlError { v := mutation; v.RelatedMessage = wire.MessageCraneResultPageRequest; return v }(),
		func() ControlError {
			v := mutation
			v.HasClientRequest = false
			v.ClientRequest = model.ClientRequestID{}
			v.ClientDigest = [32]byte{}
			return v
		}(),
		func() ControlError { v := mutation; v.ClientDigest = [32]byte{}; return v }(),
		func() ControlError {
			v := fixture.controlError
			v.RelatedMessage = wire.MessageCraneCancelRequest
			return v
		}(),
		func() ControlError {
			v := status
			v.HasClientRequest = true
			v.ClientRequest = fixture.request
			v.ClientDigest = fixture.submitDigest
			return v
		}(),
		func() ControlError { v := status; v.RelatedMessage = wire.MessageCraneSubmitResponse; return v }(),
	}
	for _, value := range invalid {
		if _, err := MarshalControlError(value); err == nil {
			t.Fatalf("accepted mismatched error selector: %#v", value)
		}
	}
}

func TestLeaderRedirectBoundsCanonicalControlEndpointsUniquenessAndOwnership(t *testing.T) {
	validCounts := [][]string{
		{"127.0.0.1:8006"},
		{"node-1.example.test:8006", "node-2.example.test:8106", "node-3.example.test:8206"},
		{"node-1.example.test:8006", "node-2.example.test:8106", "node-3.example.test:8206", "node-4.example.test:8306", "node-5.example.test:8406"},
	}
	for _, endpoints := range validCounts {
		if _, err := MarshalLeaderRedirect(LeaderRedirect{Endpoints: endpoints}); err != nil {
			t.Fatalf("valid redirect %v: %v", endpoints, err)
		}
	}
	invalid := [][]string{
		nil,
		{"node-1.example.test:8006", "node-2.example.test:8106"},
		{"node-1.example.test:8006", "node-2.example.test:8106", "node-3.example.test:8206", "node-4.example.test:8306"},
		{"node-2.example.test:8106", "node-1.example.test:8006"},
		{"node-1.example.test:8006", "node-1.example.test:8006"},
		{"Node-1.example.test:8006"},
		{"node-1.example.test.:8006"},
		{"node 1.example.test:8006"},
		{"127.000.000.001:8006"},
		{"[2001:0db8::1]:8006"},
		{"node.example.test:0"},
		{"node.example.test:65536"},
		{"1:1", "2:2", "3:3", "4:4", "5:5", "6:6"},
		{strings.Repeat("a", MaxControlEndpointBytes) + ":1"},
	}
	for _, endpoints := range invalid {
		if _, err := MarshalLeaderRedirect(LeaderRedirect{Endpoints: endpoints}); err == nil {
			t.Fatalf("accepted invalid redirect endpoints %q", endpoints)
		}
	}
}

func TestControlDecoderPreflightsFrameCountsAndDeclaredLengthsBeforeAllocation(t *testing.T) {
	if _, err := UnmarshalControlMessage(wire.MessageCraneStatusRequest, make([]byte, MaxControlPayloadBytes+1)); !errors.Is(err, ErrControlMessageTooLarge) {
		t.Fatalf("payload maximum + 1 = %v", err)
	}

	fixture := controlFixture(t)
	responseBytes, err := MarshalResultPageResponse(fixture.pageResponse)
	if err != nil {
		t.Fatal(err)
	}
	// Prefix, JobID, manifest, request cursor, and PageBytes precede the record count.
	const resultPageCountOffset = 4 + 16 + 32 + 1 + 76 + 4
	malformedCount := append([]byte(nil), responseBytes[:resultPageCountOffset+2]...)
	binary.BigEndian.PutUint16(malformedCount[resultPageCountOffset:], math.MaxUint16)
	if _, err := UnmarshalResultPageResponse(malformedCount); !errors.Is(err, ErrMalformedControlMessage) {
		t.Fatalf("impossible result count error = %v", err)
	}

	redirect := []byte{0, 1, 0, 248, 0, 1, 1, 4}
	if _, err := UnmarshalLeaderRedirect(redirect); !errors.Is(err, ErrMalformedControlMessage) {
		t.Fatalf("oversized declared endpoint error = %v", err)
	}

	page := fixture.pageResponse
	page.Records = make([]model.ResultRecord, MaxResultPageRecords+1)
	if _, err := MarshalResultPageResponse(page); err == nil {
		t.Fatal("accepted record count above allocation bound")
	}
}

func TestSubmitRequestWorstStructuredLegalTopologyRealAuthenticatedFrameAndDeclaredBoundary(t *testing.T) {
	install := worstLegalAssignmentInstall(t)
	topology := install.Specification
	limits := model.LimitsV1()
	if uint64(len(topology.Stages)) != limits.MaxStages || uint64(len(topology.Edges)) != limits.MaxEdges || len(topology.Name) != int(limits.MaxIdentifierBytes) {
		t.Fatal("maximum submit fixture misses topology/name structural maxima")
	}
	if topology.Stages[0].Operator.Name != "range" || len(topology.Stages[0].Operator.Settings) != 2 || topology.Stages[len(topology.Stages)-1].Operator.Name != "collect" {
		t.Fatal("maximum submit fixture does not use validated source/settings/sink maxima")
	}
	for index, stage := range topology.Stages {
		if len(stage.Name) != int(limits.MaxIdentifierBytes) {
			t.Fatalf("stage %d name is not maximal", index)
		}
		if stage.Role == model.Transform && !reflect.DeepEqual(stage.Operator, largestLegalTransformOperator(t)) {
			t.Fatalf("stage %d does not use registry-selected largest legal transform", index)
		}
	}
	for index, edge := range topology.Edges {
		if edge.Routing != model.FieldHash || edge.Field != "value" {
			t.Fatalf("edge %d does not use largest legal routing representation", index)
		}
	}

	requestID := model.ClientRequestID{ClientID: model.ClientID{0x77}, Sequence: 1}
	digest, err := SubmitCommandDigest(requestID, topology)
	if err != nil {
		t.Fatal(err)
	}
	request := SubmitRequest{Request: requestID, Topology: topology, Digest: digest}
	payload, err := MarshalSubmitRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}
	frameLimits := wire.DefaultLimits()
	frameLimits.MaxFrameSize = int(limits.MaxControlFrameBytes)
	frame, err := wire.Encode(wire.Header{Version: wire.Version1, Message: wire.MessageCraneSubmitRequest, ClusterID: [16]byte{1}, SenderID: 1, RequestID: wire.RequestID{1}, TimestampMillis: 1, Codec: wire.CodecBinary}, payload, wire.NewHMACAuthenticator([]byte("public-control-maximum-frame-key!")), frameLimits)
	if err != nil {
		t.Fatalf("authenticated maximum SubmitRequest: %v", err)
	}
	wantBytes, err := model.CompleteSubmitRequestBytes(uint64(len(validated.CanonicalBytes())))
	if err != nil || wantBytes != uint64(len(frame)) {
		t.Fatalf("model/real SubmitRequest bytes = %d/%d/%v", wantBytes, len(frame), err)
	}
	if uint64(len(frame)) > limits.MaxControlFrameBytes {
		t.Fatalf("maximum authenticated SubmitRequest = %d, max %d", len(frame), limits.MaxControlFrameBytes)
	}
	if _, err := UnmarshalSubmitRequest(payload); err != nil {
		t.Fatalf("maximum SubmitRequest decoder: %v", err)
	}
	t.Logf("actual 64-stage/256-edge/1,024-task-topology authenticated SubmitRequest: %d bytes", len(frame))

	decodeSentinel := errors.New("decode boundary reached")
	prefixBytes := 4 + 24
	exact := make([]byte, prefixBytes+int(limits.MaxTopologyBytes)+32)
	binary.BigEndian.PutUint16(exact[:2], ControlSchemaVersion)
	binary.BigEndian.PutUint16(exact[2:4], uint16(wire.MessageCraneSubmitRequest))
	copy(exact[4:20], requestID.ClientID[:])
	binary.BigEndian.PutUint64(exact[20:28], requestID.Sequence)
	binary.BigEndian.PutUint64(exact[28:36], limits.MaxTopologyBytes-8)
	called := false
	if _, err := unmarshalSubmitRequestWith(exact, func([]byte) (model.ValidatedTopology, error) {
		called = true
		return model.ValidatedTopology{}, decodeSentinel
	}); !called || err == nil || !strings.Contains(err.Error(), decodeSentinel.Error()) {
		t.Fatalf("exact declared topology boundary did not reach decoder: called=%v err=%v", called, err)
	}

	over := make([]byte, prefixBytes+8)
	binary.BigEndian.PutUint16(over[:2], ControlSchemaVersion)
	binary.BigEndian.PutUint16(over[2:4], uint16(wire.MessageCraneSubmitRequest))
	copy(over[4:20], requestID.ClientID[:])
	binary.BigEndian.PutUint64(over[20:28], requestID.Sequence)
	binary.BigEndian.PutUint64(over[28:36], limits.MaxTopologyBytes-8+1)
	called = false
	if _, err := unmarshalSubmitRequestWith(over, func([]byte) (model.ValidatedTopology, error) {
		called = true
		return model.ValidatedTopology{}, nil
	}); err == nil || called {
		t.Fatalf("topology maximum + 1 reached decoder/publication: called=%v err=%v", called, err)
	}

	if complete, err := model.CompleteSubmitRequestBytes(limits.MaxControlFrameBytes - limits.SubmitRequestFixedBytes); err != nil || complete != limits.MaxControlFrameBytes {
		t.Fatalf("exact abstract SubmitRequest boundary = %d,%v", complete, err)
	}
	if _, err := model.CompleteSubmitRequestBytes(limits.MaxControlFrameBytes - limits.SubmitRequestFixedBytes + 1); err == nil {
		t.Fatal("abstract SubmitRequest boundary + 1 accepted")
	}
}

func FuzzUnmarshalControlMessage(f *testing.F) {
	fixture, err := makeControlFixture()
	if err == nil {
		for _, message := range fixture.messages() {
			encoded, marshalErr := MarshalControlMessage(message)
			if marshalErr == nil {
				f.Add(uint16(message.MessageType()), encoded)
			}
		}
	}
	f.Fuzz(func(t *testing.T, typeID uint16, encoded []byte) {
		if len(encoded) > MaxControlPayloadBytes+1 {
			encoded = encoded[:MaxControlPayloadBytes+1]
		}
		_, _ = UnmarshalControlMessage(wire.MessageType(typeID), encoded)
	})
}

type controlMessageFixture struct {
	request        model.ClientRequestID
	topology       model.TopologySpec
	topologyDigest [32]byte
	job            model.JobID
	manifestDigest [32]byte
	submitDigest   [32]byte
	cancelDigest   [32]byte
	submitRequest  SubmitRequest
	submitResponse SubmitResponse
	cancelRequest  CancelRequest
	cancelResponse CancelResponse
	statusRequest  StatusRequest
	statusResponse StatusResponse
	pageRequest    ResultPageRequest
	pageResponse   ResultPageResponse
	redirect       LeaderRedirect
	controlError   ControlError
}

func (fixture controlMessageFixture) messages() []ControlMessage {
	return []ControlMessage{fixture.submitRequest, fixture.submitResponse, fixture.cancelRequest, fixture.cancelResponse, fixture.statusRequest, fixture.statusResponse, fixture.pageRequest, fixture.pageResponse, fixture.redirect, fixture.controlError}
}

func controlFixture(t *testing.T) controlMessageFixture {
	t.Helper()
	fixture, err := makeControlFixture()
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func makeControlFixture() (controlMessageFixture, error) {
	request := model.ClientRequestID{ClientID: model.ClientID{0x11}, Sequence: 7}
	topology := model.TopologySpec{SchemaVersion: 1, Name: "control", RegistryFingerprint: model.RegistryFingerprint(), Stages: []model.StageSpec{
		{StageID: 1, Name: "source", Role: model.Source, Parallelism: 1, Operator: model.OperatorSpec{Name: "range", Version: 1, Settings: []model.Setting{{Key: "end_exclusive", Value: "4"}, {Key: "start", Value: "1"}}}},
		{StageID: 2, Name: "sink", Role: model.Sink, Parallelism: 1, Operator: model.OperatorSpec{Name: "collect", Version: 1}},
	}, Edges: []model.EdgeSpec{{EdgeID: 1, SourceStageID: 1, DestinationStageID: 2, Routing: model.Shuffle}}}
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		return controlMessageFixture{}, err
	}
	submitDigest := independentSubmitDigestValue(request, validated.CanonicalBytes())
	job := model.DeriveJobID(request, validated.Digest())
	cancelDigest := independentCancelDigest(request, job, 9)
	manifestDigest := sha256.Sum256([]byte("manifest-set"))
	source := model.TaskID{JobID: job, StageID: 1}
	sink := model.TaskID{JobID: job, StageID: 2}
	value, err := model.MarshalTuple(model.Tuple{})
	if err != nil {
		return controlMessageFixture{}, err
	}
	last := model.DeriveSourceTupleID(job, source, 1)
	records := make([]model.ResultRecord, 2)
	for index := range records {
		records[index], err = model.NewResultRecord(model.DeriveSourceTupleID(job, source, uint64(index+2)), sink, validated.Digest(), value)
		if err != nil {
			return controlMessageFixture{}, err
		}
	}
	pageBytes := 0
	for _, record := range records {
		stream, streamErr := model.MarshalResultRecord(record)
		if streamErr != nil {
			return controlMessageFixture{}, streamErr
		}
		pageBytes += 4 + len(stream)
	}
	pageRequest := ResultPageRequest{JobID: job, ManifestDigest: manifestDigest, HasLastTuple: true, Last: last, PageBytes: uint32(pageBytes)}
	pageResponse := ResultPageResponse{JobID: job, ManifestDigest: manifestDigest, RequestHasLastTuple: true, RequestLast: last, PageBytes: uint32(pageBytes), Records: records, NextHasLastTuple: true, NextLast: records[len(records)-1].TupleID, End: false}
	pageLimitRequest := pageRequest
	pageLimitRequest.PageBytes = 1
	return controlMessageFixture{
		request: request, topology: topology, topologyDigest: validated.Digest(), job: job, manifestDigest: manifestDigest, submitDigest: submitDigest, cancelDigest: cancelDigest,
		submitRequest:  SubmitRequest{Request: request, Topology: topology, Digest: submitDigest},
		submitResponse: SubmitResponse{Request: request, Digest: submitDigest, JobID: job, JobControlRevision: 1, State: JobPending},
		cancelRequest:  CancelRequest{Request: request, JobID: job, ExpectedJobControlRevision: 9, Digest: cancelDigest},
		cancelResponse: CancelResponse{Request: request, Digest: cancelDigest, JobID: job, JobControlRevision: 10, State: JobCanceled},
		statusRequest:  StatusRequest{JobID: job},
		statusResponse: StatusResponse{JobID: job, AppliedIndex: 11, TopologyDigest: validated.Digest(), JobControlRevision: 10, State: JobDraining, HasAssignment: true, AssignmentRevision: 3, AssignmentDigest: [32]byte{0x22}, SourceTaskCount: 1, ResultPartitionCount: 1, CompletedSourceTasks: 1, ManifestCount: 1, HasManifestSet: true, ManifestSetDigest: manifestDigest},
		pageRequest:    pageRequest, pageResponse: pageResponse,
		redirect:     LeaderRedirect{Endpoints: []string{"node-1.example.test:8006", "node-2.example.test:8106", "node-3.example.test:8206"}},
		controlError: ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, Code: ControlErrorPageLimitTooSmall, HasResultPage: true, ResultPage: pageLimitRequest, RequiredBytes: uint32(4 + mustResultStreamLength(records[0])), Detail: []byte("page too small")},
	}, nil
}

func mustResultStreamLength(record model.ResultRecord) int {
	encoded, err := model.MarshalResultRecord(record)
	if err != nil {
		panic(err)
	}
	return len(encoded)
}

func clonePage(page ResultPageResponse) ResultPageResponse {
	clone := page
	clone.Records = append([]model.ResultRecord(nil), page.Records...)
	for index := range clone.Records {
		clone.Records[index].Value = append([]byte(nil), page.Records[index].Value...)
	}
	return clone
}

func invalidReorderedPage(page ResultPageResponse) ResultPageResponse {
	invalid := clonePage(page)
	invalid.Records[0], invalid.Records[1] = invalid.Records[1], invalid.Records[0]
	return invalid
}

func independentSubmitDigest(t *testing.T, request model.ClientRequestID, topology model.TopologySpec) [32]byte {
	t.Helper()
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		t.Fatal(err)
	}
	return independentSubmitDigestValue(request, validated.CanonicalBytes())
}

func independentSubmitDigestValue(request model.ClientRequestID, topology []byte) [32]byte {
	encoded := []byte("crane/submit-control-command/v1\x00")
	encoded = append(encoded, request.ClientID[:]...)
	encoded = appendU64Golden(encoded, request.Sequence)
	encoded = append(encoded, topology...)
	return sha256.Sum256(encoded)
}

func independentCancelDigest(request model.ClientRequestID, job model.JobID, revision uint64) [32]byte {
	encoded := []byte("crane/cancel-control-command/v1\x00")
	encoded = append(encoded, request.ClientID[:]...)
	encoded = appendU64Golden(encoded, request.Sequence)
	encoded = append(encoded, job[:]...)
	encoded = appendU64Golden(encoded, revision)
	return sha256.Sum256(encoded)
}

func goldenPrefix(message wire.MessageType) []byte {
	return []byte{0, 1, byte(uint16(message) >> 8), byte(message)}
}

func appendU16Golden(destination []byte, value uint16) []byte {
	return append(destination, byte(value>>8), byte(value))
}

func appendU32Golden(destination []byte, value uint32) []byte {
	return append(destination, byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func appendU64Golden(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendBoolGolden(destination []byte, value bool) []byte {
	if value {
		return append(destination, 1)
	}
	return append(destination, 0)
}

func appendRequestGolden(destination []byte, request model.ClientRequestID) []byte {
	destination = append(destination, request.ClientID[:]...)
	return appendU64Golden(destination, request.Sequence)
}

func appendTaskGolden(destination []byte, task model.TaskID) []byte {
	destination = append(destination, task.JobID[:]...)
	destination = appendU16Golden(destination, task.StageID)
	return appendU16Golden(destination, task.Partition)
}

func appendTupleGolden(destination []byte, tuple model.TupleID) []byte {
	destination = append(destination, tuple.JobID[:]...)
	destination = appendTaskGolden(destination, tuple.SourceTask)
	destination = appendU64Golden(destination, tuple.SourceSequence)
	return append(destination, tuple.PathDigest[:]...)
}

func appendPageBindingGolden(destination []byte, page ResultPageRequest) []byte {
	destination = append(destination, page.JobID[:]...)
	destination = append(destination, page.ManifestDigest[:]...)
	destination = appendBoolGolden(destination, page.HasLastTuple)
	destination = appendTupleGolden(destination, page.Last)
	return appendU32Golden(destination, page.PageBytes)
}

func appendBytes16Golden(destination, value []byte) []byte {
	destination = appendU16Golden(destination, uint16(len(value)))
	return append(destination, value...)
}

func goldenSubmitRequest(f controlMessageFixture) []byte {
	encoded := appendRequestGolden(goldenPrefix(wire.MessageCraneSubmitRequest), f.request)
	validated, _ := model.ValidateTopology(f.topology)
	encoded = append(encoded, validated.CanonicalBytes()...)
	return append(encoded, f.submitDigest[:]...)
}

func goldenSubmitResponse(f controlMessageFixture) []byte {
	m := f.submitResponse
	encoded := appendRequestGolden(goldenPrefix(m.MessageType()), m.Request)
	encoded = append(encoded, m.Digest[:]...)
	encoded = append(encoded, m.JobID[:]...)
	encoded = appendU64Golden(encoded, m.JobControlRevision)
	return append(encoded, byte(m.State))
}

func goldenCancelRequest(f controlMessageFixture) []byte {
	m := f.cancelRequest
	encoded := appendRequestGolden(goldenPrefix(m.MessageType()), m.Request)
	encoded = append(encoded, m.JobID[:]...)
	encoded = appendU64Golden(encoded, m.ExpectedJobControlRevision)
	return append(encoded, m.Digest[:]...)
}

func goldenCancelResponse(f controlMessageFixture) []byte {
	m := f.cancelResponse
	encoded := appendRequestGolden(goldenPrefix(m.MessageType()), m.Request)
	encoded = append(encoded, m.Digest[:]...)
	encoded = append(encoded, m.JobID[:]...)
	encoded = appendU64Golden(encoded, m.JobControlRevision)
	return append(encoded, byte(m.State))
}

func goldenStatusRequest(f controlMessageFixture) []byte {
	return append(goldenPrefix(f.statusRequest.MessageType()), f.job[:]...)
}

func goldenStatusResponse(f controlMessageFixture) []byte {
	m := f.statusResponse
	encoded := append(goldenPrefix(m.MessageType()), m.JobID[:]...)
	encoded = appendU64Golden(encoded, m.AppliedIndex)
	encoded = append(encoded, m.TopologyDigest[:]...)
	encoded = appendU64Golden(encoded, m.JobControlRevision)
	encoded = append(encoded, byte(m.State))
	encoded = appendBoolGolden(encoded, m.HasAssignment)
	encoded = appendU64Golden(encoded, m.AssignmentRevision)
	encoded = append(encoded, m.AssignmentDigest[:]...)
	encoded = appendU16Golden(encoded, m.SourceTaskCount)
	encoded = appendU16Golden(encoded, m.ResultPartitionCount)
	encoded = appendU16Golden(encoded, m.CompletedSourceTasks)
	encoded = appendU16Golden(encoded, m.ManifestCount)
	encoded = appendBoolGolden(encoded, m.HasManifestSet)
	encoded = append(encoded, m.ManifestSetDigest[:]...)
	encoded = appendBoolGolden(encoded, m.HasFailure)
	encoded = appendU16Golden(encoded, uint16(m.FailureCode))
	return append(encoded, m.FailureDetailDigest[:]...)
}

func goldenResultPageRequest(f controlMessageFixture) []byte {
	return appendPageBindingGolden(goldenPrefix(f.pageRequest.MessageType()), f.pageRequest)
}

func goldenResultPageResponse(f controlMessageFixture) []byte {
	m := f.pageResponse
	encoded := append(goldenPrefix(m.MessageType()), m.JobID[:]...)
	encoded = append(encoded, m.ManifestDigest[:]...)
	encoded = appendBoolGolden(encoded, m.RequestHasLastTuple)
	encoded = appendTupleGolden(encoded, m.RequestLast)
	encoded = appendU32Golden(encoded, m.PageBytes)
	encoded = appendU16Golden(encoded, uint16(len(m.Records)))
	for _, record := range m.Records {
		stream, _ := model.MarshalResultRecord(record)
		encoded = appendU32Golden(encoded, uint32(len(stream)))
		encoded = append(encoded, stream...)
	}
	encoded = appendBoolGolden(encoded, m.NextHasLastTuple)
	encoded = appendTupleGolden(encoded, m.NextLast)
	return appendBoolGolden(encoded, m.End)
}

func goldenLeaderRedirect(f controlMessageFixture) []byte {
	encoded := appendU16Golden(goldenPrefix(f.redirect.MessageType()), uint16(len(f.redirect.Endpoints)))
	for _, endpoint := range f.redirect.Endpoints {
		encoded = appendBytes16Golden(encoded, []byte(endpoint))
	}
	return encoded
}

func goldenControlError(f controlMessageFixture) []byte {
	m := f.controlError
	encoded := appendU16Golden(goldenPrefix(m.MessageType()), uint16(m.RelatedMessage))
	encoded = appendU16Golden(encoded, uint16(m.Code))
	encoded = appendBoolGolden(encoded, m.Retryable)
	encoded = appendBoolGolden(encoded, m.HasClientRequest)
	encoded = appendRequestGolden(encoded, m.ClientRequest)
	encoded = append(encoded, m.ClientDigest[:]...)
	encoded = appendBoolGolden(encoded, m.HasStatusRequest)
	encoded = append(encoded, m.StatusJobID[:]...)
	encoded = appendBoolGolden(encoded, m.HasResultPage)
	encoded = appendPageBindingGolden(encoded, m.ResultPage)
	encoded = appendU32Golden(encoded, m.RequiredBytes)
	return appendBytes16Golden(encoded, m.Detail)
}
