package protocol

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/wire"
)

func TestControlNestedLayoutsEnumsAndPrefixesComeFromActualCodec(t *testing.T) {
	contract := model.PublicControlContractV1()
	if got := ControlNestedEncodingLayouts(); !reflect.DeepEqual(got, contract.NestedLayouts) {
		t.Fatalf("actual nested layouts\n got: %#v\nwant: %#v", got, contract.NestedLayouts)
	}
	if got := ControlEnumDomains(); !reflect.DeepEqual(got, contract.EnumDomains) {
		t.Fatalf("actual enum domains\n got: %#v\nwant: %#v", got, contract.EnumDomains)
	}
	if got := ControlErrorCodeMatrix(); !reflect.DeepEqual(got, contract.ErrorCodeMatrix) {
		t.Fatalf("actual error-code matrix\n got: %#v\nwant: %#v", got, contract.ErrorCodeMatrix)
	}
	for _, message := range controlFixture(t).messages() {
		encoded, err := MarshalControlMessage(message)
		if err != nil {
			t.Fatal(err)
		}
		if gotSchema, gotType := uint16(encoded[0])<<8|uint16(encoded[1]), uint16(encoded[2])<<8|uint16(encoded[3]); gotSchema != contract.SchemaVersion || gotType != uint16(message.MessageType()) {
			t.Fatalf("actual prefix = %d/%d, contract = %d/%d", gotSchema, gotType, contract.SchemaVersion, message.MessageType())
		}
	}
}

func TestMutationResponseRevisionCorrelationAndCancelOverflow(t *testing.T) {
	fixture := controlFixture(t)
	submit := fixture.submitResponse
	submit.JobControlRevision = 2
	if err := ValidateSubmitResponseCorrelation(fixture.submitRequest, submit); err == nil {
		t.Fatal("submit correlation accepted revision other than one")
	}
	cancel := fixture.cancelResponse
	cancel.JobControlRevision = fixture.cancelRequest.ExpectedJobControlRevision + 2
	if err := ValidateCancelResponseCorrelation(fixture.cancelRequest, cancel); err == nil {
		t.Fatal("cancel correlation accepted revision other than expected plus one")
	}
	if _, err := CancelCommandDigest(fixture.request, fixture.job, math.MaxUint64); err == nil {
		t.Fatal("cancel digest accepted revision whose successor overflows")
	}
}

func TestExactControlErrorCorrelationRejectsDifferentRequests(t *testing.T) {
	fixture := controlFixture(t)
	statusError := ControlError{RelatedMessage: wire.MessageCraneStatusRequest, Code: ControlErrorNotFound, HasStatusRequest: true, StatusJobID: fixture.job}
	pageError := ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, Code: ControlErrorResultUnavailable, HasResultPage: true, ResultPage: fixture.pageRequest}
	submitError := ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, Code: ControlErrorCapacityExhausted, HasClientRequest: true, ClientRequest: fixture.request, ClientDigest: fixture.submitDigest}
	cancelError := ControlError{RelatedMessage: wire.MessageCraneCancelRequest, Code: ControlErrorRevisionMismatch, HasClientRequest: true, ClientRequest: fixture.request, ClientDigest: fixture.cancelDigest}
	jobListError := ControlError{RelatedMessage: wire.MessageCraneJobListRequest, Code: ControlErrorNotLeader, Retryable: true}
	checks := []struct {
		name      string
		valid     func() error
		different func() error
	}{
		{"submit", func() error { return ValidateSubmitErrorCorrelation(fixture.submitRequest, submitError) }, func() error {
			changed := fixture.submitRequest
			changed.Request.Sequence++
			changed.Digest, _ = SubmitCommandDigest(changed.Request, changed.Topology)
			return ValidateSubmitErrorCorrelation(changed, submitError)
		}},
		{"cancel", func() error { return ValidateCancelErrorCorrelation(fixture.cancelRequest, cancelError) }, func() error {
			changed := fixture.cancelRequest
			changed.ExpectedJobControlRevision--
			changed.Digest, _ = CancelCommandDigest(changed.Request, changed.JobID, changed.ExpectedJobControlRevision)
			return ValidateCancelErrorCorrelation(changed, cancelError)
		}},
		{"status", func() error { return ValidateStatusErrorCorrelation(fixture.statusRequest, statusError) }, func() error {
			changed := fixture.statusRequest
			changed.JobID[0]++
			return ValidateStatusErrorCorrelation(changed, statusError)
		}},
		{"page", func() error { return ValidateResultPageErrorCorrelation(fixture.pageRequest, pageError) }, func() error {
			changed := fixture.pageRequest
			changed.PageBytes++
			return ValidateResultPageErrorCorrelation(changed, pageError)
		}},
		{"job list", func() error { return ValidateJobListErrorCorrelation(fixture.jobListRequest, jobListError) }, func() error {
			incompatible := jobListError
			incompatible.Code = ControlErrorNotFound
			bound := jobListError
			bound.HasStatusRequest = true
			bound.StatusJobID = fixture.job
			foreign := jobListError
			foreign.RelatedMessage = wire.MessageCraneStatusRequest
			for _, hostile := range []ControlError{incompatible, bound, foreign} {
				if err := ValidateJobListErrorCorrelation(fixture.jobListRequest, hostile); err == nil {
					return nil
				}
			}
			return errors.New("rejected every job-list binding other than the stateless one")
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.valid(); err != nil {
				t.Fatalf("valid correlation: %v", err)
			}
			if err := check.different(); err == nil {
				t.Fatal("accepted well-formed error for a different request")
			}
		})
	}
}

func TestControlErrorRequestCodeCompatibilityMatrix(t *testing.T) {
	fixture := controlFixture(t)
	tests := []struct {
		name    string
		base    ControlError
		allowed map[ControlErrorCode]bool
	}{
		{"submit", ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, HasClientRequest: true, ClientRequest: fixture.request, ClientDigest: fixture.submitDigest}, codeSet(ControlErrorStarting, ControlErrorNotLeader, ControlErrorStaleRequest, ControlErrorSkippedRequest, ControlErrorIdentityReuse, ControlErrorCapacityExhausted, ControlErrorResultTooLarge)},
		{"cancel", ControlError{RelatedMessage: wire.MessageCraneCancelRequest, HasClientRequest: true, ClientRequest: fixture.request, ClientDigest: fixture.cancelDigest}, codeSet(ControlErrorStarting, ControlErrorNotLeader, ControlErrorStaleRequest, ControlErrorSkippedRequest, ControlErrorIdentityReuse, ControlErrorNotFound, ControlErrorRevisionMismatch, ControlErrorResultTooLarge)},
		{"status", ControlError{RelatedMessage: wire.MessageCraneStatusRequest, HasStatusRequest: true, StatusJobID: fixture.job}, codeSet(ControlErrorStarting, ControlErrorNotLeader, ControlErrorNotFound)},
		{"page", ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, HasResultPage: true, ResultPage: fixture.pageRequest}, codeSet(ControlErrorStarting, ControlErrorNotLeader, ControlErrorNotFound, ControlErrorPageLimitTooSmall, ControlErrorResultUnavailable, ControlErrorCorruptResult)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for code := ControlErrorMalformed; code <= ControlErrorResultTooLarge; code++ {
				value := test.base
				value.Code = code
				if code == ControlErrorPageLimitTooSmall {
					value.ResultPage.PageBytes = 1
					value.RequiredBytes = uint32(MinEncodedResultRecordBytes)
				}
				_, err := MarshalControlError(value)
				if (err == nil) != test.allowed[code] {
					t.Fatalf("code %d allowed=%v error=%v", code, test.allowed[code], err)
				}
			}
		})
	}
	for _, message := range []wire.MessageType{wire.MessageCraneSubmitRequest, wire.MessageCraneCancelRequest, wire.MessageCraneStatusRequest, wire.MessageCraneResultPageRequest} {
		for code := ControlErrorMalformed; code <= ControlErrorResultTooLarge; code++ {
			_, err := MarshalControlError(ControlError{RelatedMessage: message, Code: code})
			want := code == ControlErrorMalformed || code == ControlErrorUnsupportedSchema || code == ControlErrorInvalidRequest
			if (err == nil) != want {
				t.Fatalf("unbound message=%d code=%d allowed=%v error=%v", message, code, want, err)
			}
		}
	}
	for code := ControlErrorMalformed; code <= ControlErrorResultTooLarge; code++ {
		_, err := MarshalControlError(ControlError{RelatedMessage: wire.MessageCraneJobListRequest, Code: code})
		want := code == ControlErrorStarting || code == ControlErrorNotLeader
		if (err == nil) != want {
			t.Fatalf("unbound job-list code=%d allowed=%v error=%v", code, want, err)
		}
	}
}

func codeSet(codes ...ControlErrorCode) map[ControlErrorCode]bool {
	result := make(map[ControlErrorCode]bool, len(codes))
	for _, code := range codes {
		result[code] = true
	}
	return result
}

func TestStatusLifecycleMatrixAndResultPartitionCount(t *testing.T) {
	fixture := controlFixture(t)
	base := fixture.statusResponse
	base.ResultPartitionCount = 2
	base.ManifestCount = 0
	base.HasManifestSet = false
	base.ManifestSetDigest = [32]byte{}
	legal := map[string]StatusResponse{
		"pending": func() StatusResponse {
			v := base
			v.State = JobPending
			v.HasAssignment = false
			v.AssignmentRevision = 0
			v.AssignmentDigest = [32]byte{}
			v.CompletedSourceTasks = 0
			return v
		}(),
		"deploying": func() StatusResponse { v := base; v.State = JobDeploying; v.CompletedSourceTasks = 0; return v }(),
		"running":   func() StatusResponse { v := base; v.State = JobRunning; v.CompletedSourceTasks = 0; return v }(),
		"draining partial": func() StatusResponse {
			v := base
			v.State = JobDraining
			v.CompletedSourceTasks = v.SourceTaskCount
			v.ManifestCount = 1
			v.HasManifestSet = true
			v.ManifestSetDigest = fixture.manifestDigest
			return v
		}(),
		"succeeded": func() StatusResponse {
			v := base
			v.State = JobSucceeded
			v.CompletedSourceTasks = v.SourceTaskCount
			v.ManifestCount = v.ResultPartitionCount
			v.HasManifestSet = true
			v.ManifestSetDigest = fixture.manifestDigest
			return v
		}(),
		"failed early": func() StatusResponse {
			v := base
			v.State = JobFailed
			v.HasAssignment = false
			v.AssignmentRevision = 0
			v.AssignmentDigest = [32]byte{}
			v.CompletedSourceTasks = 0
			v.HasFailure = true
			v.FailureCode = model.FailureStorage
			v.FailureDetailDigest = [32]byte{1}
			return v
		}(),
		"canceled retained": func() StatusResponse { v := base; v.State = JobCanceled; return v }(),
	}
	for name, value := range legal {
		t.Run("legal "+name, func(t *testing.T) {
			if _, err := MarshalStatusResponse(value); err != nil {
				t.Fatal(err)
			}
		})
	}
	illegal := map[string]StatusResponse{
		"zero partitions":         func() StatusResponse { v := legal["running"]; v.ResultPartitionCount = 0; return v }(),
		"manifest above expected": func() StatusResponse { v := legal["draining partial"]; v.ManifestCount = 3; return v }(),
		"pending assignment": func() StatusResponse {
			v := legal["pending"]
			v.HasAssignment = true
			v.AssignmentRevision = 1
			v.AssignmentDigest = [32]byte{1}
			return v
		}(),
		"deploying progress": func() StatusResponse { v := legal["deploying"]; v.CompletedSourceTasks = 1; return v }(),
		"running manifests": func() StatusResponse {
			v := legal["running"]
			v.ManifestCount = 1
			v.HasManifestSet = true
			v.ManifestSetDigest = fixture.manifestDigest
			return v
		}(),
		"draining incomplete": func() StatusResponse { v := legal["draining partial"]; v.CompletedSourceTasks = 0; return v }(),
		"succeeded partial":   func() StatusResponse { v := legal["succeeded"]; v.ManifestCount--; return v }(),
		"failed without failure": func() StatusResponse {
			v := legal["failed early"]
			v.HasFailure = false
			v.FailureCode = 0
			v.FailureDetailDigest = [32]byte{}
			return v
		}(),
		"canceled failure": func() StatusResponse {
			v := legal["canceled retained"]
			v.HasFailure = true
			v.FailureCode = model.FailureStorage
			v.FailureDetailDigest = [32]byte{1}
			return v
		}(),
	}
	for name, value := range illegal {
		t.Run("illegal "+name, func(t *testing.T) {
			if _, err := MarshalStatusResponse(value); err == nil {
				t.Fatal("accepted illegal lifecycle")
			}
		})
	}
}

func TestResultEntryAndPageLimitTooSmallExactBoundaries(t *testing.T) {
	for _, test := range []struct {
		bytes uint64
		valid bool
	}{{1, false}, {2, true}, {512, true}, {513, false}} {
		_, err := model.PublicControlResultRecordEntryBytesV1(test.bytes)
		if (err == nil) != test.valid {
			t.Fatalf("tuple bytes %d valid=%v err=%v", test.bytes, test.valid, err)
		}
	}
	fixture := controlFixture(t)
	for _, test := range []struct {
		required uint32
		valid    bool
	}{{169, false}, {170, true}, {680, true}, {681, false}} {
		request := fixture.pageRequest
		request.PageBytes = 1
		value := ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, Code: ControlErrorPageLimitTooSmall, HasResultPage: true, ResultPage: request, RequiredBytes: test.required}
		_, err := MarshalControlError(value)
		if (err == nil) != test.valid {
			t.Fatalf("required %d valid=%v err=%v", test.required, test.valid, err)
		}
		if test.valid && ValidateResultPageErrorCorrelation(request, value) != nil {
			t.Fatalf("required %d did not correlate", test.required)
		}
	}
}

func TestEveryFingerprintedPublicControlRuleHasBlackBoxCoverage(t *testing.T) {
	fixture := controlFixture(t)
	reject := func(message ControlMessage) func(*testing.T) {
		return func(t *testing.T) {
			if _, err := MarshalControlMessage(message); err == nil {
				t.Fatal("fingerprinted rule violation was accepted")
			}
		}
	}
	changedSubmit := fixture.submitRequest
	changedSubmit.Digest[0]++
	changedCancel := fixture.cancelRequest
	changedCancel.Digest[0]++
	badStatus := fixture.statusResponse
	badStatus.AppliedIndex = 0
	badLifecycle := fixture.statusResponse
	badLifecycle.State = JobRunning
	badPageBinding := clonePage(fixture.pageResponse)
	badPageBinding.ManifestDigest[0]++
	badEntry := clonePage(fixture.pageResponse)
	badEntry.PageBytes--
	badOrder := invalidReorderedPage(fixture.pageResponse)
	badNext := clonePage(fixture.pageResponse)
	badNext.NextLast = badNext.Records[0].TupleID
	badEmpty := ResultPageResponse{JobID: fixture.job, ManifestDigest: fixture.manifestDigest, PageBytes: 1}
	badRedirect := LeaderRedirect{Endpoints: []string{"node-b.test:1", "node-a.test:1", "node-c.test:1"}}
	badSelectors := fixture.controlError
	badSelectors.HasClientRequest = true
	badSelectors.ClientRequest = fixture.request
	badSelectors.ClientDigest = fixture.submitDigest
	badMutationError := ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, Code: ControlErrorCapacityExhausted, HasClientRequest: true, ClientRequest: fixture.request, ClientDigest: fixture.cancelDigest}
	badMatrixError := ControlError{RelatedMessage: wire.MessageCraneStatusRequest, Code: ControlErrorIdentityReuse, HasStatusRequest: true, StatusJobID: fixture.job}
	badJobListError := ControlError{RelatedMessage: wire.MessageCraneJobListRequest, Code: ControlErrorNotLeader, HasStatusRequest: true, StatusJobID: fixture.job}
	unboundStarting := ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, Code: ControlErrorStarting}
	unknownState := fixture.statusResponse
	unknownState.State = 99

	coverage := map[string]func(*testing.T){
		"payload-prefix-is-schema-version-then-message-type": func(t *testing.T) {
			encoded, _ := MarshalSubmitRequest(fixture.submitRequest)
			if len(encoded) < 4 || encoded[1] != byte(ControlSchemaVersion) || encoded[3] != byte(wire.MessageCraneSubmitRequest) {
				t.Fatal("wrong prefix")
			}
		},
		"submit-digest-binds-client-request-and-canonical-topology":    reject(changedSubmit),
		"cancel-digest-binds-client-request-job-and-expected-revision": reject(changedCancel),
		"mutation-response-echoes-request-digest-job-revision-and-terminal-command-state": func(t *testing.T) {
			changed := fixture.cancelResponse
			changed.Digest[0]++
			if ValidateCancelResponseCorrelation(fixture.cancelRequest, changed) == nil {
				t.Fatal("accepted foreign mutation response")
			}
		},
		"submit-response-revision-is-one-and-cancel-response-revision-is-checked-successor": func(t *testing.T) {
			changed := fixture.submitResponse
			changed.JobControlRevision++
			if ValidateSubmitResponseCorrelation(fixture.submitRequest, changed) == nil {
				t.Fatal("accepted submit revision")
			}
		},
		"status-binds-applied-index-topology-assignment-manifest-and-failure-identities": reject(badStatus),
		"status-lifecycle-binds-immutable-result-partition-count-and-progress-matrix":    reject(badLifecycle),
		"result-page-cursor-is-global-stateless-and-bound-to-job-manifest-and-page-limit": func(t *testing.T) {
			if ValidateResultPageResponseCorrelation(fixture.pageRequest, badPageBinding) == nil {
				t.Fatal("accepted foreign page binding")
			}
		},
		"result-record-entries-are-complete-canonical-streams-and-never-split":               reject(badEntry),
		"result-records-are-strictly-globally-increasing-and-cross-job-records-are-rejected": reject(badOrder),
		"nonempty-page-next-cursor-equals-last-record":                                       reject(badNext),
		"empty-terminal-page-preserves-request-cursor":                                       reject(badEmpty),
		"page-limit-too-small-repeats-request-binding-required-bytes-and-does-not-advance": func(t *testing.T) {
			changed := fixture.controlError
			changed.ResultPage.JobID[0]++
			if ValidateResultPageErrorCorrelation(fixture.controlError.ResultPage, changed) == nil {
				t.Fatal("accepted foreign page error")
			}
		},
		"page-limit-required-bytes-is-one-complete-encoded-result-entry-from-170-through-680": func(t *testing.T) {
			changed := fixture.controlError
			changed.RequiredBytes = 169
			if _, err := MarshalControlError(changed); err == nil {
				t.Fatal("accepted undersize requirement")
			}
		},
		"result-page-serving-sizes-records-with-encoded-result-page-record-bytes": func(t *testing.T) {
			entry, err := EncodedResultPageRecordBytes(fixture.pageResponse.Records[0])
			if err != nil || len(entry) != int(fixture.controlError.RequiredBytes) {
				t.Fatal("serving size helper drift")
			}
		},
		"redirect-endpoints-are-exactly-one-or-three-or-five-canonical-sorted-unique-control-endpoints": reject(badRedirect),
		"control-error-binding-selectors-are-exclusive-and-match-related-request-type":                  reject(badSelectors),
		"mutation-control-errors-echo-client-request-and-candidate-command-digest": func(t *testing.T) {
			if ValidateSubmitErrorCorrelation(fixture.submitRequest, badMutationError) == nil {
				t.Fatal("accepted wrong candidate digest")
			}
		},
		"control-error-request-type-and-code-follow-the-fingerprinted-compatibility-matrix":                  reject(badMatrixError),
		"unbound-control-errors-are-limited-to-malformed-unsupported-schema-or-invalid-request-before-trust": reject(unboundStarting),
		"job-list-errors-bind-only-the-related-request-without-a-selector":                                   reject(badJobListError),
		"decoded-variable-values-are-owned": func(t *testing.T) {
			encoded, _ := MarshalControlError(fixture.controlError)
			decoded, err := UnmarshalControlError(encoded)
			if err != nil {
				t.Fatal(err)
			}
			encoded[len(encoded)-1] ^= 1
			if reflect.DeepEqual(decoded.Detail, encoded[len(encoded)-len(decoded.Detail):]) {
				t.Fatal("decoded detail aliases input")
			}
		},
		"bounds-and-declared-lengths-are-checked-before-allocation": func(t *testing.T) {
			if _, err := UnmarshalControlMessage(wire.MessageCraneStatusRequest, make([]byte, MaxControlPayloadBytes+1)); err == nil {
				t.Fatal("accepted oversized payload")
			}
		},
		"unknown-enums-trailing-bytes-type-mismatches-and-noncanonical-bytes-are-rejected": func(t *testing.T) {
			if _, err := MarshalStatusResponse(unknownState); err == nil {
				t.Fatal("accepted unknown state")
			}
			encoded, _ := MarshalStatusRequest(fixture.statusRequest)
			if _, err := UnmarshalStatusRequest(append(encoded, 0)); err == nil {
				t.Fatal("accepted trailing bytes")
			}
		},
	}
	contract := model.PublicControlContractV1()
	if len(coverage) != len(contract.Rules) {
		t.Fatalf("behavior coverage count = %d, fingerprinted rules = %d", len(coverage), len(contract.Rules))
	}
	for _, rule := range contract.Rules {
		check, ok := coverage[rule]
		if !ok {
			t.Fatalf("fingerprinted rule lacks explicit black-box test: %s", rule)
		}
		t.Run(rule, check)
	}
}
