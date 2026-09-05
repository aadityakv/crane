package model

import (
	"crypto/sha256"
	"reflect"
	"testing"
)

func TestPublicControlContractFingerprintSensitiveToEveryCategory(t *testing.T) {
	baseline := PublicControlContractV1()
	want := sha256.Sum256(canonicalPublicControlContractBytes(baseline))
	tests := map[string]func(*PublicControlContract){
		"message": func(v *PublicControlContract) { v.Messages[0].Fields[0].Name += "x" },
		"nested":  func(v *PublicControlContract) { v.NestedLayouts[0].Fields[0].Encoding += "x" },
		"enum":    func(v *PublicControlContract) { v.EnumDomains[0].Values[0] += "x" },
		"bound":   func(v *PublicControlContract) { v.MaxResultPageBytes++ },
		"domain":  func(v *PublicControlContract) { v.IdentityDomains[0] += "x" },
		"matrix":  func(v *PublicControlContract) { v.ErrorCodeMatrix[0] += "x" },
		"rule":    func(v *PublicControlContract) { v.Rules[0] += "x" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := PublicControlContractV1()
			mutate(&changed)
			if got := sha256.Sum256(canonicalPublicControlContractBytes(changed)); got == want {
				t.Fatal("contract mutation did not change canonical hash")
			}
		})
	}
}

func TestPublicControlResultEntryBoundsAreMechanicallyDerived(t *testing.T) {
	emptyTuple, err := MarshalTuple(Tuple{})
	if err != nil {
		t.Fatal(err)
	}
	if len(emptyTuple) != 2 {
		t.Fatalf("canonical empty tuple bytes = %d, want 2", len(emptyTuple))
	}
	minimum, err := PublicControlResultRecordEntryBytesV1(uint64(len(emptyTuple)))
	if err != nil || minimum != 170 {
		t.Fatalf("minimum result entry bytes = %d, %v; want 170", minimum, err)
	}
	if minimum != PublicControlContractV1().MinEncodedResultRecordBytes {
		t.Fatalf("derived minimum = %d, fingerprinted contract = %d", minimum, PublicControlContractV1().MinEncodedResultRecordBytes)
	}
	maximum, err := PublicControlResultRecordEntryBytesV1(LimitsV1().MaxTuplePayloadBytes)
	if err != nil || maximum != PublicControlMaxEncodedResultRecordBytesV1 {
		t.Fatalf("maximum result entry bytes = %d, %v; contract = %d", maximum, err, PublicControlMaxEncodedResultRecordBytesV1)
	}
	if got := PublicControlMaxResultPageBytesV1 / minimum; got != PublicControlMaxResultPageRecordsV1 {
		t.Fatalf("minimum entries fitting page = %d, contract = %d", got, PublicControlMaxResultPageRecordsV1)
	}
	if PublicControlMaxResultPageRecordsV1*minimum > PublicControlMaxResultPageBytesV1 {
		t.Fatal("3,084 minimum entries do not fit the page budget")
	}
	if (PublicControlMaxResultPageRecordsV1+1)*minimum <= PublicControlMaxResultPageBytesV1 {
		t.Fatal("3,085 minimum entries unexpectedly fit the page budget")
	}
}

func TestPublicControlContractV1PinsOwnedLayoutsBoundsEnumsDomainsAndRules(t *testing.T) {
	want := PublicControlContract{
		SchemaVersion:               1,
		MessageTypeMin:              240,
		MessageTypeMax:              251,
		MaxControlFrameBytes:        1 << 20,
		MaxResultPageBytes:          512 << 10,
		MaxResultPageRecords:        3084,
		MinEncodedResultRecordBytes: 170,
		MaxEncodedResultRecordBytes: 680,
		MaxRedirectEndpoints:        5,
		MaxEndpointBytes:            259,
		MaxErrorDetailBytes:         256,
		Messages: []PublicControlMessageDescriptor{
			{Name: "SubmitRequest", MessageType: 240, SchemaVersion: 1, Fields: fields("Request:ClientRequestID", "Topology:TopologySpec", "Digest:sha256")},
			{Name: "SubmitResponse", MessageType: 241, SchemaVersion: 1, Fields: fields("Request:ClientRequestID", "Digest:sha256", "JobID:JobID", "JobControlRevision:u64", "State:u8")},
			{Name: "CancelRequest", MessageType: 242, SchemaVersion: 1, Fields: fields("Request:ClientRequestID", "JobID:JobID", "ExpectedJobControlRevision:u64", "Digest:sha256")},
			{Name: "CancelResponse", MessageType: 243, SchemaVersion: 1, Fields: fields("Request:ClientRequestID", "Digest:sha256", "JobID:JobID", "JobControlRevision:u64", "State:u8")},
			{Name: "StatusRequest", MessageType: 244, SchemaVersion: 1, Fields: fields("JobID:JobID")},
			{Name: "StatusResponse", MessageType: 245, SchemaVersion: 1, Fields: fields("JobID:JobID", "AppliedIndex:u64", "TopologyDigest:sha256", "JobControlRevision:u64", "State:u8", "HasAssignment:bool", "AssignmentRevision:u64", "AssignmentDigest:sha256", "SourceTaskCount:u16", "ResultPartitionCount:u16", "CompletedSourceTasks:u16", "ManifestCount:u16", "HasManifestSet:bool", "ManifestSetDigest:sha256", "HasFailure:bool", "FailureCode:u16", "FailureDetailDigest:sha256")},
			{Name: "ResultPageRequest", MessageType: 246, SchemaVersion: 1, Fields: fields("JobID:JobID", "ManifestDigest:sha256", "HasLastTuple:bool", "Last:TupleID", "PageBytes:u32")},
			{Name: "ResultPageResponse", MessageType: 247, SchemaVersion: 1, Fields: fields("JobID:JobID", "ManifestDigest:sha256", "RequestHasLastTuple:bool", "RequestLast:TupleID", "PageBytes:u32", "Records:list(ResultRecordEntry)", "NextHasLastTuple:bool", "NextLast:TupleID", "End:bool")},
			{Name: "LeaderRedirect", MessageType: 248, SchemaVersion: 1, Fields: fields("Endpoints:list(string16)")},
			{Name: "ControlError", MessageType: 249, SchemaVersion: 1, Fields: fields("RelatedMessage:u16", "Code:u16", "Retryable:bool", "HasClientRequest:bool", "ClientRequest:ClientRequestID", "ClientDigest:sha256", "HasStatusRequest:bool", "StatusJobID:JobID", "HasResultPage:bool", "ResultPage:ResultPageBinding", "RequiredBytes:u32", "Detail:bytes16")},
			{Name: "JobListRequest", MessageType: 250, SchemaVersion: 1},
			{Name: "JobListResponse", MessageType: 251, SchemaVersion: 1, Fields: fields("LeaderNodeID:u16", "AppliedIndex:u64", "Jobs:list(StatusResponse)")},
		},
		NestedLayouts: []PublicControlNestedDescriptor{
			{Name: "ClientRequestID", Fields: fields("ClientID:bytes16(nonzero)", "Sequence:u64(nonzero)")},
			{Name: "JobID", Fields: fields("Value:bytes16(nonzero)")},
			{Name: "TaskID", Fields: fields("JobID:JobID", "StageID:u16(nonzero)", "Partition:u16")},
			{Name: "TupleID", Fields: fields("JobID:JobID", "SourceTask:TaskID", "SourceSequence:u64(nonzero)", "PathDigest:sha256(nonzero)")},
			{Name: "TopologySpec", Fields: fields("CanonicalTopology:bytes64")},
			{Name: "ResultRecordEntry", Fields: fields("Length:u32", "Record:canonical-result-record-v1")},
			{Name: "ResultPageBinding", Fields: fields("JobID:JobID", "ManifestDigest:sha256", "HasLastTuple:bool", "Last:TupleID", "PageBytes:u32")},
		},
		EnumDomains: []PublicControlEnumDescriptor{
			{Name: "JobState", Values: []string{"Pending=1", "Deploying=2", "Running=3", "Draining=4", "Succeeded=5", "Failed=6", "Canceled=7"}},
			{Name: "ControlErrorCode", Values: []string{"Malformed=1", "UnsupportedSchema=2", "InvalidRequest=3", "Starting=4", "NotLeader=5", "StaleRequest=6", "SkippedRequest=7", "IdentityReuse=8", "NotFound=9", "RevisionMismatch=10", "CapacityExhausted=11", "PageLimitTooSmall=12", "ResultUnavailable=13", "CorruptResult=14", "ResultTooLarge=15", "ResultsNoLongerRetained=16"}},
			{Name: "FailureCode", Values: []string{"Operator=1", "TupleInvalid=2", "Storage=3"}},
		},
		IdentityDomains: []string{
			"crane/submit-control-command/v1",
			"crane/cancel-control-command/v1",
		},
		ErrorCodeMatrix: []string{
			"Unbound=Malformed,UnsupportedSchema,InvalidRequest",
			"SubmitRequest=Starting,NotLeader,StaleRequest,SkippedRequest,IdentityReuse,CapacityExhausted,ResultTooLarge",
			"CancelRequest=Starting,NotLeader,StaleRequest,SkippedRequest,IdentityReuse,NotFound,RevisionMismatch,ResultTooLarge",
			"StatusRequest=Starting,NotLeader,NotFound",
			"ResultPageRequest=Starting,NotLeader,NotFound,PageLimitTooSmall,ResultUnavailable,CorruptResult,ResultsNoLongerRetained",
			"JobListRequest=Starting,NotLeader",
		},
		Rules: []string{
			"payload-prefix-is-schema-version-then-message-type",
			"submit-digest-binds-client-request-and-canonical-topology",
			"cancel-digest-binds-client-request-job-and-expected-revision",
			"mutation-response-echoes-request-digest-job-revision-and-terminal-command-state",
			"submit-response-revision-is-one-and-cancel-response-revision-is-checked-successor",
			"status-binds-applied-index-topology-assignment-manifest-and-failure-identities",
			"status-lifecycle-binds-immutable-result-partition-count-and-progress-matrix",
			"result-page-cursor-is-global-stateless-and-bound-to-job-manifest-and-page-limit",
			"result-record-entries-are-complete-canonical-streams-and-never-split",
			"result-records-are-strictly-globally-increasing-and-cross-job-records-are-rejected",
			"nonempty-page-next-cursor-equals-last-record",
			"empty-terminal-page-preserves-request-cursor",
			"page-limit-too-small-repeats-request-binding-required-bytes-and-does-not-advance",
			"page-limit-required-bytes-is-one-complete-encoded-result-entry-from-170-through-680",
			"result-page-serving-sizes-records-with-encoded-result-page-record-bytes",
			"redirect-endpoints-are-exactly-one-or-three-or-five-canonical-sorted-unique-control-endpoints",
			"control-error-binding-selectors-are-exclusive-and-match-related-request-type",
			"mutation-control-errors-echo-client-request-and-candidate-command-digest",
			"control-error-request-type-and-code-follow-the-fingerprinted-compatibility-matrix",
			"unbound-control-errors-are-limited-to-malformed-unsupported-schema-or-invalid-request-before-trust",
			"job-list-errors-bind-only-the-related-request-without-a-selector",
			"decoded-variable-values-are-owned",
			"bounds-and-declared-lengths-are-checked-before-allocation",
			"unknown-enums-trailing-bytes-type-mismatches-and-noncanonical-bytes-are-rejected",
		},
	}

	got := PublicControlContractV1()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PublicControlContractV1()\n got: %#v\nwant: %#v", got, want)
	}

	got.Messages[0].Name = "mutated"
	got.Messages[0].Fields[0].Name = "mutated"
	got.NestedLayouts[0].Fields[0].Encoding = "mutated"
	got.EnumDomains[0].Values[0] = "mutated"
	got.IdentityDomains[0] = "mutated"
	got.ErrorCodeMatrix[0] = "mutated"
	got.Rules[0] = "mutated"
	again := PublicControlContractV1()
	if again.Messages[0].Name != "SubmitRequest" || again.Messages[0].Fields[0].Name != "Request" || again.NestedLayouts[0].Fields[0].Encoding != "bytes16(nonzero)" || again.EnumDomains[0].Values[0] != "Pending=1" || again.IdentityDomains[0] != "crane/submit-control-command/v1" || again.ErrorCodeMatrix[0] != "Unbound=Malformed,UnsupportedSchema,InvalidRequest" || again.Rules[0] != "payload-prefix-is-schema-version-then-message-type" {
		t.Fatalf("PublicControlContractV1 shares mutable storage: %#v", again)
	}
}

func fields(values ...string) []PublicControlFieldDescriptor {
	result := make([]PublicControlFieldDescriptor, len(values))
	for index, value := range values {
		separator := -1
		for i := range value {
			if value[i] == ':' {
				separator = i
				break
			}
		}
		if separator < 1 {
			panic("invalid test field descriptor")
		}
		result[index] = PublicControlFieldDescriptor{Name: value[:separator], Encoding: value[separator+1:]}
	}
	return result
}
