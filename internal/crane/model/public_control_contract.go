package model

import (
	"crypto/sha256"
	"errors"
)

const publicControlContractFingerprintDomain = "crane/public-control-contract/v1\x00"

var errPublicControlResultEntrySize = errors.New("public control result entry size outside tuple bounds")

const (
	publicControlResultRecordStreamFixedBytesV1 = v1Uint16Bytes + v1TupleIDBytes + v1TaskIDBytes + v1DigestBytes + v1Uint16Bytes + v1DigestBytes
	publicControlResultRecordEntryPrefixBytesV1 = uint64(4)
	publicControlMinimumTupleBytesV1            = v1Uint16Bytes

	// PublicControlSchemaVersionV1 is the canonical +4 payload schema.
	PublicControlSchemaVersionV1 uint16 = 1
	// PublicControlMaxFrameBytesV1 bounds a complete authenticated +4 frame.
	PublicControlMaxFrameBytesV1 = 1 << 20
	// PublicControlMaxResultPageBytesV1 bounds complete encoded result entries in one page.
	PublicControlMaxResultPageBytesV1 = 512 << 10
	// PublicControlMaxResultPageRecordsV1 is the maximum number of minimum-size entries fitting one page.
	PublicControlMaxResultPageRecordsV1 = 3084
	// PublicControlMinEncodedResultRecordBytesV1 is one length prefix, fixed record stream, and empty tuple.
	PublicControlMinEncodedResultRecordBytesV1 = publicControlResultRecordEntryPrefixBytesV1 + publicControlResultRecordStreamFixedBytesV1 + publicControlMinimumTupleBytesV1
	// PublicControlMaxEncodedResultRecordBytesV1 bounds one length-prefixed result entry.
	PublicControlMaxEncodedResultRecordBytesV1 = 680
	// PublicControlMaxRedirectEndpointsV1 is the configured five-voter ceiling.
	PublicControlMaxRedirectEndpointsV1 = 5
	// PublicControlMaxEndpointBytesV1 bounds one canonical host:port spelling.
	PublicControlMaxEndpointBytesV1 = 259
	// PublicControlMaxErrorDetailBytesV1 bounds authentication-safe error detail.
	PublicControlMaxErrorDetailBytesV1 = 256
)

// PublicControlResultRecordEntryBytesV1 calculates one length-prefixed result
// entry from the canonical tuple byte length using checked arithmetic.
func PublicControlResultRecordEntryBytesV1(tupleBytes uint64) (uint64, error) {
	if tupleBytes < publicControlMinimumTupleBytesV1 || tupleBytes > LimitsV1().MaxTuplePayloadBytes {
		return 0, errPublicControlResultEntrySize
	}
	result, ok := checkedAddUint64(publicControlResultRecordEntryPrefixBytesV1, publicControlResultRecordStreamFixedBytesV1)
	if !ok {
		return 0, errPublicControlResultEntrySize
	}
	result, ok = checkedAddUint64(result, tupleBytes)
	if !ok {
		return 0, errPublicControlResultEntrySize
	}
	return result, nil
}

// PublicControlFieldDescriptor names one field emitted by the actual +4 encoder.
type PublicControlFieldDescriptor struct {
	// Name is the stable field name.
	Name string
	// Encoding is the canonical wire representation or nested layout name.
	Encoding string
}

// PublicControlMessageDescriptor identifies one owned +4 message schema.
type PublicControlMessageDescriptor struct {
	// Name is the stable message name.
	Name string
	// MessageType is the authenticated uint16 wire ID.
	MessageType uint16
	// SchemaVersion is the payload schema prefix.
	SchemaVersion uint16
	// Fields is the exact top-level encoder order.
	Fields []PublicControlFieldDescriptor
}

// PublicControlNestedDescriptor pins one nested canonical layout.
type PublicControlNestedDescriptor struct {
	// Name is the nested layout name referenced by message fields.
	Name string
	// Fields is the exact nested field order.
	Fields []PublicControlFieldDescriptor
}

// PublicControlEnumDescriptor pins one compatibility-sensitive enum domain.
type PublicControlEnumDescriptor struct {
	// Name is the enum type name.
	Name string
	// Values lists every accepted name/value pair in ascending value order.
	Values []string
}

// PublicControlContract contains every compatibility-sensitive +4 rule.
// PublicControlContractV1 returns deep-owned slices to callers.
type PublicControlContract struct {
	// SchemaVersion is the common v1 payload schema.
	SchemaVersion uint16
	// MessageTypeMin and MessageTypeMax delimit the contiguous owned +4 range.
	MessageTypeMin, MessageTypeMax uint16
	// Messages pins every exact top-level layout.
	Messages []PublicControlMessageDescriptor
	// NestedLayouts pins referenced identity and record layouts.
	NestedLayouts []PublicControlNestedDescriptor
	// EnumDomains pins all accepted public-control enum values.
	EnumDomains []PublicControlEnumDescriptor
	// MaxControlFrameBytes bounds complete authenticated frames.
	MaxControlFrameBytes uint64
	// MaxResultPageBytes bounds complete encoded result entries per page.
	MaxResultPageBytes uint64
	// MaxResultPageRecords bounds decoder allocation for page records.
	MaxResultPageRecords uint64
	// MinEncodedResultRecordBytes is the smallest complete length-prefixed canonical entry.
	MinEncodedResultRecordBytes uint64
	// MaxEncodedResultRecordBytes bounds one complete length-prefixed entry.
	MaxEncodedResultRecordBytes uint64
	// MaxRedirectEndpoints bounds leader/static redirect alternatives.
	MaxRedirectEndpoints uint64
	// MaxEndpointBytes bounds each canonical endpoint spelling.
	MaxEndpointBytes uint64
	// MaxErrorDetailBytes bounds typed error detail.
	MaxErrorDetailBytes uint64
	// IdentityDomains pins mutation digest separation.
	IdentityDomains []string
	// ErrorCodeMatrix pins the exact accepted code domain for each binding class.
	ErrorCodeMatrix []string
	// Rules pins cross-field canonical validation.
	Rules []string
}

var publicControlMessageDescriptorsV1 = []PublicControlMessageDescriptor{
	{Name: "SubmitRequest", MessageType: 240, SchemaVersion: 1, Fields: controlFields("Request", "ClientRequestID", "Topology", "TopologySpec", "Digest", "sha256")},
	{Name: "SubmitResponse", MessageType: 241, SchemaVersion: 1, Fields: controlFields("Request", "ClientRequestID", "Digest", "sha256", "JobID", "JobID", "JobControlRevision", "u64", "State", "u8")},
	{Name: "CancelRequest", MessageType: 242, SchemaVersion: 1, Fields: controlFields("Request", "ClientRequestID", "JobID", "JobID", "ExpectedJobControlRevision", "u64", "Digest", "sha256")},
	{Name: "CancelResponse", MessageType: 243, SchemaVersion: 1, Fields: controlFields("Request", "ClientRequestID", "Digest", "sha256", "JobID", "JobID", "JobControlRevision", "u64", "State", "u8")},
	{Name: "StatusRequest", MessageType: 244, SchemaVersion: 1, Fields: controlFields("JobID", "JobID")},
	{Name: "StatusResponse", MessageType: 245, SchemaVersion: 1, Fields: controlFields("JobID", "JobID", "AppliedIndex", "u64", "TopologyDigest", "sha256", "JobControlRevision", "u64", "State", "u8", "HasAssignment", "bool", "AssignmentRevision", "u64", "AssignmentDigest", "sha256", "SourceTaskCount", "u16", "ResultPartitionCount", "u16", "CompletedSourceTasks", "u16", "ManifestCount", "u16", "HasManifestSet", "bool", "ManifestSetDigest", "sha256", "HasFailure", "bool", "FailureCode", "u16", "FailureDetailDigest", "sha256")},
	{Name: "ResultPageRequest", MessageType: 246, SchemaVersion: 1, Fields: controlFields("JobID", "JobID", "ManifestDigest", "sha256", "HasLastTuple", "bool", "Last", "TupleID", "PageBytes", "u32")},
	{Name: "ResultPageResponse", MessageType: 247, SchemaVersion: 1, Fields: controlFields("JobID", "JobID", "ManifestDigest", "sha256", "RequestHasLastTuple", "bool", "RequestLast", "TupleID", "PageBytes", "u32", "Records", "list(ResultRecordEntry)", "NextHasLastTuple", "bool", "NextLast", "TupleID", "End", "bool")},
	{Name: "LeaderRedirect", MessageType: 248, SchemaVersion: 1, Fields: controlFields("Endpoints", "list(string16)")},
	{Name: "ControlError", MessageType: 249, SchemaVersion: 1, Fields: controlFields("RelatedMessage", "u16", "Code", "u16", "Retryable", "bool", "HasClientRequest", "bool", "ClientRequest", "ClientRequestID", "ClientDigest", "sha256", "HasStatusRequest", "bool", "StatusJobID", "JobID", "HasResultPage", "bool", "ResultPage", "ResultPageBinding", "RequiredBytes", "u32", "Detail", "bytes16")},
	{Name: "JobListRequest", MessageType: 250, SchemaVersion: 1, Fields: controlFields()},
	{Name: "JobListResponse", MessageType: 251, SchemaVersion: 1, Fields: controlFields("LeaderNodeID", "u16", "AppliedIndex", "u64", "Jobs", "list(StatusResponse)")},
}

var publicControlNestedLayoutsV1 = []PublicControlNestedDescriptor{
	{Name: "ClientRequestID", Fields: controlFields("ClientID", "bytes16(nonzero)", "Sequence", "u64(nonzero)")},
	{Name: "JobID", Fields: controlFields("Value", "bytes16(nonzero)")},
	{Name: "TaskID", Fields: controlFields("JobID", "JobID", "StageID", "u16(nonzero)", "Partition", "u16")},
	{Name: "TupleID", Fields: controlFields("JobID", "JobID", "SourceTask", "TaskID", "SourceSequence", "u64(nonzero)", "PathDigest", "sha256(nonzero)")},
	{Name: "TopologySpec", Fields: controlFields("CanonicalTopology", "bytes64")},
	{Name: "ResultRecordEntry", Fields: controlFields("Length", "u32", "Record", "canonical-result-record-v1")},
	{Name: "ResultPageBinding", Fields: controlFields("JobID", "JobID", "ManifestDigest", "sha256", "HasLastTuple", "bool", "Last", "TupleID", "PageBytes", "u32")},
}

var publicControlEnumDomainsV1 = []PublicControlEnumDescriptor{
	{Name: "JobState", Values: []string{"Pending=1", "Deploying=2", "Running=3", "Draining=4", "Succeeded=5", "Failed=6", "Canceled=7"}},
	{Name: "ControlErrorCode", Values: []string{"Malformed=1", "UnsupportedSchema=2", "InvalidRequest=3", "Starting=4", "NotLeader=5", "StaleRequest=6", "SkippedRequest=7", "IdentityReuse=8", "NotFound=9", "RevisionMismatch=10", "CapacityExhausted=11", "PageLimitTooSmall=12", "ResultUnavailable=13", "CorruptResult=14", "ResultTooLarge=15"}},
	{Name: "FailureCode", Values: []string{"Operator=1", "TupleInvalid=2", "Storage=3"}},
}

var publicControlIdentityDomainsV1 = []string{
	"crane/submit-control-command/v1",
	"crane/cancel-control-command/v1",
}

var publicControlErrorCodeMatrixV1 = []string{
	"Unbound=Malformed,UnsupportedSchema,InvalidRequest",
	"SubmitRequest=Starting,NotLeader,StaleRequest,SkippedRequest,IdentityReuse,CapacityExhausted,ResultTooLarge",
	"CancelRequest=Starting,NotLeader,StaleRequest,SkippedRequest,IdentityReuse,NotFound,RevisionMismatch,ResultTooLarge",
	"StatusRequest=Starting,NotLeader,NotFound",
	"ResultPageRequest=Starting,NotLeader,NotFound,PageLimitTooSmall,ResultUnavailable,CorruptResult",
	"JobListRequest=Starting,NotLeader",
}

var publicControlRulesV1 = []string{
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
}

// PublicControlContractV1 returns the immutable v1 +4 public-control contract.
func PublicControlContractV1() PublicControlContract {
	return PublicControlContract{
		SchemaVersion: PublicControlSchemaVersionV1, MessageTypeMin: 240, MessageTypeMax: 251,
		Messages:                    cloneControlMessages(publicControlMessageDescriptorsV1),
		NestedLayouts:               cloneControlNested(publicControlNestedLayoutsV1),
		EnumDomains:                 cloneControlEnums(publicControlEnumDomainsV1),
		MaxControlFrameBytes:        PublicControlMaxFrameBytesV1,
		MaxResultPageBytes:          PublicControlMaxResultPageBytesV1,
		MaxResultPageRecords:        PublicControlMaxResultPageRecordsV1,
		MinEncodedResultRecordBytes: PublicControlMinEncodedResultRecordBytesV1,
		MaxEncodedResultRecordBytes: PublicControlMaxEncodedResultRecordBytesV1,
		MaxRedirectEndpoints:        PublicControlMaxRedirectEndpointsV1,
		MaxEndpointBytes:            PublicControlMaxEndpointBytesV1,
		MaxErrorDetailBytes:         PublicControlMaxErrorDetailBytesV1,
		IdentityDomains:             append([]string(nil), publicControlIdentityDomainsV1...),
		ErrorCodeMatrix:             append([]string(nil), publicControlErrorCodeMatrixV1...),
		Rules:                       append([]string(nil), publicControlRulesV1...),
	}
}

// PublicSubmitCommandDigest hashes a client identity and complete canonical topology.
func PublicSubmitCommandDigest(request ClientRequestID, canonicalTopology []byte) [32]byte {
	encoded := append([]byte(publicControlIdentityDomainsV1[0]+"\x00"), request.ClientID[:]...)
	encoded = appendUint64(encoded, request.Sequence)
	encoded = append(encoded, canonicalTopology...)
	return sha256.Sum256(encoded)
}

// PublicCancelCommandDigest hashes a client identity, exact job, and expected revision.
func PublicCancelCommandDigest(request ClientRequestID, job JobID, expectedRevision uint64) [32]byte {
	encoded := append([]byte(publicControlIdentityDomainsV1[1]+"\x00"), request.ClientID[:]...)
	encoded = appendUint64(encoded, request.Sequence)
	encoded = append(encoded, job[:]...)
	encoded = appendUint64(encoded, expectedRevision)
	return sha256.Sum256(encoded)
}

func canonicalPublicControlContractBytes(contract PublicControlContract) []byte {
	encoded := appendString([]byte(publicControlContractFingerprintDomain), "crane-public-control")
	encoded = appendUint16(encoded, contract.SchemaVersion)
	encoded = appendUint16(encoded, contract.MessageTypeMin)
	encoded = appendUint16(encoded, contract.MessageTypeMax)
	encoded = appendUint16(encoded, uint16(len(contract.Messages)))
	for _, message := range contract.Messages {
		encoded = appendString(encoded, message.Name)
		encoded = appendUint16(encoded, message.MessageType)
		encoded = appendUint16(encoded, message.SchemaVersion)
		encoded = appendControlFields(encoded, message.Fields)
	}
	encoded = appendUint16(encoded, uint16(len(contract.NestedLayouts)))
	for _, nested := range contract.NestedLayouts {
		encoded = appendString(encoded, nested.Name)
		encoded = appendControlFields(encoded, nested.Fields)
	}
	encoded = appendUint16(encoded, uint16(len(contract.EnumDomains)))
	for _, enum := range contract.EnumDomains {
		encoded = appendString(encoded, enum.Name)
		encoded = appendUint16(encoded, uint16(len(enum.Values)))
		for _, value := range enum.Values {
			encoded = appendString(encoded, value)
		}
	}
	for _, bound := range []uint64{contract.MaxControlFrameBytes, contract.MaxResultPageBytes, contract.MaxResultPageRecords, contract.MinEncodedResultRecordBytes, contract.MaxEncodedResultRecordBytes, contract.MaxRedirectEndpoints, contract.MaxEndpointBytes, contract.MaxErrorDetailBytes} {
		encoded = appendUint64(encoded, bound)
	}
	encoded = appendStringList(encoded, contract.IdentityDomains)
	encoded = appendStringList(encoded, contract.ErrorCodeMatrix)
	return appendStringList(encoded, contract.Rules)
}

func controlFields(values ...string) []PublicControlFieldDescriptor {
	fields := make([]PublicControlFieldDescriptor, 0, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		fields = append(fields, PublicControlFieldDescriptor{Name: values[index], Encoding: values[index+1]})
	}
	return fields
}

func cloneControlMessages(input []PublicControlMessageDescriptor) []PublicControlMessageDescriptor {
	output := append([]PublicControlMessageDescriptor(nil), input...)
	for index := range output {
		output[index].Fields = append([]PublicControlFieldDescriptor(nil), input[index].Fields...)
	}
	return output
}

func cloneControlNested(input []PublicControlNestedDescriptor) []PublicControlNestedDescriptor {
	output := append([]PublicControlNestedDescriptor(nil), input...)
	for index := range output {
		output[index].Fields = append([]PublicControlFieldDescriptor(nil), input[index].Fields...)
	}
	return output
}

func cloneControlEnums(input []PublicControlEnumDescriptor) []PublicControlEnumDescriptor {
	output := append([]PublicControlEnumDescriptor(nil), input...)
	for index := range output {
		output[index].Values = append([]string(nil), input[index].Values...)
	}
	return output
}

func appendControlFields(encoded []byte, fields []PublicControlFieldDescriptor) []byte {
	encoded = appendUint16(encoded, uint16(len(fields)))
	for _, field := range fields {
		encoded = appendString(encoded, field.Name)
		encoded = appendString(encoded, field.Encoding)
	}
	return encoded
}

func appendStringList(encoded []byte, values []string) []byte {
	encoded = appendUint16(encoded, uint16(len(values)))
	for _, value := range values {
		encoded = appendString(encoded, value)
	}
	return encoded
}
