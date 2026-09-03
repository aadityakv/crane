package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	// ControlSchemaVersion is the canonical public-control payload schema.
	ControlSchemaVersion = model.PublicControlSchemaVersionV1
	// MaxControlPayloadBytes excludes the authenticated fixed header and MAC.
	MaxControlPayloadBytes = model.PublicControlMaxFrameBytesV1 - wire.FixedHeaderSize - wire.MACSize
	// MaxResultPageBytes bounds complete encoded result-record entries.
	MaxResultPageBytes = model.PublicControlMaxResultPageBytesV1
	// MaxResultPageRecords bounds allocation before record decoding.
	MaxResultPageRecords = model.PublicControlMaxResultPageRecordsV1
	// MinEncodedResultRecordBytes is the smallest u32-length-prefixed canonical record.
	MinEncodedResultRecordBytes = model.PublicControlMinEncodedResultRecordBytesV1
	// MaxEncodedResultRecordBytes bounds one u32-length-prefixed canonical record.
	MaxEncodedResultRecordBytes = model.PublicControlMaxEncodedResultRecordBytesV1
	// MaxLeaderRedirectEndpoints is the fixed five-voter ceiling.
	MaxLeaderRedirectEndpoints = model.PublicControlMaxRedirectEndpointsV1
	// MaxControlEndpointBytes bounds one canonical +6 host:port spelling.
	MaxControlEndpointBytes = model.PublicControlMaxEndpointBytesV1
	// MaxControlErrorDetailBytes bounds typed error detail.
	MaxControlErrorDetailBytes = model.PublicControlMaxErrorDetailBytesV1
)

var (
	// ErrMalformedControlMessage classifies truncated, trailing, or noncanonical payloads.
	ErrMalformedControlMessage = errors.New("malformed Crane public-control message")
	// ErrUnsupportedControlSchema classifies an unknown +6 payload schema.
	ErrUnsupportedControlSchema = errors.New("unsupported Crane public-control schema")
	// ErrUnexpectedControlMessage classifies an unknown or mismatched +6 message type.
	ErrUnexpectedControlMessage = errors.New("unexpected Crane public-control message type")
	// ErrInvalidControlMessage classifies a well-formed payload violating its schema.
	ErrInvalidControlMessage = errors.New("invalid Crane public-control message")
	// ErrControlMessageTooLarge classifies payloads above the complete-frame budget.
	ErrControlMessageTooLarge = errors.New("crane public-control message too large")
)

// ControlMessage is one concrete payload owned by message IDs 240 through 249.
type ControlMessage interface{ MessageType() wire.MessageType }

// JobState is the public, stable job-lifecycle domain.
type JobState uint8

const (
	// JobPending has a committed definition but no complete assignment.
	JobPending JobState = iota + 1
	// JobDeploying is installing a complete assignment and source EOF vector.
	JobDeploying
	// JobRunning admits deterministic source and tuple work.
	JobRunning
	// JobDraining has closed source admission and is sealing results.
	JobDraining
	// JobSucceeded has final checkpoints and a complete manifest set.
	JobSucceeded
	// JobFailed is terminal after a deterministic failure.
	JobFailed
	// JobCanceled is terminal after an applied client cancellation.
	JobCanceled
)

// ControlErrorCode is the bounded public-control rejection domain.
type ControlErrorCode uint16

const (
	// ControlErrorMalformed reports a malformed payload.
	ControlErrorMalformed ControlErrorCode = iota + 1
	// ControlErrorUnsupportedSchema reports an unknown schema version.
	ControlErrorUnsupportedSchema
	// ControlErrorInvalidRequest reports a canonical schema violation.
	ControlErrorInvalidRequest
	// ControlErrorStarting reports a local dependency gate that is not ready.
	ControlErrorStarting
	// ControlErrorNotLeader reports leader-gated service refusal.
	ControlErrorNotLeader
	// ControlErrorStaleRequest reports an older client sequence.
	ControlErrorStaleRequest
	// ControlErrorSkippedRequest reports a gap in client sequence.
	ControlErrorSkippedRequest
	// ControlErrorIdentityReuse reports one request identity with changed bytes.
	ControlErrorIdentityReuse
	// ControlErrorNotFound reports an unknown retained job.
	ControlErrorNotFound
	// ControlErrorRevisionMismatch reports a stale expected job revision.
	ControlErrorRevisionMismatch
	// ControlErrorCapacityExhausted reports a deterministic bounded-capacity refusal.
	ControlErrorCapacityExhausted
	// ControlErrorPageLimitTooSmall reports the first complete record size required.
	ControlErrorPageLimitTooSmall
	// ControlErrorResultUnavailable reports unavailable committed result copies.
	ControlErrorResultUnavailable
	// ControlErrorCorruptResult reports an inconsistent committed manifest or artifact.
	ControlErrorCorruptResult
	// ControlErrorResultTooLarge reports a deterministically consumed client
	// mutation whose durable result exceeded the replicated cache bound, so
	// the sequence is spent but no cached outcome can ever be served.
	ControlErrorResultTooLarge
)

// SubmitRequest submits one canonical topology under a sequenced client identity.
type SubmitRequest struct {
	// Request is the durable client sequence used for retry suppression.
	Request model.ClientRequestID
	// Topology is the complete validated immutable job graph.
	Topology model.TopologySpec
	// Digest binds Request and the complete canonical topology bytes.
	Digest [32]byte
}

// MessageType returns the stable submit-request ID.
func (SubmitRequest) MessageType() wire.MessageType { return wire.MessageCraneSubmitRequest }

// SubmitResponse echoes the durable result of one applied submission.
type SubmitResponse struct {
	// Request is the exact applied client request identity.
	Request model.ClientRequestID
	// Digest is the exact applied submit-command digest.
	Digest [32]byte
	// JobID is the deterministic retained job identity.
	JobID model.JobID
	// JobControlRevision is the durable job revision returned by the state machine.
	JobControlRevision uint64
	// State is JobPending for the cached submit result.
	State JobState
}

// MessageType returns the stable submit-response ID.
func (SubmitResponse) MessageType() wire.MessageType { return wire.MessageCraneSubmitResponse }

// CancelRequest cancels one exact retained job revision.
type CancelRequest struct {
	// Request is the durable client sequence used for retry suppression.
	Request model.ClientRequestID
	// JobID names the job-control subject.
	JobID model.JobID
	// ExpectedJobControlRevision fences concurrent lifecycle changes.
	ExpectedJobControlRevision uint64
	// Digest binds Request, JobID, the cancel action, and expected revision.
	Digest [32]byte
}

// MessageType returns the stable cancel-request ID.
func (CancelRequest) MessageType() wire.MessageType { return wire.MessageCraneCancelRequest }

// CancelResponse echoes the durable result of one applied cancellation.
type CancelResponse struct {
	// Request is the exact applied client request identity.
	Request model.ClientRequestID
	// Digest is the exact applied cancel-command digest.
	Digest [32]byte
	// JobID is the canceled job identity.
	JobID model.JobID
	// JobControlRevision is the durable revision after cancellation.
	JobControlRevision uint64
	// State is JobCanceled for the cached cancel result.
	State JobState
}

// MessageType returns the stable cancel-response ID.
func (CancelResponse) MessageType() wire.MessageType { return wire.MessageCraneCancelResponse }

// StatusRequest asks for one linearizable retained-job summary.
type StatusRequest struct {
	// JobID names the retained job.
	JobID model.JobID
}

// MessageType returns the stable status-request ID.
func (StatusRequest) MessageType() wire.MessageType { return wire.MessageCraneStatusRequest }

// StatusResponse is a concrete bounded summary of replicated job and result state.
type StatusResponse struct {
	// JobID names the retained job.
	JobID model.JobID
	// AppliedIndex tags the atomic Crane view used after the read barrier.
	AppliedIndex uint64
	// TopologyDigest binds the immutable canonical specification.
	TopologyDigest [32]byte
	// JobControlRevision fences lifecycle and assignment changes.
	JobControlRevision uint64
	// State is the exact public lifecycle value.
	State JobState
	// HasAssignment distinguishes an absent set from a present nonzero revision.
	HasAssignment bool
	// AssignmentRevision is the complete set revision when HasAssignment is true.
	AssignmentRevision uint64
	// AssignmentDigest binds the complete assignment when HasAssignment is true.
	AssignmentDigest [32]byte
	// SourceTaskCount is the immutable number of source partitions.
	SourceTaskCount uint16
	// ResultPartitionCount is the immutable expected number of result manifests.
	ResultPartitionCount uint16
	// CompletedSourceTasks counts checkpoints exactly at committed EOF.
	CompletedSourceTasks uint16
	// ManifestCount is the number of committed result manifests.
	ManifestCount uint16
	// HasManifestSet distinguishes no manifests from a bound committed set.
	HasManifestSet bool
	// ManifestSetDigest binds every committed manifest in canonical order.
	ManifestSetDigest [32]byte
	// HasFailure distinguishes a terminal failure from zero failure fields.
	HasFailure bool
	// FailureCode is the deterministic worker failure category.
	FailureCode model.FailureCode
	// FailureDetailDigest binds bounded private failure detail without exposing it.
	FailureDetailDigest [32]byte
}

// MessageType returns the stable status-response ID.
func (StatusResponse) MessageType() wire.MessageType { return wire.MessageCraneStatusResponse }

// ResultPageRequest names a stateless global result cursor.
type ResultPageRequest struct {
	// JobID names the retained job.
	JobID model.JobID
	// ManifestDigest binds the exact committed manifest set.
	ManifestDigest [32]byte
	// HasLastTuple distinguishes the beginning of results from a real cursor.
	HasLastTuple bool
	// Last is the last globally returned TupleID when HasLastTuple is true.
	Last model.TupleID
	// PageBytes bounds complete encoded record entries from 1 through 512 KiB.
	PageBytes uint32
}

// MessageType returns the stable result-page-request ID.
func (ResultPageRequest) MessageType() wire.MessageType { return wire.MessageCraneResultPageRequest }

// ResultPageResponse repeats a request binding and advances one global cursor.
type ResultPageResponse struct {
	// JobID repeats the requested retained job.
	JobID model.JobID
	// ManifestDigest repeats the exact committed manifest-set identity.
	ManifestDigest [32]byte
	// RequestHasLastTuple repeats the request cursor selector.
	RequestHasLastTuple bool
	// RequestLast repeats the request cursor value.
	RequestLast model.TupleID
	// PageBytes repeats the request record-entry budget.
	PageBytes uint32
	// Records are complete owned records in strict global TupleID order.
	Records []model.ResultRecord
	// NextHasLastTuple identifies whether the next cursor is present.
	NextHasLastTuple bool
	// NextLast is the last emitted record or the unchanged request cursor for an empty page.
	NextLast model.TupleID
	// End reports that no later record exists in the bound manifest set.
	End bool
}

// MessageType returns the stable result-page-response ID.
func (ResultPageResponse) MessageType() wire.MessageType { return wire.MessageCraneResultPageResponse }

// LeaderRedirect returns one known leader or the complete static voter alternatives.
type LeaderRedirect struct {
	// Endpoints contains exactly one, three, or five sorted unique canonical +6 endpoints.
	Endpoints []string
}

// MessageType returns the stable redirect ID.
func (LeaderRedirect) MessageType() wire.MessageType { return wire.MessageCraneLeaderRedirect }

// ControlError is a typed bounded rejection correlated by the authenticated frame.
type ControlError struct {
	// RelatedMessage is the request type that was rejected.
	RelatedMessage wire.MessageType
	// Code is the stable rejection category.
	Code ControlErrorCode
	// Retryable advises whether unchanged external state may later succeed.
	Retryable bool
	// HasClientRequest selects the fixed mutation binding.
	HasClientRequest bool
	// ClientRequest is the exact mutation identity when selected.
	ClientRequest model.ClientRequestID
	// ClientDigest is the exact candidate command digest when the mutation binding is selected.
	ClientDigest [32]byte
	// HasStatusRequest selects the exact retained-job status binding.
	HasStatusRequest bool
	// StatusJobID is the exact status subject when selected.
	StatusJobID model.JobID
	// HasResultPage selects the fixed stateless result-page binding.
	HasResultPage bool
	// ResultPage is the exact page request when selected.
	ResultPage ResultPageRequest
	// RequiredBytes is nonzero only for ControlErrorPageLimitTooSmall.
	RequiredBytes uint32
	// Detail is bounded owned UTF-8 text without result or secret bytes.
	Detail []byte
}

// MessageType returns the stable typed-error ID.
func (ControlError) MessageType() wire.MessageType { return wire.MessageCraneControlError }

// SubmitCommandDigest returns the canonical digest for a validated public submission.
func SubmitCommandDigest(request model.ClientRequestID, topology model.TopologySpec) ([32]byte, error) {
	if err := request.Validate(); err != nil {
		return [32]byte{}, err
	}
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		return [32]byte{}, err
	}
	return model.PublicSubmitCommandDigest(request, validated.CanonicalBytes()), nil
}

// CancelCommandDigest returns the canonical digest for one exact cancel action.
func CancelCommandDigest(request model.ClientRequestID, job model.JobID, expectedRevision uint64) ([32]byte, error) {
	if err := request.Validate(); err != nil {
		return [32]byte{}, err
	}
	if err := job.Validate(); err != nil {
		return [32]byte{}, err
	}
	if expectedRevision == 0 || expectedRevision == ^uint64(0) {
		return [32]byte{}, errors.New("expected job-control revision has no valid successor")
	}
	return model.PublicCancelCommandDigest(request, job, expectedRevision), nil
}

// ValidateSubmitResponseCorrelation binds an applied submit result to one exact request.
func ValidateSubmitResponseCorrelation(request SubmitRequest, response SubmitResponse) error {
	if err := validateControlMessage(request); err != nil {
		return err
	}
	if err := validateControlMessage(response); err != nil {
		return err
	}
	validated, err := model.ValidateTopology(request.Topology)
	if err != nil {
		return err
	}
	wantJob := model.DeriveJobID(request.Request, validated.Digest())
	if response.Request != request.Request || response.Digest != request.Digest || response.JobID != wantJob || response.JobControlRevision != 1 {
		return errors.New("submit response does not bind the exact durable request")
	}
	return nil
}

// ValidateCancelResponseCorrelation binds an applied cancel result to one exact request.
func ValidateCancelResponseCorrelation(request CancelRequest, response CancelResponse) error {
	if err := validateControlMessage(request); err != nil {
		return err
	}
	if err := validateControlMessage(response); err != nil {
		return err
	}
	wantRevision := request.ExpectedJobControlRevision + 1
	if response.Request != request.Request || response.Digest != request.Digest || response.JobID != request.JobID || response.JobControlRevision != wantRevision {
		return errors.New("cancel response does not bind the exact durable request")
	}
	return nil
}

// ValidateSubmitErrorCorrelation binds an error to one exact canonical submit request.
func ValidateSubmitErrorCorrelation(request SubmitRequest, controlError ControlError) error {
	if err := validateControlMessage(request); err != nil {
		return err
	}
	if err := validateControlError(controlError); err != nil {
		return err
	}
	if controlError.RelatedMessage != wire.MessageCraneSubmitRequest || !controlError.HasClientRequest || controlError.ClientRequest != request.Request || controlError.ClientDigest != request.Digest {
		return errors.New("submit error does not bind the exact request")
	}
	return nil
}

// ValidateCancelErrorCorrelation binds an error to one exact canonical cancel request.
func ValidateCancelErrorCorrelation(request CancelRequest, controlError ControlError) error {
	if err := validateControlMessage(request); err != nil {
		return err
	}
	if err := validateControlError(controlError); err != nil {
		return err
	}
	if controlError.RelatedMessage != wire.MessageCraneCancelRequest || !controlError.HasClientRequest || controlError.ClientRequest != request.Request || controlError.ClientDigest != request.Digest {
		return errors.New("cancel error does not bind the exact request")
	}
	return nil
}

// ValidateStatusErrorCorrelation binds an error to one exact status request.
func ValidateStatusErrorCorrelation(request StatusRequest, controlError ControlError) error {
	if err := validateControlMessage(request); err != nil {
		return err
	}
	if err := validateControlError(controlError); err != nil {
		return err
	}
	if controlError.RelatedMessage != wire.MessageCraneStatusRequest || !controlError.HasStatusRequest || controlError.StatusJobID != request.JobID {
		return errors.New("status error does not bind the exact request")
	}
	return nil
}

// ValidateResultPageErrorCorrelation binds an error to one exact stateless page request.
func ValidateResultPageErrorCorrelation(request ResultPageRequest, controlError ControlError) error {
	if err := validateControlMessage(request); err != nil {
		return err
	}
	if err := validateControlError(controlError); err != nil {
		return err
	}
	if controlError.RelatedMessage != wire.MessageCraneResultPageRequest || !controlError.HasResultPage || controlError.ResultPage != request {
		return errors.New("result-page error does not bind the exact request")
	}
	return nil
}

// EncodedResultPageRecordBytes returns one complete u32-length-prefixed record entry.
func EncodedResultPageRecordBytes(record model.ResultRecord) ([]byte, error) {
	stream, err := model.MarshalResultRecord(record)
	if err != nil {
		return nil, err
	}
	if len(stream)+4 > MaxEncodedResultRecordBytes {
		return nil, errors.New("result record entry exceeds public page bound")
	}
	encoded := make([]byte, 4, 4+len(stream))
	encoded[0], encoded[1], encoded[2], encoded[3] = byte(len(stream)>>24), byte(len(stream)>>16), byte(len(stream)>>8), byte(len(stream))
	return append(encoded, stream...), nil
}

// ValidateResultPageResponseCorrelation requires a response to repeat one exact
// stateless job, manifest, cursor, and page-limit request binding.
func ValidateResultPageResponseCorrelation(request ResultPageRequest, response ResultPageResponse) error {
	if err := validateResultPageRequest(request); err != nil {
		return err
	}
	if err := validateResultPageResponse(response); err != nil {
		return err
	}
	if response.JobID != request.JobID || response.ManifestDigest != request.ManifestDigest || response.RequestHasLastTuple != request.HasLastTuple || response.RequestLast != request.Last || response.PageBytes != request.PageBytes {
		return errors.New("result-page response does not repeat the exact request binding")
	}
	return nil
}

func validateControlMessage(message ControlMessage) error {
	switch value := message.(type) {
	case SubmitRequest:
		want, err := SubmitCommandDigest(value.Request, value.Topology)
		if err != nil || value.Digest == ([32]byte{}) || value.Digest != want {
			return errors.New("invalid submit request or digest")
		}
	case SubmitResponse:
		if value.Request.Validate() != nil || value.Digest == ([32]byte{}) || value.JobID.Validate() != nil || value.JobControlRevision != 1 || value.State != JobPending {
			return errors.New("invalid submit response")
		}
	case CancelRequest:
		want, err := CancelCommandDigest(value.Request, value.JobID, value.ExpectedJobControlRevision)
		if err != nil || value.Digest == ([32]byte{}) || value.Digest != want {
			return errors.New("invalid cancel request or digest")
		}
	case CancelResponse:
		if value.Request.Validate() != nil || value.Digest == ([32]byte{}) || value.JobID.Validate() != nil || value.JobControlRevision == 0 || value.State != JobCanceled {
			return errors.New("invalid cancel response")
		}
	case StatusRequest:
		return value.JobID.Validate()
	case StatusResponse:
		return validateStatusResponse(value)
	case ResultPageRequest:
		return validateResultPageRequest(value)
	case ResultPageResponse:
		return validateResultPageResponse(value)
	case LeaderRedirect:
		return validateLeaderRedirect(value)
	case ControlError:
		return validateControlError(value)
	default:
		return ErrUnexpectedControlMessage
	}
	return nil
}

func validateStatusResponse(value StatusResponse) error {
	if value.JobID.Validate() != nil || value.AppliedIndex == 0 || value.TopologyDigest == ([32]byte{}) || value.JobControlRevision == 0 || value.State < JobPending || value.State > JobCanceled {
		return errors.New("invalid status identity")
	}
	assignmentPresent := value.AssignmentRevision != 0 && value.AssignmentDigest != ([32]byte{})
	assignmentAbsent := value.AssignmentRevision == 0 && value.AssignmentDigest == ([32]byte{})
	if value.HasAssignment && !assignmentPresent || !value.HasAssignment && !assignmentAbsent {
		return errors.New("contradictory assignment binding")
	}
	if value.SourceTaskCount == 0 || uint64(value.SourceTaskCount) > model.LimitsV1().MaxTasksPerStage || value.ResultPartitionCount == 0 || uint64(value.ResultPartitionCount) > model.LimitsV1().MaxResultManifestsPerJob || value.CompletedSourceTasks > value.SourceTaskCount || value.ManifestCount > value.ResultPartitionCount {
		return errors.New("status progress outside bounds")
	}
	manifestPresent := value.ManifestCount > 0 && value.ManifestSetDigest != ([32]byte{})
	manifestAbsent := value.ManifestCount == 0 && value.ManifestSetDigest == ([32]byte{})
	if value.HasManifestSet && !manifestPresent || !value.HasManifestSet && !manifestAbsent {
		return errors.New("contradictory manifest binding")
	}
	if value.HasFailure {
		if value.State != JobFailed || value.FailureCode < model.FailureOperator || value.FailureCode > model.FailureStorage || value.FailureDetailDigest == ([32]byte{}) {
			return errors.New("invalid failure binding")
		}
	} else if value.FailureCode != 0 || value.FailureDetailDigest != ([32]byte{}) || value.State == JobFailed {
		return errors.New("missing or contradictory failure binding")
	}
	if !value.HasAssignment && (value.CompletedSourceTasks != 0 || value.ManifestCount != 0) {
		return errors.New("progress exists without assignment")
	}
	if value.ManifestCount > 0 && (!value.HasAssignment || value.CompletedSourceTasks != value.SourceTaskCount) {
		return errors.New("manifest exists before assigned sources complete")
	}
	switch value.State {
	case JobPending:
		if value.HasAssignment || value.CompletedSourceTasks != 0 || value.ManifestCount != 0 {
			return errors.New("pending status contains deployment progress")
		}
	case JobDeploying:
		if !value.HasAssignment || value.CompletedSourceTasks != 0 || value.ManifestCount != 0 {
			return errors.New("deploying status contradicts assignment or progress")
		}
	case JobRunning:
		if !value.HasAssignment || value.ManifestCount != 0 {
			return errors.New("running status contradicts assignment or manifests")
		}
	case JobDraining:
		if !value.HasAssignment || value.CompletedSourceTasks != value.SourceTaskCount {
			return errors.New("draining status requires completed assigned sources")
		}
	case JobSucceeded:
		if !value.HasAssignment || value.CompletedSourceTasks != value.SourceTaskCount || value.ManifestCount != value.ResultPartitionCount || !value.HasManifestSet {
			return errors.New("succeeded status is incomplete")
		}
	case JobFailed, JobCanceled:
	}
	return nil
}

func validateResultPageRequest(value ResultPageRequest) error {
	if value.JobID.Validate() != nil || value.ManifestDigest == ([32]byte{}) || value.PageBytes == 0 || value.PageBytes > MaxResultPageBytes {
		return errors.New("invalid result-page binding or limit")
	}
	if value.HasLastTuple {
		if value.Last.Validate() != nil || value.Last.JobID != value.JobID {
			return errors.New("invalid or foreign result-page cursor")
		}
	} else if value.Last != (model.TupleID{}) {
		return errors.New("cursor value present without selector")
	}
	return nil
}

func validateResultPageResponse(value ResultPageResponse) error {
	request := ResultPageRequest{JobID: value.JobID, ManifestDigest: value.ManifestDigest, HasLastTuple: value.RequestHasLastTuple, Last: value.RequestLast, PageBytes: value.PageBytes}
	if err := validateResultPageRequest(request); err != nil {
		return err
	}
	if len(value.Records) > MaxResultPageRecords {
		return errors.New("result page count exceeds allocation bound")
	}
	used := uint32(0)
	prior := value.RequestLast
	for index, record := range value.Records {
		if err := record.Validate(); err != nil || record.TupleID.JobID != value.JobID {
			return errors.New("invalid or foreign result record")
		}
		if (value.RequestHasLastTuple || index > 0) && !tupleIDLess(prior, record.TupleID) {
			return errors.New("result records are not strictly globally increasing")
		}
		entry, err := EncodedResultPageRecordBytes(record)
		if err != nil || uint32(len(entry)) > value.PageBytes-used {
			return errors.New("complete result record does not fit page budget")
		}
		used += uint32(len(entry))
		prior = record.TupleID
	}
	if len(value.Records) == 0 {
		if !value.End || value.NextHasLastTuple != value.RequestHasLastTuple || value.NextLast != value.RequestLast {
			return errors.New("empty page must terminate and preserve cursor")
		}
		return nil
	}
	if !value.NextHasLastTuple || value.NextLast != value.Records[len(value.Records)-1].TupleID {
		return errors.New("next cursor does not equal last emitted record")
	}
	return nil
}

func validateLeaderRedirect(value LeaderRedirect) error {
	if len(value.Endpoints) != 1 && len(value.Endpoints) != 3 && len(value.Endpoints) != 5 {
		return errors.New("redirect endpoint count must be one, three, or five")
	}
	for index, endpoint := range value.Endpoints {
		if err := validateControlEndpoint(endpoint); err != nil {
			return err
		}
		if index > 0 && value.Endpoints[index-1] >= endpoint {
			return errors.New("redirect endpoints are not sorted unique")
		}
	}
	return nil
}

func validateControlEndpoint(endpoint string) error {
	if endpoint == "" || len(endpoint) > MaxControlEndpointBytes {
		return errors.New("control endpoint length outside bounds")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return errors.New("invalid control endpoint")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 || strconv.FormatUint(port, 10) != portText {
		return errors.New("invalid control endpoint port")
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.String() != host || net.JoinHostPort(host, portText) != endpoint {
			return errors.New("noncanonical control IP endpoint")
		}
		return nil
	}
	if host != strings.ToLower(host) || strings.HasSuffix(host, ".") || len(host) > 253 || strings.Trim(host, "0123456789.") == "" {
		return errors.New("noncanonical control DNS endpoint")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errors.New("invalid control DNS label")
		}
		for _, character := range []byte(label) {
			if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
				return errors.New("invalid control DNS character")
			}
		}
	}
	if net.JoinHostPort(host, portText) != endpoint {
		return errors.New("noncanonical control endpoint")
	}
	return nil
}

func validateControlError(value ControlError) error {
	if value.Code < ControlErrorMalformed || value.Code > ControlErrorResultTooLarge || len(value.Detail) > MaxControlErrorDetailBytes || !utf8.Valid(value.Detail) || bytes.IndexByte(value.Detail, 0) >= 0 {
		return errors.New("invalid control error code or detail")
	}
	selectors := 0
	if value.HasClientRequest {
		selectors++
	}
	if value.HasStatusRequest {
		selectors++
	}
	if value.HasResultPage {
		selectors++
	}
	if selectors > 1 {
		return errors.New("control error bindings are mutually exclusive")
	}
	switch value.RelatedMessage {
	case wire.MessageCraneSubmitRequest, wire.MessageCraneCancelRequest, wire.MessageCraneStatusRequest, wire.MessageCraneResultPageRequest:
	default:
		return errors.New("control error relates to a non-request message")
	}
	if value.HasClientRequest {
		if value.RelatedMessage != wire.MessageCraneSubmitRequest && value.RelatedMessage != wire.MessageCraneCancelRequest || value.ClientRequest.Validate() != nil || value.ClientDigest == ([32]byte{}) || value.HasStatusRequest || value.StatusJobID != (model.JobID{}) || value.ResultPage != (ResultPageRequest{}) {
			return errors.New("mutation error has an incompatible client binding")
		}
	} else if value.ClientRequest != (model.ClientRequestID{}) || value.ClientDigest != ([32]byte{}) {
		return errors.New("unselected client binding is nonzero")
	}
	if value.HasStatusRequest {
		if value.RelatedMessage != wire.MessageCraneStatusRequest || value.StatusJobID.Validate() != nil || value.ResultPage != (ResultPageRequest{}) {
			return errors.New("status error has an incompatible request binding")
		}
	} else if value.StatusJobID != (model.JobID{}) {
		return errors.New("unselected status binding is nonzero")
	}
	if value.HasResultPage {
		if value.RelatedMessage != wire.MessageCraneResultPageRequest || validateResultPageRequest(value.ResultPage) != nil {
			return errors.New("result error has an incompatible page binding")
		}
	} else if value.ResultPage != (ResultPageRequest{}) {
		return errors.New("unselected page binding is nonzero")
	}
	if selectors == 0 && !predecodeControlError(value.Code) {
		return errors.New("unbound error is not a predecode rejection")
	}
	if selectors > 0 && !controlErrorCodeCompatible(value.RelatedMessage, value.Code) {
		return errors.New("control error code is incompatible with request type")
	}
	if value.Code == ControlErrorPageLimitTooSmall {
		if value.RelatedMessage != wire.MessageCraneResultPageRequest || value.RequiredBytes < uint32(MinEncodedResultRecordBytes) || value.RequiredBytes > MaxEncodedResultRecordBytes || value.RequiredBytes <= value.ResultPage.PageBytes {
			return errors.New("invalid PageLimitTooSmall required byte count")
		}
	} else if value.RequiredBytes != 0 {
		return errors.New("required bytes present on another error")
	}
	return nil
}

func predecodeControlError(code ControlErrorCode) bool {
	return code == ControlErrorMalformed || code == ControlErrorUnsupportedSchema || code == ControlErrorInvalidRequest
}

func controlErrorCodeCompatible(message wire.MessageType, code ControlErrorCode) bool {
	if code == ControlErrorStarting || code == ControlErrorNotLeader {
		return true
	}
	switch message {
	case wire.MessageCraneSubmitRequest:
		return code == ControlErrorStaleRequest || code == ControlErrorSkippedRequest || code == ControlErrorIdentityReuse || code == ControlErrorCapacityExhausted || code == ControlErrorResultTooLarge
	case wire.MessageCraneCancelRequest:
		return code == ControlErrorStaleRequest || code == ControlErrorSkippedRequest || code == ControlErrorIdentityReuse || code == ControlErrorNotFound || code == ControlErrorRevisionMismatch || code == ControlErrorResultTooLarge
	case wire.MessageCraneStatusRequest:
		return code == ControlErrorNotFound
	case wire.MessageCraneResultPageRequest:
		return code == ControlErrorNotFound || code == ControlErrorPageLimitTooSmall || code == ControlErrorResultUnavailable || code == ControlErrorCorruptResult
	default:
		return false
	}
}

func tupleIDLess(left, right model.TupleID) bool {
	if comparison := bytes.Compare(left.JobID[:], right.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if comparison := bytes.Compare(left.SourceTask.JobID[:], right.SourceTask.JobID[:]); comparison != 0 {
		return comparison < 0
	}
	if left.SourceTask.StageID != right.SourceTask.StageID {
		return left.SourceTask.StageID < right.SourceTask.StageID
	}
	if left.SourceTask.Partition != right.SourceTask.Partition {
		return left.SourceTask.Partition < right.SourceTask.Partition
	}
	if left.SourceSequence != right.SourceSequence {
		return left.SourceSequence < right.SourceSequence
	}
	return bytes.Compare(left.PathDigest[:], right.PathDigest[:]) < 0
}

func invalidControl(message ControlMessage, err error) error {
	return fmt.Errorf("%w: message %d: %v", ErrInvalidControlMessage, message.MessageType(), err)
}
