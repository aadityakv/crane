package control

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/aadityakv/crane/internal/crane/membership"
	"github.com/aadityakv/crane/internal/crane/model"
	"github.com/aadityakv/crane/internal/crane/protocol"
	"github.com/aadityakv/crane/internal/crane/state"
	"github.com/aadityakv/crane/internal/raft"
	"github.com/aadityakv/crane/internal/swim"
	"github.com/aadityakv/crane/internal/wire"
)

// ErrControlRequestUnauthorized classifies +6 frames that fail cluster,
// membership, source-IP, or replay admission and are dropped without response.
var ErrControlRequestUnauthorized = errors.New("crane control request unauthorized")

// handleConnection serves exactly one bounded request and one correlated
// response, then returns so the caller closes the connection.
func (service *Service) handleConnection(ctx context.Context, connection net.Conn) {
	requestContext, cancel := context.WithTimeout(ctx, service.timeout)
	defer cancel()
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.PublicControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &service.clusterID
	stream := wire.NewTCPFrameStream(connection, service.authenticator, limits, service.timeout)
	frame, err := stream.ReadFrame(requestContext)
	if err != nil {
		return
	}
	response := service.serveFrame(requestContext, connection.RemoteAddr(), frame)
	if response == nil {
		return
	}
	payload, err := protocol.MarshalControlMessage(response)
	if err != nil {
		return
	}
	outbound := wire.Frame{Header: wire.Header{
		Version: wire.Version1, Message: response.MessageType(), ClusterID: service.clusterID,
		SenderID: service.configuration.NodeID, RequestID: frame.Header.RequestID,
		TimestampMillis: service.clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}, Payload: payload}
	_ = stream.WriteFrame(requestContext, outbound)
}

// serveFrame validates one authenticated frame through replay, schema, and
// membership admission, then dispatches it. A nil result closes the
// connection without a response.
func (service *Service) serveFrame(ctx context.Context, remote net.Addr, frame wire.Frame) protocol.ControlMessage {
	if validateControlRequestHeader(service.clusterID, frame.Header) != nil {
		return nil
	}
	sender, requestID := frame.Header.SenderID, frame.Header.RequestID
	timestamp := time.UnixMilli(frame.Header.TimestampMillis)
	if service.replay.preflight(sender, requestID, timestamp) != nil {
		return nil
	}
	message, err := protocol.UnmarshalControlMessage(frame.Header.Message, frame.Payload)
	if err != nil {
		service.replay.recordInvalid(sender, requestID, timestamp)
		if service.authorizeSender(sender, remote) != nil {
			return nil
		}
		return predecodeErrorResponse(frame.Header.Message, err)
	}
	if service.authorizeSender(sender, remote) != nil {
		service.replay.recordInvalid(sender, requestID, timestamp)
		return nil
	}
	if service.replay.commit(sender, requestID, timestamp) != nil {
		return nil
	}
	select {
	case <-service.membership.Ready():
	default:
		return requestBoundError(message, protocol.ControlErrorStarting, true, "membership admission is not ready")
	}
	return service.dispatch(ctx, message)
}

// validateControlRequestHeader accepts only canonical +6 request frames.
func validateControlRequestHeader(clusterID [16]byte, header wire.Header) error {
	if header.Version != wire.Version1 || header.Codec != wire.CodecBinary || header.ClusterID != clusterID || header.SenderID == 0 || header.RequestID == (wire.RequestID{}) {
		return ErrControlRequestUnauthorized
	}
	switch header.Message {
	case wire.MessageCraneSubmitRequest, wire.MessageCraneCancelRequest, wire.MessageCraneStatusRequest, wire.MessageCraneResultPageRequest, wire.MessageCraneJobListRequest:
		return nil
	default:
		return ErrControlRequestUnauthorized
	}
}

// authorizeSender requires an active membership record and an authorized
// source IP for the authenticated sender. An unready membership view fails
// closed here and is reported as Starting after decode.
func (service *Service) authorizeSender(sender uint16, remote net.Addr) error {
	select {
	case <-service.membership.Ready():
	default:
		return nil
	}
	view := service.membership.View()
	if _, ok := activeViewMember(view, sender); !ok {
		return ErrControlRequestUnauthorized
	}
	if service.membership.AuthorizeTCP(sender, remote) != nil {
		return ErrControlRequestUnauthorized
	}
	return nil
}

// activeViewMember locates one Alive or Suspect member in an owned view.
func activeViewMember(view membership.View, node uint16) (swim.Member, bool) {
	for _, member := range view.Members {
		if member.NodeID == node && (member.Status == swim.Alive || member.Status == swim.Suspect) {
			return member, true
		}
	}
	return swim.Member{}, false
}

// dispatch performs the leader, gate, and per-request handling for one
// decoded request, redirecting non-leaders with checked endpoints.
func (service *Service) dispatch(ctx context.Context, message protocol.ControlMessage) protocol.ControlMessage {
	if !service.voter {
		return service.staticRedirect()
	}
	select {
	case <-service.raft.Ready():
	default:
		return requestBoundError(message, protocol.ControlErrorStarting, true, "consensus is not ready")
	}
	if _, err := service.raft.Barrier(ctx); err != nil {
		if redirect, ok := service.redirectForRaftError(err); ok {
			return redirect
		}
		return requestBoundError(message, protocol.ControlErrorStarting, true, "leader barrier failed")
	}
	gateEpoch, open := service.gate.AdmissionEpoch()
	if !open {
		return requestBoundError(message, protocol.ControlErrorStarting, true, "admission gate is closed")
	}
	switch request := message.(type) {
	case protocol.SubmitRequest:
		return service.handleSubmit(ctx, request)
	case protocol.CancelRequest:
		return service.handleCancel(ctx, request)
	case protocol.StatusRequest:
		return service.handleStatus(request, gateEpoch)
	case protocol.JobListRequest:
		return service.handleJobList(request, gateEpoch)
	case protocol.ResultPageRequest:
		return service.handleResultPage(ctx, request, gateEpoch)
	default:
		return nil
	}
}

// redirectForRaftError converts a typed non-leader rejection into a checked
// redirect: the hinted leader's derived endpoint or the static voter set.
func (service *Service) redirectForRaftError(err error) (protocol.ControlMessage, bool) {
	var notLeader *raft.NotLeaderError
	if errors.As(err, &notLeader) {
		return service.leaderRedirect(notLeader.LeaderID), true
	}
	if errors.Is(err, raft.ErrNotLeader) {
		return service.staticRedirect(), true
	}
	return nil, false
}

// handleSubmit canonicalizes and proposes one client submission under the
// current committed coordinator fence, rejecting over-bound commands before
// any proposal.
func (service *Service) handleSubmit(ctx context.Context, request protocol.SubmitRequest) protocol.ControlMessage {
	view := service.machine.View()
	if view.CoordinatorEpoch == (model.CoordinatorEpoch{}) {
		return requestBoundError(request, protocol.ControlErrorStarting, true, "no committed coordinator epoch")
	}
	command, err := state.NewSubmitJob(request.Request, request.Topology, view.CoordinatorEpoch)
	if err != nil {
		return protocol.ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, Code: protocol.ControlErrorInvalidRequest, Detail: []byte("submit command rejected")}
	}
	encoded, err := state.MarshalCommand(command)
	if err != nil || uint64(len(encoded)) > service.maxCommand.Load() {
		return protocol.ControlError{RelatedMessage: wire.MessageCraneSubmitRequest, Code: protocol.ControlErrorInvalidRequest, Detail: []byte("submit command exceeds the replicated command bound")}
	}
	result, rejection := service.proposeClientCommand(ctx, request, encoded)
	if rejection != nil {
		return rejection
	}
	switch result.Code {
	case state.ResultSuccess:
		response := protocol.SubmitResponse{Request: request.Request, Digest: request.Digest, JobID: command.JobID(), JobControlRevision: result.Revision, State: protocol.JobPending}
		if protocol.ValidateSubmitResponseCorrelation(request, response) != nil {
			return nil
		}
		service.wake()
		return response
	default:
		return service.clientRejection(request, result)
	}
}

// handleCancel proposes one exact-revision cancellation and maps the durable
// result onto the typed public matrix.
func (service *Service) handleCancel(ctx context.Context, request protocol.CancelRequest) protocol.ControlMessage {
	view := service.machine.View()
	if view.CoordinatorEpoch == (model.CoordinatorEpoch{}) {
		return requestBoundError(request, protocol.ControlErrorStarting, true, "no committed coordinator epoch")
	}
	command, err := state.NewCancelJob(request.Request, request.JobID, request.ExpectedJobControlRevision, view.CoordinatorEpoch)
	if err != nil {
		return protocol.ControlError{RelatedMessage: wire.MessageCraneCancelRequest, Code: protocol.ControlErrorInvalidRequest, Detail: []byte("cancel command rejected")}
	}
	encoded, err := state.MarshalCommand(command)
	if err != nil || uint64(len(encoded)) > service.maxCommand.Load() {
		return protocol.ControlError{RelatedMessage: wire.MessageCraneCancelRequest, Code: protocol.ControlErrorInvalidRequest, Detail: []byte("cancel command exceeds the replicated command bound")}
	}
	result, rejection := service.proposeClientCommand(ctx, request, encoded)
	if rejection != nil {
		return rejection
	}
	switch result.Code {
	case state.ResultSuccess:
		response := protocol.CancelResponse{Request: request.Request, Digest: request.Digest, JobID: request.JobID, JobControlRevision: result.Revision, State: protocol.JobCanceled}
		if protocol.ValidateCancelResponseCorrelation(request, response) != nil {
			return nil
		}
		service.wake()
		return response
	case state.ResultRevisionMismatch:
		if _, ok := viewJob(service.machine.View(), request.JobID); !ok {
			return requestBoundError(request, protocol.ControlErrorNotFound, false, "unknown retained job")
		}
		return requestBoundError(request, protocol.ControlErrorRevisionMismatch, false, "stale expected job-control revision")
	default:
		return service.clientRejection(request, result)
	}
}

// proposeClientCommand enters the shared admission gate for the proposal and
// decodes the exact durable result. A non-nil second result is the complete
// typed response to send instead.
func (service *Service) proposeClientCommand(ctx context.Context, request protocol.ControlMessage, encoded []byte) (state.CommandResult, protocol.ControlMessage) {
	exit, err := service.gate.Enter()
	if err != nil {
		return state.CommandResult{}, requestBoundError(request, protocol.ControlErrorStarting, true, "admission gate is closed")
	}
	defer exit()
	proposal, err := service.raft.Propose(ctx, encoded)
	if err != nil {
		if redirect, ok := service.redirectForRaftError(err); ok {
			return state.CommandResult{}, redirect
		}
		if errors.Is(err, raft.ErrOverloaded) {
			return state.CommandResult{}, requestBoundError(request, protocol.ControlErrorCapacityExhausted, true, "consensus is overloaded")
		}
		return state.CommandResult{}, requestBoundError(request, protocol.ControlErrorStarting, true, "proposal failed")
	}
	result, err := state.UnmarshalCommandResult(proposal.Result)
	if err != nil {
		return state.CommandResult{}, nil
	}
	return result, nil
}

// clientRejection maps the shared deterministic client-dedup rejections onto
// their fingerprinted public error codes.
func (service *Service) clientRejection(request protocol.ControlMessage, result state.CommandResult) protocol.ControlMessage {
	switch result.Code {
	case state.ResultIdentityReuse:
		return requestBoundError(request, protocol.ControlErrorIdentityReuse, false, "request identity was reused with changed bytes")
	case state.ResultStaleRequest:
		return requestBoundError(request, protocol.ControlErrorStaleRequest, false, "client sequence is older than the durable history")
	case state.ResultSkippedRequest:
		return requestBoundError(request, protocol.ControlErrorSkippedRequest, false, "client sequence skips the durable history")
	case state.ResultCapacityExhausted:
		return requestBoundError(request, protocol.ControlErrorCapacityExhausted, true, "replicated capacity exhausted")
	case state.ResultResultTooLarge:
		// The machine consumed the sequence but could not cache the outcome;
		// the client must resolve the reservation instead of resuming forever.
		return requestBoundError(request, protocol.ControlErrorResultTooLarge, false, "durable command result exceeds the replicated cache bound")
	case state.ResultStaleEpoch:
		// The stamped committed fence lost to a newer epoch mid-request; the
		// unchanged retry observes the newer fence.
		return requestBoundError(request, protocol.ControlErrorStarting, true, "coordinator fence advanced during the request")
	default:
		return nil
	}
}

// handleJobList performs the atomic post-barrier view read of every retained
// job summary and aborts instead of answering when the admission gate was
// lost during the read.
func (service *Service) handleJobList(request protocol.JobListRequest, gateEpoch model.CoordinatorEpoch) protocol.ControlMessage {
	view := service.machine.View()
	jobs := make([]protocol.StatusResponse, 0, len(view.Jobs))
	for _, record := range view.Jobs {
		summary, err := buildStatusResponse(view, record)
		if err != nil {
			return nil
		}
		jobs = append(jobs, summary)
	}
	if !service.gateStillOpen(gateEpoch) {
		return requestBoundError(request, protocol.ControlErrorStarting, true, "admission gate lost during the read")
	}
	return protocol.JobListResponse{LeaderNodeID: service.configuration.NodeID, AppliedIndex: view.AppliedIndex, Jobs: jobs}
}

// handleStatus performs the atomic post-barrier view read and aborts instead
// of answering when the admission gate was lost during the read.
func (service *Service) handleStatus(request protocol.StatusRequest, gateEpoch model.CoordinatorEpoch) protocol.ControlMessage {
	view := service.machine.View()
	record, ok := viewJob(view, request.JobID)
	if !ok {
		return requestBoundError(request, protocol.ControlErrorNotFound, false, "unknown retained job")
	}
	response, err := buildStatusResponse(view, record)
	if err != nil {
		return nil
	}
	if !service.gateStillOpen(gateEpoch) {
		return requestBoundError(request, protocol.ControlErrorStarting, true, "admission gate lost during the read")
	}
	return response
}

// handleResultPage serves one linearizable manifest-bound global result page
// and aborts instead of answering when the admission gate was lost.
func (service *Service) handleResultPage(ctx context.Context, request protocol.ResultPageRequest, gateEpoch model.CoordinatorEpoch) protocol.ControlMessage {
	if service.results.Fetcher == nil {
		return requestBoundError(request, protocol.ControlErrorResultUnavailable, true, "result transfer is not composed")
	}
	page, err := service.results.Page(ctx, request)
	if err != nil {
		return service.resultPageRejection(request, err)
	}
	if !service.gateStillOpen(gateEpoch) {
		return requestBoundError(request, protocol.ControlErrorStarting, true, "admission gate lost during the read")
	}
	return page
}

// resultPageRejection maps query-engine failures onto the typed page matrix.
func (service *Service) resultPageRejection(request protocol.ResultPageRequest, err error) protocol.ControlMessage {
	var tooSmall PageLimitTooSmallError
	switch {
	case errors.As(err, &tooSmall):
		response, ok := requestBoundError(request, protocol.ControlErrorPageLimitTooSmall, false, "first complete record exceeds the page budget").(protocol.ControlError)
		if !ok {
			return nil
		}
		response.RequiredBytes = tooSmall.RequiredBytes
		return response
	case errors.Is(err, ErrInvalidResultPage):
		return protocol.ControlError{RelatedMessage: wire.MessageCraneResultPageRequest, Code: protocol.ControlErrorInvalidRequest, Detail: []byte("invalid result page binding")}
	case errors.Is(err, ErrCorruptResultSet):
		return requestBoundError(request, protocol.ControlErrorCorruptResult, false, "committed result set is corrupt")
	case errors.Is(err, ErrResultQueryUnavailable):
		if _, ok := viewJob(service.machine.View(), request.JobID); !ok {
			return requestBoundError(request, protocol.ControlErrorNotFound, false, "unknown retained job")
		}
		return requestBoundError(request, protocol.ControlErrorResultUnavailable, true, "no complete current manifest set matches the request")
	default:
		detail := "result stream failed: " + err.Error()
		if len(detail) > 120 {
			detail = detail[:120]
		}
		return requestBoundError(request, protocol.ControlErrorResultUnavailable, true, detail)
	}
}

// gateStillOpen reports whether the shared gate still admits the exact epoch
// observed when the request began.
func (service *Service) gateStillOpen(epoch model.CoordinatorEpoch) bool {
	current, open := service.gate.AdmissionEpoch()
	return open && current == epoch
}

// buildStatusResponse derives the complete bounded public summary from one
// atomic view record.
func buildStatusResponse(view state.View, record state.JobRecord) (protocol.StatusResponse, error) {
	topology, err := model.DecodeTopology(record.TopologyBytes)
	if err != nil {
		return protocol.StatusResponse{}, err
	}
	spec := topology.Spec()
	sourceTotal, sinkTotal, completed := 0, 0, 0
	for _, stage := range spec.Stages {
		switch stage.Role {
		case model.StageSource:
			sourceTotal += int(stage.Parallelism)
			for partition := uint16(0); partition < stage.Parallelism; partition++ {
				task := model.TaskID{JobID: record.JobID, StageID: stage.StageID, Partition: partition}
				eof, ok := record.SourceEOFs[task]
				if !ok {
					continue
				}
				if eof.EOF == 0 {
					// An empty source is trivially finally checkpointed.
					completed++
					continue
				}
				if checkpoint, ok := record.Checkpoints[task]; ok && checkpoint.Watermark == eof.EOF {
					completed++
				}
			}
		case model.StageSink:
			sinkTotal += int(stage.Parallelism)
		}
	}
	if sourceTotal == 0 || sourceTotal > 65535 || sinkTotal == 0 || sinkTotal > 65535 {
		return protocol.StatusResponse{}, errors.New("status counts outside public bounds")
	}
	response := protocol.StatusResponse{
		JobID: record.JobID, AppliedIndex: view.AppliedIndex, TopologyDigest: record.TopologyDigest,
		JobControlRevision: record.JobControlRevision, State: publicJobState(record.Lifecycle),
		SourceTaskCount: uint16(sourceTotal), ResultPartitionCount: uint16(sinkTotal), CompletedSourceTasks: uint16(completed),
	}
	if record.Assignment != nil {
		response.HasAssignment = true
		response.AssignmentRevision = record.Assignment.Revision
		response.AssignmentDigest = record.Assignment.Digest
	}
	if len(record.Manifests) > 0 {
		manifests := make([]state.ResultManifest, 0, len(record.Manifests))
		for _, manifest := range record.Manifests {
			manifests = append(manifests, manifest)
		}
		response.ManifestCount = uint16(len(manifests))
		response.HasManifestSet = true
		response.ManifestSetDigest = ResultManifestSetDigest(manifests)
	}
	if record.Failure != nil {
		response.HasFailure = true
		response.FailureCode = record.Failure.Code
		response.FailureDetailDigest = record.Failure.DetailDigest
	}
	return response, nil
}

// publicJobState maps the replicated lifecycle onto the stable public domain.
func publicJobState(lifecycle state.JobLifecycle) protocol.JobState {
	switch lifecycle {
	case state.JobPending:
		return protocol.JobPending
	case state.JobDeploying:
		return protocol.JobDeploying
	case state.JobRunning:
		return protocol.JobRunning
	case state.JobDraining:
		return protocol.JobDraining
	case state.JobSucceeded:
		return protocol.JobSucceeded
	case state.JobFailed:
		return protocol.JobFailed
	case state.JobCanceled:
		return protocol.JobCanceled
	default:
		return 0
	}
}

// predecodeErrorResponse classifies undecodable payloads with the unbound
// predecode error codes.
func predecodeErrorResponse(related wire.MessageType, err error) protocol.ControlMessage {
	code := protocol.ControlErrorInvalidRequest
	switch {
	case errors.Is(err, protocol.ErrMalformedControlMessage), errors.Is(err, protocol.ErrControlMessageTooLarge), errors.Is(err, protocol.ErrUnexpectedControlMessage):
		code = protocol.ControlErrorMalformed
	case errors.Is(err, protocol.ErrUnsupportedControlSchema):
		code = protocol.ControlErrorUnsupportedSchema
	}
	return protocol.ControlError{RelatedMessage: related, Code: code, Detail: []byte("request rejected before decode")}
}

// requestBoundError builds one typed error carrying the exact request binding
// of the rejected message.
func requestBoundError(message protocol.ControlMessage, code protocol.ControlErrorCode, retryable bool, detail string) protocol.ControlMessage {
	controlError := protocol.ControlError{Code: code, Retryable: retryable, Detail: []byte(detail)}
	switch request := message.(type) {
	case protocol.SubmitRequest:
		controlError.RelatedMessage = wire.MessageCraneSubmitRequest
		controlError.HasClientRequest = true
		controlError.ClientRequest = request.Request
		controlError.ClientDigest = request.Digest
	case protocol.CancelRequest:
		controlError.RelatedMessage = wire.MessageCraneCancelRequest
		controlError.HasClientRequest = true
		controlError.ClientRequest = request.Request
		controlError.ClientDigest = request.Digest
	case protocol.StatusRequest:
		controlError.RelatedMessage = wire.MessageCraneStatusRequest
		controlError.HasStatusRequest = true
		controlError.StatusJobID = request.JobID
	case protocol.JobListRequest:
		controlError.RelatedMessage = wire.MessageCraneJobListRequest
	case protocol.ResultPageRequest:
		controlError.RelatedMessage = wire.MessageCraneResultPageRequest
		controlError.HasResultPage = true
		controlError.ResultPage = request
	default:
		return nil
	}
	return controlError
}
