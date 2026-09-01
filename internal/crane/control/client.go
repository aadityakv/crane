package control

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/aaditya/cs425mp3/internal/clock"
	"github.com/aaditya/cs425mp3/internal/config"
	"github.com/aaditya/cs425mp3/internal/crane/clientstate"
	"github.com/aaditya/cs425mp3/internal/crane/model"
	"github.com/aaditya/cs425mp3/internal/crane/protocol"
	"github.com/aaditya/cs425mp3/internal/wire"
)

const (
	// DefaultClientMaxAttempts bounds complete network exchanges per request.
	DefaultClientMaxAttempts = 8
	// DefaultClientMaxRedirects bounds followed redirects per request.
	DefaultClientMaxRedirects = 4
	// DefaultClientRetryBackoff separates retry attempts of one request.
	DefaultClientRetryBackoff = 200 * time.Millisecond
)

var (
	// ErrClientAttemptsExhausted reports a request whose bounded attempt
	// budget ran out; a durable reservation stays pending for resumption.
	ErrClientAttemptsExhausted = errors.New("Crane client attempts exhausted")
	// ErrClientRedirectLoop reports redirects that only name endpoints this
	// request already tried.
	ErrClientRedirectLoop = errors.New("Crane client redirect loop")
	// ErrClientRedirectUntrusted reports a redirect naming an endpoint outside
	// the configured static voter control set.
	ErrClientRedirectUntrusted = errors.New("Crane client redirect names an unconfigured endpoint")
	// ErrClientIdentityForfeited reports that the durable client state no
	// longer matches the cluster's replicated dedup history, so the prior
	// dedup identity is forfeit; the local state is preserved for inspection.
	ErrClientIdentityForfeited = errors.New("Crane client dedup identity forfeited by lost or rolled-back state")
	// ErrClientStoreRequired reports a mutation attempted without a durable
	// client identity store.
	ErrClientStoreRequired = errors.New("Crane client mutations require a durable identity store")
)

// errClientRetryExchange marks one unusable attempt inside the bounded loop.
var errClientRetryExchange = errors.New("retry Crane client exchange")

// RequestRejectedError is one typed terminal +6 rejection of a client request.
type RequestRejectedError struct {
	// Code is the stable fingerprinted rejection category.
	Code protocol.ControlErrorCode
	// Retryable repeats the server's advice for unchanged external state.
	Retryable bool
	// RequiredBytes is nonzero only for PageLimitTooSmall rejections.
	RequiredBytes uint32
	// Detail is the bounded server-provided rejection text.
	Detail string
}

// Error renders the stable rejection category and detail.
func (rejection *RequestRejectedError) Error() string {
	return fmt.Sprintf("Crane control request rejected: code %d: %s", rejection.Code, rejection.Detail)
}

// ClientOptions fixes every dependency of one durable Crane control client.
type ClientOptions struct {
	// Config is the complete validated local member configuration; the
	// client authenticates as its NodeID and derives the static voter +6
	// endpoint set from its RaftVoters.
	Config config.NodeConfig
	// Authenticator signs and verifies every +6 frame with the cluster secret.
	Authenticator wire.Authenticator
	// Clock supplies frame timestamps and retry backoff timers.
	Clock clock.Clock
	// Store is the durable client identity; it may be nil for a read-only
	// client, which then refuses Submit and Cancel.
	Store *clientstate.ClientStore
	// MaxAttempts bounds complete network exchanges per request; zero selects
	// DefaultClientMaxAttempts.
	MaxAttempts int
	// MaxRedirects bounds followed redirects per request; zero selects
	// DefaultClientMaxRedirects.
	MaxRedirects int
	// RetryBackoff separates retry attempts; zero disables waiting and a
	// negative value is invalid.
	RetryBackoff time.Duration
	// RequestTimeout bounds one dial-to-response exchange; zero selects the
	// configured Crane worker-control timeout.
	RequestTimeout time.Duration
	// Dial optionally replaces the TCP dialer for one canonical endpoint.
	Dial func(ctx context.Context, address string) (net.Conn, error)
}

// Client performs crash-safe, exactly-correlated Crane public-control requests
// against the configured static voter +6 endpoints.
type Client struct {
	authenticator wire.Authenticator
	clock         clock.Clock
	store         *clientstate.ClientStore
	clusterID     [16]byte
	senderID      uint16
	endpoints     []string
	endpointSet   map[string]struct{}
	maxAttempts   int
	maxRedirects  int
	backoff       time.Duration
	timeout       time.Duration
	dial          func(ctx context.Context, address string) (net.Conn, error)
}

// NewClient validates the complete client composition without opening any
// network resource.
func NewClient(options ClientOptions) (*Client, error) {
	if options.Authenticator == nil || options.Clock == nil {
		return nil, errors.New("Crane client requires authenticator and clock")
	}
	if options.MaxAttempts < 0 || options.MaxRedirects < 0 || options.RetryBackoff < 0 || options.RequestTimeout < 0 {
		return nil, errors.New("Crane client bounds must not be negative")
	}
	configuration := cloneControlNodeConfig(options.Config)
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("validate Crane client configuration: %w", err)
	}
	clusterID, err := decodeControlClusterID(configuration.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("decode Crane cluster ID: %w", err)
	}
	if options.Store != nil {
		if bound := options.Store.State().ClusterID; bound != clusterID {
			return nil, fmt.Errorf("Crane client store binds cluster %x, configuration names %x", bound, clusterID)
		}
	}
	endpoints, err := deriveVoterControlEndpoints(configuration.RaftVoters)
	if err != nil {
		return nil, err
	}
	endpointSet := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		endpointSet[endpoint] = struct{}{}
	}
	client := &Client{
		authenticator: options.Authenticator, clock: options.Clock, store: options.Store,
		clusterID: clusterID, senderID: configuration.NodeID,
		endpoints: endpoints, endpointSet: endpointSet,
		maxAttempts: options.MaxAttempts, maxRedirects: options.MaxRedirects,
		backoff: options.RetryBackoff, timeout: options.RequestTimeout, dial: options.Dial,
	}
	if client.maxAttempts == 0 {
		client.maxAttempts = DefaultClientMaxAttempts
	}
	if client.maxRedirects == 0 {
		client.maxRedirects = DefaultClientMaxRedirects
	}
	if client.timeout == 0 {
		client.timeout = time.Duration(configuration.Crane.WorkerControlTimeout)
	}
	if client.dial == nil {
		dialer := &net.Dialer{}
		client.dial = func(ctx context.Context, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		}
	}
	return client, nil
}

// Submit durably reserves, sends, and resolves one topology submission,
// resuming any pending request first. Re-running the exact unresolved
// submission resumes its reserved sequence instead of creating a second job.
func (client *Client) Submit(ctx context.Context, topology model.TopologySpec) (model.JobID, error) {
	if client.store == nil {
		return model.JobID{}, ErrClientStoreRequired
	}
	validated, err := model.ValidateTopology(topology)
	if err != nil {
		return model.JobID{}, fmt.Errorf("validate submit topology: %w", err)
	}
	resumed, err := client.resumePendingMatch(ctx, func(pending protocol.ControlMessage) bool {
		request, ok := pending.(protocol.SubmitRequest)
		if !ok {
			return false
		}
		pendingValidated, validateErr := model.ValidateTopology(request.Topology)
		return validateErr == nil && pendingValidated.Digest() == validated.Digest()
	})
	if err != nil {
		return model.JobID{}, err
	}
	if resumed != nil {
		response, ok := resumed.(protocol.SubmitResponse)
		if !ok {
			return model.JobID{}, fmt.Errorf("pending submit resolved to unexpected %T", resumed)
		}
		return response.JobID, nil
	}

	request := protocol.SubmitRequest{Request: client.store.NextRequestID(), Topology: topology}
	request.Digest, err = protocol.SubmitCommandDigest(request.Request, topology)
	if err != nil {
		return model.JobID{}, fmt.Errorf("derive submit digest: %w", err)
	}
	payload, err := protocol.MarshalControlMessage(request)
	if err != nil {
		return model.JobID{}, fmt.Errorf("marshal submit request: %w", err)
	}
	if _, _, err := client.store.Begin(payload); err != nil {
		return model.JobID{}, fmt.Errorf("reserve submit request: %w", err)
	}
	resolution, err := client.resolveMutation(ctx, payload)
	if err != nil {
		return model.JobID{}, err
	}
	response, ok := resolution.(protocol.SubmitResponse)
	if !ok {
		return model.JobID{}, fmt.Errorf("submit resolved to unexpected %T", resolution)
	}
	return response.JobID, nil
}

// Cancel durably reserves, sends, and resolves one exact-revision cancel,
// resuming any pending request first. Deterministically consumed rejections
// (NotFound, RevisionMismatch) advance the durable sequence and surface as a
// typed RequestRejectedError.
func (client *Client) Cancel(ctx context.Context, job model.JobID, expectedRevision uint64) (uint64, error) {
	if client.store == nil {
		return 0, ErrClientStoreRequired
	}
	resumed, err := client.resumePendingMatch(ctx, func(pending protocol.ControlMessage) bool {
		request, ok := pending.(protocol.CancelRequest)
		return ok && request.JobID == job && request.ExpectedJobControlRevision == expectedRevision
	})
	if err != nil {
		return 0, err
	}
	if resumed == nil {
		request := protocol.CancelRequest{Request: client.store.NextRequestID(), JobID: job, ExpectedJobControlRevision: expectedRevision}
		request.Digest, err = protocol.CancelCommandDigest(request.Request, job, expectedRevision)
		if err != nil {
			return 0, fmt.Errorf("derive cancel digest: %w", err)
		}
		payload, marshalErr := protocol.MarshalControlMessage(request)
		if marshalErr != nil {
			return 0, fmt.Errorf("marshal cancel request: %w", marshalErr)
		}
		if _, _, beginErr := client.store.Begin(payload); beginErr != nil {
			return 0, fmt.Errorf("reserve cancel request: %w", beginErr)
		}
		resumed, err = client.resolveMutation(ctx, payload)
		if err != nil {
			return 0, err
		}
	}
	switch response := resumed.(type) {
	case protocol.CancelResponse:
		return response.JobControlRevision, nil
	case protocol.ControlError:
		return 0, rejectionError(response)
	default:
		return 0, fmt.Errorf("cancel resolved to unexpected %T", resumed)
	}
}

// ResumePending drives any durable unresolved request to resolution and
// reports whether one existed.
func (client *Client) ResumePending(ctx context.Context) (bool, error) {
	if client.store == nil {
		return false, ErrClientStoreRequired
	}
	pending := client.store.State().Pending
	if len(pending) == 0 {
		return false, nil
	}
	if _, err := client.resolveMutation(ctx, pending); err != nil {
		return true, err
	}
	return true, nil
}

// Status performs one linearizable retained-job status read.
func (client *Client) Status(ctx context.Context, job model.JobID) (protocol.StatusResponse, error) {
	request := protocol.StatusRequest{JobID: job}
	payload, err := protocol.MarshalControlMessage(request)
	if err != nil {
		return protocol.StatusResponse{}, fmt.Errorf("marshal status request: %w", err)
	}
	response, err := client.exchange(ctx, request.MessageType(), payload, client.statusAccept(request))
	if err != nil {
		return protocol.StatusResponse{}, err
	}
	return response.(protocol.StatusResponse), nil
}

// ResultPage performs one exactly bound stateless global result-page read.
func (client *Client) ResultPage(ctx context.Context, request protocol.ResultPageRequest) (protocol.ResultPageResponse, error) {
	payload, err := protocol.MarshalControlMessage(request)
	if err != nil {
		return protocol.ResultPageResponse{}, fmt.Errorf("marshal result-page request: %w", err)
	}
	response, err := client.exchange(ctx, request.MessageType(), payload, client.resultPageAccept(request))
	if err != nil {
		return protocol.ResultPageResponse{}, err
	}
	return response.(protocol.ResultPageResponse), nil
}

// resumePendingMatch resolves any pending request first. When the pending
// request matches the caller's intended command, its resolution is returned so
// the caller does not issue a duplicate; otherwise nil is returned after the
// old request resolves and the caller proceeds with a fresh reservation.
func (client *Client) resumePendingMatch(ctx context.Context, matches func(protocol.ControlMessage) bool) (protocol.ControlMessage, error) {
	pending := client.store.State().Pending
	if len(pending) == 0 {
		return nil, nil
	}
	request, err := decodePendingRequest(pending)
	if err != nil {
		return nil, err
	}
	resolution, err := client.resolveMutation(ctx, pending)
	if err != nil {
		return nil, fmt.Errorf("resolve pending request before a new command: %w", err)
	}
	if matches(request) {
		return resolution, nil
	}
	return nil, nil
}

// resolveMutation retries the exact reserved bytes until a durably correlated
// resolution exists, records it, and returns the decoded resolution. Every
// error before resolution keeps the reservation pending for resumption.
func (client *Client) resolveMutation(ctx context.Context, pending []byte) (protocol.ControlMessage, error) {
	request, err := decodePendingRequest(pending)
	if err != nil {
		return nil, err
	}
	var accept func(protocol.ControlMessage) error
	switch typed := request.(type) {
	case protocol.SubmitRequest:
		accept = client.submitAccept(typed)
	case protocol.CancelRequest:
		accept = client.cancelAccept(typed)
	default:
		return nil, fmt.Errorf("pending request has unexpected type %T", request)
	}
	response, err := client.exchange(ctx, request.MessageType(), pending, accept)
	if err != nil {
		return nil, err
	}
	resolution, err := protocol.MarshalControlMessage(response)
	if err != nil {
		return nil, fmt.Errorf("marshal durable resolution: %w", err)
	}
	if err := client.store.Resolve(resolution); err != nil {
		return nil, fmt.Errorf("record durable resolution: %w", err)
	}
	return response, nil
}

// decodePendingRequest decodes one reserved payload through its embedded
// canonical message type.
func decodePendingRequest(pending []byte) (protocol.ControlMessage, error) {
	if len(pending) < 4 {
		return nil, errors.New("pending request payload is truncated")
	}
	messageType := wire.MessageType(binary.BigEndian.Uint16(pending[2:4]))
	request, err := protocol.UnmarshalControlMessage(messageType, pending)
	if err != nil {
		return nil, fmt.Errorf("decode pending request: %w", err)
	}
	return request, nil
}

// submitAccept classifies one response to an exact submit request. Submit has
// no deterministically consumed rejection, so only the validated correlated
// response completes the exchange.
func (client *Client) submitAccept(request protocol.SubmitRequest) func(protocol.ControlMessage) error {
	return func(response protocol.ControlMessage) error {
		switch typed := response.(type) {
		case protocol.SubmitResponse:
			if err := protocol.ValidateSubmitResponseCorrelation(request, typed); err != nil {
				return fmt.Errorf("%w: %v", errClientRetryExchange, err)
			}
			return nil
		case protocol.ControlError:
			if protocol.ValidateSubmitErrorCorrelation(request, typed) != nil {
				return classifyUncorrelatedError(typed)
			}
			return classifyMutationError(typed, nil)
		default:
			return fmt.Errorf("%w: unexpected %T", errClientRetryExchange, response)
		}
	}
}

// cancelAccept classifies one response to an exact cancel request. NotFound
// and RevisionMismatch are deterministically consumed by the replicated dedup
// history, so they complete the exchange and must be durably resolved.
func (client *Client) cancelAccept(request protocol.CancelRequest) func(protocol.ControlMessage) error {
	consumed := map[protocol.ControlErrorCode]bool{
		protocol.ControlErrorNotFound:         true,
		protocol.ControlErrorRevisionMismatch: true,
	}
	return func(response protocol.ControlMessage) error {
		switch typed := response.(type) {
		case protocol.CancelResponse:
			if err := protocol.ValidateCancelResponseCorrelation(request, typed); err != nil {
				return fmt.Errorf("%w: %v", errClientRetryExchange, err)
			}
			return nil
		case protocol.ControlError:
			if protocol.ValidateCancelErrorCorrelation(request, typed) != nil {
				return classifyUncorrelatedError(typed)
			}
			return classifyMutationError(typed, consumed)
		default:
			return fmt.Errorf("%w: unexpected %T", errClientRetryExchange, response)
		}
	}
}

// statusAccept classifies one response to an exact status request.
func (client *Client) statusAccept(request protocol.StatusRequest) func(protocol.ControlMessage) error {
	return func(response protocol.ControlMessage) error {
		switch typed := response.(type) {
		case protocol.StatusResponse:
			if typed.JobID != request.JobID {
				return fmt.Errorf("%w: status names foreign job", errClientRetryExchange)
			}
			return nil
		case protocol.ControlError:
			if protocol.ValidateStatusErrorCorrelation(request, typed) != nil {
				return classifyUncorrelatedError(typed)
			}
			if typed.Retryable {
				return fmt.Errorf("%w: %s", errClientRetryExchange, typed.Detail)
			}
			return rejectionError(typed)
		default:
			return fmt.Errorf("%w: unexpected %T", errClientRetryExchange, response)
		}
	}
}

// resultPageAccept classifies one response to an exact result-page request.
func (client *Client) resultPageAccept(request protocol.ResultPageRequest) func(protocol.ControlMessage) error {
	return func(response protocol.ControlMessage) error {
		switch typed := response.(type) {
		case protocol.ResultPageResponse:
			if err := protocol.ValidateResultPageResponseCorrelation(request, typed); err != nil {
				return fmt.Errorf("%w: %v", errClientRetryExchange, err)
			}
			return nil
		case protocol.ControlError:
			if protocol.ValidateResultPageErrorCorrelation(request, typed) != nil {
				return classifyUncorrelatedError(typed)
			}
			if typed.Retryable {
				return fmt.Errorf("%w: %s", errClientRetryExchange, typed.Detail)
			}
			return rejectionError(typed)
		default:
			return fmt.Errorf("%w: unexpected %T", errClientRetryExchange, response)
		}
	}
}

// classifyMutationError maps one correlated mutation rejection: retryable
// codes retry the exact bytes, forfeited-identity codes fail closed, consumed
// codes complete the exchange for durable resolution, and anything else is a
// terminal typed rejection that keeps the reservation.
func classifyMutationError(controlError protocol.ControlError, consumed map[protocol.ControlErrorCode]bool) error {
	if controlError.Retryable {
		return fmt.Errorf("%w: %s", errClientRetryExchange, controlError.Detail)
	}
	switch controlError.Code {
	case protocol.ControlErrorStaleRequest, protocol.ControlErrorSkippedRequest, protocol.ControlErrorIdentityReuse:
		return fmt.Errorf("%w: code %d: %s", ErrClientIdentityForfeited, controlError.Code, controlError.Detail)
	}
	if consumed[controlError.Code] {
		return nil
	}
	return rejectionError(controlError)
}

// classifyUncorrelatedError handles a typed error that does not bind this
// exact request: unbound predecode rejections are terminal, anything else is
// an unusable response worth one bounded retry.
func classifyUncorrelatedError(controlError protocol.ControlError) error {
	switch controlError.Code {
	case protocol.ControlErrorMalformed, protocol.ControlErrorUnsupportedSchema, protocol.ControlErrorInvalidRequest:
		if !controlError.HasClientRequest && !controlError.HasStatusRequest && !controlError.HasResultPage {
			return rejectionError(controlError)
		}
	}
	return fmt.Errorf("%w: error does not bind this request", errClientRetryExchange)
}

// rejectionError builds the typed terminal rejection for one control error.
func rejectionError(controlError protocol.ControlError) *RequestRejectedError {
	return &RequestRejectedError{
		Code: controlError.Code, Retryable: controlError.Retryable,
		RequiredBytes: controlError.RequiredBytes, Detail: string(controlError.Detail),
	}
}

// exchange performs bounded attempts of one request against the static voter
// endpoints, following checked redirects, until accept completes with the
// exact correlated response, accept fails terminally, or the budget runs out.
// Every attempt uses a fresh RequestID with the exact same payload bytes.
func (client *Client) exchange(ctx context.Context, messageType wire.MessageType, payload []byte, accept func(protocol.ControlMessage) error) (protocol.ControlMessage, error) {
	if ctx == nil {
		return nil, errors.New("Crane client exchange requires a context")
	}
	targets := client.endpoints
	visited := make(map[string]bool, len(client.endpoints))
	index, redirects := 0, 0
	var lastErr error
	for attempts := 0; attempts < client.maxAttempts; attempts++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target := targets[index%len(targets)]
		index++
		visited[target] = true
		response, err := client.exchangeOnce(ctx, target, messageType, payload)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			if waitErr := client.waitBackoff(ctx); waitErr != nil {
				return nil, waitErr
			}
			continue
		}
		if redirect, ok := response.(protocol.LeaderRedirect); ok {
			redirects++
			if redirects > client.maxRedirects {
				return nil, fmt.Errorf("%w: %d redirects exceed the bound", ErrClientRedirectLoop, redirects)
			}
			next := make([]string, 0, len(redirect.Endpoints))
			for _, endpoint := range redirect.Endpoints {
				if _, trusted := client.endpointSet[endpoint]; !trusted {
					return nil, fmt.Errorf("%w: %q", ErrClientRedirectUntrusted, endpoint)
				}
				if !visited[endpoint] {
					next = append(next, endpoint)
				}
			}
			if len(next) == 0 {
				return nil, fmt.Errorf("%w: every redirected endpoint was already tried", ErrClientRedirectLoop)
			}
			targets, index = next, 0
			continue
		}
		acceptErr := accept(response)
		switch {
		case acceptErr == nil:
			return response, nil
		case errors.Is(acceptErr, errClientRetryExchange):
			lastErr = acceptErr
			if waitErr := client.waitBackoff(ctx); waitErr != nil {
				return nil, waitErr
			}
		default:
			return nil, acceptErr
		}
	}
	return nil, fmt.Errorf("%w: %d attempts: %v", ErrClientAttemptsExhausted, client.maxAttempts, lastErr)
}

// exchangeOnce performs one complete dial, authenticated request frame,
// correlated response frame, and canonical decode.
func (client *Client) exchangeOnce(ctx context.Context, target string, messageType wire.MessageType, payload []byte) (protocol.ControlMessage, error) {
	requestID, err := newClientRequestFrameID()
	if err != nil {
		return nil, err
	}
	exchangeCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	connection, err := client.dial(exchangeCtx, target)
	if err != nil {
		return nil, fmt.Errorf("dial Crane control %q: %w", target, err)
	}
	defer connection.Close()
	// Cancellation must interrupt a blocked read immediately, not wait for
	// the connection deadline.
	stopOnCancel := context.AfterFunc(exchangeCtx, func() { _ = connection.Close() })
	defer stopOnCancel()
	limits := wire.DefaultLimits()
	limits.MaxFrameSize = int(model.PublicControlMaxFrameBytesV1)
	limits.ExpectedClusterID = &client.clusterID
	stream := wire.NewTCPFrameStream(connection, client.authenticator, limits, client.timeout)
	outbound := wire.Frame{Header: wire.Header{
		Version: wire.Version1, Message: messageType, ClusterID: client.clusterID, SenderID: client.senderID,
		RequestID: requestID, TimestampMillis: client.clock.Now().UnixMilli(), Codec: wire.CodecBinary,
	}, Payload: payload}
	if err := stream.WriteFrame(exchangeCtx, outbound); err != nil {
		return nil, fmt.Errorf("send Crane control request: %w", err)
	}
	response, err := stream.ReadFrame(exchangeCtx)
	if err != nil {
		return nil, fmt.Errorf("receive Crane control response: %w", err)
	}
	if response.Header.RequestID != requestID {
		return nil, errors.New("Crane control response does not echo the request identity")
	}
	message, err := protocol.UnmarshalControlMessage(response.Header.Message, response.Payload)
	if err != nil {
		return nil, fmt.Errorf("decode Crane control response: %w", err)
	}
	return message, nil
}

// waitBackoff separates attempts without ever outliving the context.
func (client *Client) waitBackoff(ctx context.Context) error {
	if client.backoff <= 0 {
		return ctx.Err()
	}
	timer := client.clock.NewTimer(client.backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

// newClientRequestFrameID returns one fresh random frame correlation identity.
func newClientRequestFrameID() (wire.RequestID, error) {
	var id wire.RequestID
	if _, err := rand.Read(id[:]); err != nil {
		return wire.RequestID{}, fmt.Errorf("generate Crane request ID: %w", err)
	}
	if id == (wire.RequestID{}) {
		return wire.RequestID{}, errors.New("generated zero Crane request ID")
	}
	return id, nil
}
