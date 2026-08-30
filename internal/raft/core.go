package raft

import (
	"fmt"
	"math"

	boundedrandom "github.com/aaditya/cs425mp3/internal/random"
)

// CoreOptions contains already-validated, side-effect-free inputs for one core.
type CoreOptions struct {
	// LocalID is the configured voter identity owned by this core.
	LocalID uint16
	// Voters is the immutable fixed quorum and trust boundary.
	Voters VoterSet
	// HardState is the recovered durable term, vote, and commit index.
	HardState HardState
	// Log is the checked recovered log; construction takes an owned copy.
	Log *Log
	// AppliedIndex must exactly match the recovered checked log.
	AppliedIndex uint64
	// ElectionTimeoutMin is the inclusive timeout bound in logical ticks.
	ElectionTimeoutMin uint64
	// ElectionTimeoutMax is the exclusive timeout bound in logical ticks.
	ElectionTimeoutMax uint64
	// Random supplies deterministic uint64 samples; the core applies unbiased bounding.
	Random interface{ Uint64() uint64 }
}

// Core is one synchronous single-owner deterministic Raft state machine.
type Core struct {
	localID uint16
	voters  VoterSet

	hardState HardState
	log       *Log
	role      Role
	leaderID  uint16

	now                uint64
	electionDeadline   uint64
	electionTimeoutMin uint64
	electionTimeoutMax uint64
	random             interface{ Uint64() uint64 }
	recentLeader       bool

	preVotes map[uint16]struct{}
	votes    map[uint16]struct{}

	pendingReady Ready
	hasPending   bool
	nextToken    ReadyToken
}

// NewCore validates recovery invariants, owns the recovered log, and samples
// the initial half-open election deadline without performing external I/O.
func NewCore(options CoreOptions) (*Core, error) {
	if err := options.Voters.ValidateLocalID(options.LocalID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCoreState, err)
	}
	if options.Log == nil {
		return nil, fmt.Errorf("%w: recovered log is nil", ErrInvalidCoreState)
	}
	if options.Random == nil {
		return nil, fmt.Errorf("%w: random source is nil", ErrInvalidCoreState)
	}
	if options.ElectionTimeoutMin == 0 || options.ElectionTimeoutMin >= options.ElectionTimeoutMax {
		return nil, fmt.Errorf(
			"%w: election interval [%d,%d) is empty or contains zero",
			ErrInvalidCoreState,
			options.ElectionTimeoutMin,
			options.ElectionTimeoutMax,
		)
	}
	state := options.Log.State()
	if state.CommitIndex != options.HardState.CommitIndex {
		return nil, fmt.Errorf(
			"%w: hard-state commit=%d log commit=%d",
			ErrInvalidCoreState,
			options.HardState.CommitIndex,
			state.CommitIndex,
		)
	}
	if state.AppliedIndex != options.AppliedIndex {
		return nil, fmt.Errorf(
			"%w: recovered applied=%d log applied=%d",
			ErrInvalidCoreState,
			options.AppliedIndex,
			state.AppliedIndex,
		)
	}
	if options.HardState.VotedFor != 0 {
		if options.HardState.Term == 0 || !options.Voters.Contains(options.HardState.VotedFor) {
			return nil, fmt.Errorf(
				"%w: vote %d is invalid in term %d",
				ErrInvalidCoreState,
				options.HardState.VotedFor,
				options.HardState.Term,
			)
		}
	}
	if lastTerm := options.Log.LastTerm(); lastTerm > options.HardState.Term {
		return nil, fmt.Errorf(
			"%w: last log term=%d exceeds current term=%d",
			ErrInvalidCoreState,
			lastTerm,
			options.HardState.Term,
		)
	}
	ownedLog, err := NewLog(
		state.SnapshotIndex,
		state.SnapshotTerm,
		state.CommitIndex,
		state.AppliedIndex,
		state.Entries,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: recovered log copy: %v", ErrInvalidCoreState, err)
	}
	core := &Core{
		localID:            options.LocalID,
		voters:             options.Voters,
		hardState:          options.HardState,
		log:                ownedLog,
		role:               RoleFollower,
		electionTimeoutMin: options.ElectionTimeoutMin,
		electionTimeoutMax: options.ElectionTimeoutMax,
		random:             options.Random,
	}
	deadline, err := core.sampleDeadline(0)
	if err != nil {
		return nil, fmt.Errorf("%w: initial deadline: %v", ErrInvalidCoreState, err)
	}
	core.electionDeadline = deadline
	return core, nil
}

// Tick advances the explicit monotonic logical clock and begins a pre-vote at
// an expired follower, pre-candidate, or candidate deadline.
func (core *Core) Tick(now uint64) error {
	if core.hasPending {
		return ErrReadyOutstanding
	}
	if now < core.now {
		return fmt.Errorf("%w: now=%d previous=%d", ErrTickRegression, now, core.now)
	}
	if core.role == RoleLeader || now < core.electionDeadline {
		core.now = now
		return nil
	}
	return core.startPreVote(now)
}

// Step applies one validated peer RPC from a configured authenticated sender.
func (core *Core) Step(senderID uint16, rpc RPC) error {
	if core.hasPending {
		return ErrReadyOutstanding
	}
	rpc = normalizeRPC(rpc)
	if err := ValidateRPCSender(rpc, senderID, core.voters); err != nil {
		return err
	}
	if err := validateRPC(rpc, DefaultCodecLimits()); err != nil {
		return err
	}

	switch message := rpc.(type) {
	case PreVoteRequest:
		return core.handlePreVoteRequest(message)
	case PreVoteResponse:
		return core.handlePreVoteResponse(message)
	case RequestVoteRequest:
		return core.handleVoteRequest(message)
	case RequestVoteResponse:
		return core.handleVoteResponse(message)
	case AppendEntriesRequest:
		return core.handleLeaderContact(message.LeaderID, message.Term)
	case InstallSnapshotRequest:
		return core.handleLeaderContact(message.LeaderID, message.Term)
	case AppendEntriesResponse:
		return core.handleHigherTermResponse(message.LeaderID, message.Term, message.RequestTerm)
	case InstallSnapshotResponse:
		return core.handleHigherTermResponse(message.LeaderID, message.Term, message.RequestTerm)
	case ErrorResponse, Handshake, HandshakeAck:
		return nil
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedCoreRPC, rpc)
	}
}

// ProposeEntry rejects non-leaders and fences leaders until a current-term
// entry is committed. Task 6 owns proposal append and completion mechanics.
func (core *Core) ProposeEntry(_ []byte) (Entry, error) {
	if core.hasPending {
		return Entry{}, ErrReadyOutstanding
	}
	if core.role != RoleLeader {
		return Entry{}, ErrNotLeader
	}
	if !core.hasCommittedCurrentTerm() {
		return Entry{}, ErrLeadershipNotAuthorized
	}
	return Entry{}, ErrLeadershipNotAuthorized
}

// Ready returns an independently owned copy of the one live protocol batch.
// Re-reading before Advance returns the same token and contents.
func (core *Core) Ready() (Ready, bool) {
	if !core.hasPending {
		return Ready{}, false
	}
	if core.pendingReady.Token == 0 {
		core.nextToken++
		if core.nextToken == 0 {
			core.nextToken++
		}
		core.pendingReady.Token = core.nextToken
	}
	return core.pendingReady.Clone(), true
}

// Advance consumes the exact live Ready batch after owner-ordered persistence,
// bounded handoff, and any later application work have succeeded.
func (core *Core) Advance(token ReadyToken) error {
	if !core.hasPending || token == 0 || token != core.pendingReady.Token {
		return ErrAdvanceToken
	}
	core.pendingReady = Ready{}
	core.hasPending = false
	return nil
}

// Status returns a point-in-time diagnostic value without borrowed storage.
func (core *Core) Status() Status {
	return Status{
		Role:         core.role,
		Term:         core.hardState.Term,
		LeaderID:     core.leaderID,
		CommitIndex:  core.log.CommitIndex(),
		AppliedIndex: core.log.AppliedIndex(),
		LastIndex:    core.log.LastIndex(),
	}
}

// HardState returns the current safety-critical state by value.
func (core *Core) HardState() HardState { return core.hardState }

// LogState returns an independently owned copy of the checked log.
func (core *Core) LogState() LogState { return core.log.State() }

// ElectionDeadline returns the current absolute logical election deadline.
func (core *Core) ElectionDeadline() uint64 { return core.electionDeadline }

// Voters returns the immutable fixed voter set used by the core.
func (core *Core) Voters() VoterSet { return core.voters }

func (core *Core) startPreVote(now uint64) error {
	if core.hardState.Term == math.MaxUint64 {
		return ErrTermOverflow
	}
	deadline, err := core.sampleDeadline(now)
	if err != nil {
		return err
	}
	prospectiveTerm := core.hardState.Term + 1
	request := PreVoteRequest{
		CandidateID:     core.localID,
		CurrentTerm:     core.hardState.Term,
		ProspectiveTerm: prospectiveTerm,
		LastLogIndex:    core.log.LastIndex(),
		LastLogTerm:     core.log.LastTerm(),
	}

	core.now = now
	core.electionDeadline = deadline
	core.role = RolePreCandidate
	core.leaderID = 0
	core.recentLeader = false
	core.preVotes = map[uint16]struct{}{core.localID: {}}
	core.votes = nil
	for _, voter := range core.voters.Voters() {
		if voter.ID != core.localID {
			core.queueMessage(voter.ID, request, DurabilityPrerequisite{})
		}
	}
	return nil
}

func (core *Core) handlePreVoteRequest(request PreVoteRequest) error {
	granted := core.role != RoleLeader &&
		!core.recentLeader &&
		request.CurrentTerm >= core.hardState.Term &&
		core.candidateLogIsUpToDate(request.LastLogIndex, request.LastLogTerm)
	response := PreVoteResponse{
		ResponderID:        core.localID,
		CandidateID:        request.CandidateID,
		Term:               core.hardState.Term,
		RequestCurrentTerm: request.CurrentTerm,
		ProspectiveTerm:    request.ProspectiveTerm,
		Granted:            granted,
	}
	core.queueMessage(request.CandidateID, response, DurabilityPrerequisite{})
	return nil
}

func (core *Core) handlePreVoteResponse(response PreVoteResponse) error {
	if core.role != RolePreCandidate ||
		response.CandidateID != core.localID ||
		response.RequestCurrentTerm != core.hardState.Term ||
		core.hardState.Term == math.MaxUint64 ||
		response.ProspectiveTerm != core.hardState.Term+1 ||
		response.Term < core.hardState.Term ||
		!response.Granted {
		return nil
	}
	if _, duplicate := core.preVotes[response.ResponderID]; duplicate {
		return nil
	}
	if len(core.preVotes)+1 < core.voters.Majority() {
		core.preVotes[response.ResponderID] = struct{}{}
		return nil
	}
	return core.startElection(response.ResponderID)
}

func (core *Core) startElection(finalPreVoter uint16) error {
	if core.hardState.Term == math.MaxUint64 {
		return ErrTermOverflow
	}
	deadline, err := core.sampleDeadline(core.now)
	if err != nil {
		return err
	}
	term := core.hardState.Term + 1
	request := RequestVoteRequest{
		CandidateID:  core.localID,
		Term:         term,
		LastLogIndex: core.log.LastIndex(),
		LastLogTerm:  core.log.LastTerm(),
	}

	core.preVotes[finalPreVoter] = struct{}{}
	core.hardState.Term = term
	core.hardState.VotedFor = core.localID
	core.role = RoleCandidate
	core.leaderID = 0
	core.recentLeader = false
	core.electionDeadline = deadline
	core.preVotes = nil
	core.votes = map[uint16]struct{}{core.localID: {}}
	core.recordHardState()
	for _, voter := range core.voters.Voters() {
		if voter.ID != core.localID {
			core.queueMessage(voter.ID, request, DurabilityPrerequisite{HardState: true})
		}
	}
	return nil
}

func (core *Core) handleVoteRequest(request RequestVoteRequest) error {
	if request.Term < core.hardState.Term {
		core.queueVoteResponse(request, false, false)
		return nil
	}
	hardStateChanged := false
	if request.Term > core.hardState.Term {
		core.adoptHigherTerm(request.Term)
		hardStateChanged = true
	}
	canVote := core.hardState.VotedFor == 0 || core.hardState.VotedFor == request.CandidateID
	granted := canVote && core.candidateLogIsUpToDate(request.LastLogIndex, request.LastLogTerm)
	if granted && core.hardState.VotedFor == 0 {
		core.hardState.VotedFor = request.CandidateID
		core.recordHardState()
		hardStateChanged = true
	}
	core.queueVoteResponse(request, granted, hardStateChanged)
	return nil
}

func (core *Core) queueVoteResponse(request RequestVoteRequest, granted, hardStateChanged bool) {
	response := RequestVoteResponse{
		ResponderID: core.localID,
		CandidateID: request.CandidateID,
		Term:        core.hardState.Term,
		RequestTerm: request.Term,
		Granted:     granted,
	}
	requires := DurabilityPrerequisite{}
	if hardStateChanged {
		requires.HardState = true
	}
	core.queueMessage(request.CandidateID, response, requires)
}

func (core *Core) handleVoteResponse(response RequestVoteResponse) error {
	if response.CandidateID != core.localID ||
		core.role != RoleCandidate ||
		response.RequestTerm != core.hardState.Term {
		return nil
	}
	if response.Term > core.hardState.Term {
		core.adoptHigherTerm(response.Term)
		return nil
	}
	if response.Term != core.hardState.Term || !response.Granted {
		return nil
	}
	if _, duplicate := core.votes[response.ResponderID]; duplicate {
		return nil
	}
	if len(core.votes)+1 < core.voters.Majority() {
		core.votes[response.ResponderID] = struct{}{}
		return nil
	}
	return core.becomeLeader(response.ResponderID)
}

func (core *Core) becomeLeader(finalVoter uint16) error {
	previousIndex := core.log.LastIndex()
	newIndex, ok := checkedNextIndex(previousIndex)
	if !ok {
		return ErrLogOverflow
	}
	entry, err := NewEntry(newIndex, core.hardState.Term, EntryNoOp, nil)
	if err != nil {
		return err
	}
	previousTerm := core.log.LastTerm()
	if _, err := core.log.Append(previousIndex, previousTerm, []Entry{entry}); err != nil {
		return err
	}

	core.votes[finalVoter] = struct{}{}
	core.role = RoleLeader
	core.leaderID = core.localID
	core.preVotes = nil
	core.recentLeader = false
	core.queueEntries([]Entry{entry})
	for _, voter := range core.voters.Voters() {
		if voter.ID == core.localID {
			continue
		}
		request := AppendEntriesRequest{
			LeaderID:     core.localID,
			Term:         core.hardState.Term,
			Generation:   1,
			PrevLogIndex: previousIndex,
			PrevLogTerm:  previousTerm,
			LeaderCommit: core.log.CommitIndex(),
			Entries:      []Entry{entry},
		}
		core.queueMessage(voter.ID, request, DurabilityPrerequisite{EntriesThrough: newIndex})
	}
	return nil
}

func (core *Core) handleLeaderContact(leaderID uint16, term uint64) error {
	if term < core.hardState.Term {
		return nil
	}
	deadline, err := core.sampleDeadline(core.now)
	if err != nil {
		return err
	}
	if term > core.hardState.Term {
		core.adoptHigherTerm(term)
	}
	core.role = RoleFollower
	core.leaderID = leaderID
	core.recentLeader = true
	core.preVotes = nil
	core.votes = nil
	core.electionDeadline = deadline
	return nil
}

func (core *Core) handleHigherTermResponse(recipientID uint16, term, requestTerm uint64) error {
	if core.role != RoleLeader ||
		recipientID != core.localID ||
		requestTerm != core.hardState.Term ||
		term <= core.hardState.Term {
		return nil
	}
	core.adoptHigherTerm(term)
	return nil
}

func (core *Core) adoptHigherTerm(term uint64) {
	core.hardState.Term = term
	core.hardState.VotedFor = 0
	core.role = RoleFollower
	core.leaderID = 0
	core.recentLeader = false
	core.preVotes = nil
	core.votes = nil
	core.recordHardState()
}

func (core *Core) candidateLogIsUpToDate(candidateIndex, candidateTerm uint64) bool {
	localTerm := core.log.LastTerm()
	if candidateTerm != localTerm {
		return candidateTerm > localTerm
	}
	return candidateIndex >= core.log.LastIndex()
}

func (core *Core) sampleDeadline(now uint64) (uint64, error) {
	span := core.electionTimeoutMax - core.electionTimeoutMin
	offset := boundedrandom.Uint64n(core.random, span)
	timeout := core.electionTimeoutMin + offset
	if now > math.MaxUint64-timeout {
		return 0, fmt.Errorf("%w: now=%d timeout=%d", ErrDeadlineOverflow, now, timeout)
	}
	return now + timeout, nil
}

func (core *Core) recordHardState() {
	hardState := core.hardState
	core.pendingReady.HardState = &hardState
	core.hasPending = true
}

func (core *Core) queueEntries(entries []Entry) {
	core.pendingReady.Entries = append(core.pendingReady.Entries, cloneEntries(entries)...)
	core.hasPending = true
}

func (core *Core) queueMessage(to uint16, rpc RPC, requires DurabilityPrerequisite) {
	core.pendingReady.Messages = append(core.pendingReady.Messages, PeerMessage{
		To:       to,
		RPC:      CloneRPC(rpc),
		Requires: requires,
	})
	core.hasPending = true
}

func (core *Core) hasCommittedCurrentTerm() bool {
	commitIndex := core.log.CommitIndex()
	if commitIndex == 0 {
		return false
	}
	term, err := core.log.Term(commitIndex)
	return err == nil && term == core.hardState.Term
}
