package raft

import (
	"bytes"
	"errors"
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
	// HeartbeatInterval is the positive logical-tick replication cadence; zero defaults to one.
	HeartbeatInterval uint64
	// MaxAppendEntries bounds entries carried by one generated AppendEntries request.
	MaxAppendEntries uint16
	// MaxAppendBytes bounds the actual canonical encoded AppendEntries payload.
	MaxAppendBytes uint64
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
	deadlineExhausted  bool
	electionTimeoutMin uint64
	electionTimeoutMax uint64
	random             interface{ Uint64() uint64 }
	recentLeader       bool
	heartbeatInterval  uint64
	heartbeatDeadline  uint64
	quorumDeadline     uint64
	quorumResponses    map[uint16]struct{}
	appendLimits       CodecLimits

	preVotes             map[uint16]struct{}
	votes                map[uint16]struct{}
	progress             map[uint16]Progress
	nextProposalID       ProposalID
	pendingProposals     map[ProposalID]Entry
	pendingProposalOrder []ProposalID
	proposalAtIndex      map[uint64]ProposalID

	pendingReady Ready
	hasPending   bool
	nextToken    ReadyToken
	terminalErr  error
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
	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 1
	}
	if heartbeatInterval >= options.ElectionTimeoutMin {
		return nil, fmt.Errorf(
			"%w: heartbeat interval %d must be below election minimum %d",
			ErrInvalidCoreState,
			heartbeatInterval,
			options.ElectionTimeoutMin,
		)
	}
	appendLimits := DefaultCodecLimits()
	if options.MaxAppendEntries != 0 {
		appendLimits.MaxAppendEntries = options.MaxAppendEntries
	}
	if options.MaxAppendBytes != 0 {
		appendLimits.MaxEncodedBytes = options.MaxAppendBytes
	}
	if _, _, err := EncodeRPC(AppendEntriesRequest{
		LeaderID: options.LocalID, Term: 1, Generation: 1,
	}, appendLimits); err != nil {
		return nil, fmt.Errorf("%w: append limits: %v", ErrInvalidCoreState, err)
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
		heartbeatInterval:  heartbeatInterval,
		appendLimits:       appendLimits,
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
	if core.terminalErr != nil {
		return core.terminalErr
	}
	if core.hasPending {
		return ErrReadyOutstanding
	}
	if now < core.now {
		return fmt.Errorf("%w: now=%d previous=%d", ErrTickRegression, now, core.now)
	}
	if core.role == RoleLeader {
		return core.tickLeader(now)
	}
	if core.deadlineExhausted {
		core.now = now
		return nil
	}
	if now < core.electionDeadline {
		core.now = now
		return nil
	}
	return core.startPreVote(now)
}

// Step applies one validated peer RPC from a configured authenticated sender.
func (core *Core) Step(senderID uint16, rpc RPC) error {
	if core.terminalErr != nil {
		return core.terminalErr
	}
	if core.hasPending {
		return ErrReadyOutstanding
	}
	rpc = normalizeRPC(rpc)
	if err := ValidateRPCSender(rpc, senderID, core.voters); err != nil {
		return err
	}
	if request, ok := rpc.(AppendEntriesRequest); ok {
		if _, _, err := EncodeRPC(request, core.appendLimits); err != nil {
			return err
		}
	} else {
		if err := validateRPC(rpc, DefaultCodecLimits()); err != nil {
			return err
		}
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
		return core.handleAppendRequest(message)
	case InstallSnapshotRequest:
		return core.handleLeaderContact(message.LeaderID, message.Term)
	case AppendEntriesResponse:
		return core.handleAppendResponse(message)
	case InstallSnapshotResponse:
		return nil
	case ErrorResponse, Handshake, HandshakeAck:
		return nil
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedCoreRPC, rpc)
	}
}

// ProposeEntry rejects non-leaders and fences leaders until a current-term
// entry is committed. Task 6 owns proposal append and completion mechanics.
func (core *Core) ProposeEntry(command []byte) (Entry, error) {
	_, entry, err := core.proposeTracked(EntryCommand, command)
	return entry, err
}

// proposeTracked appends one command or barrier and returns its exact waiter identity.
func (core *Core) proposeTracked(kind EntryKind, command []byte) (ProposalID, Entry, error) {
	if core.terminalErr != nil {
		return 0, Entry{}, core.terminalErr
	}
	if core.hasPending {
		return 0, Entry{}, ErrReadyOutstanding
	}
	if core.role != RoleLeader {
		return 0, Entry{}, ErrNotLeader
	}
	if !core.hasCommittedCurrentTerm() {
		return 0, Entry{}, ErrLeadershipNotAuthorized
	}
	if uint64(len(command)) > core.appendLimits.MaxCommandBytes {
		return 0, Entry{}, fmt.Errorf("%w: proposal command is %d bytes, maximum is %d", ErrRPCTooLarge, len(command), core.appendLimits.MaxCommandBytes)
	}
	if core.nextProposalID == ProposalID(math.MaxUint64) {
		return 0, Entry{}, ErrProposalIdentityOverflow
	}
	for _, voter := range core.voters.Voters() {
		if voter.ID == core.localID {
			continue
		}
		if progress := core.progress[voter.ID]; progress.Generation == RequestGeneration(math.MaxUint64) {
			return 0, Entry{}, ErrReplicationGenerationOverflow
		}
	}
	previousIndex := core.log.LastIndex()
	newIndex, ok := checkedNextIndex(previousIndex)
	if !ok {
		return 0, Entry{}, ErrLogOverflow
	}
	selfNext, ok := checkedNextIndex(newIndex)
	if !ok {
		return 0, Entry{}, ErrLogOverflow
	}
	entry, err := NewEntry(newIndex, core.hardState.Term, kind, command)
	if err != nil {
		return 0, Entry{}, err
	}
	if _, err := core.log.Append(previousIndex, core.log.LastTerm(), []Entry{entry}); err != nil {
		return 0, Entry{}, err
	}
	proposalID := core.nextProposalID + 1
	core.nextProposalID = proposalID
	if core.pendingProposals == nil {
		core.pendingProposals = make(map[ProposalID]Entry)
		core.proposalAtIndex = make(map[uint64]ProposalID)
	}
	core.pendingProposals[proposalID] = entry.Clone()
	core.pendingProposalOrder = append(core.pendingProposalOrder, proposalID)
	core.proposalAtIndex[entry.Index] = proposalID
	core.progress[core.localID] = Progress{MatchIndex: newIndex, NextIndex: selfNext}
	core.queueEntries([]Entry{entry})
	for _, voter := range core.voters.Voters() {
		if voter.ID == core.localID {
			continue
		}
		if err := core.issueAppend(voter.ID); err != nil {
			return 0, Entry{}, err
		}
	}
	return proposalID, entry.Clone(), nil
}

// Ready returns an independently owned copy of the one live protocol batch.
// Re-reading before Advance returns the same token and contents.
func (core *Core) Ready() (Ready, bool) {
	if !core.hasPending {
		return Ready{}, false
	}
	if core.pendingReady.Token == 0 {
		if core.nextToken == ReadyToken(math.MaxUint64) {
			core.terminalErr = ErrReadyTokenExhausted
			return Ready{}, false
		}
		core.nextToken++
		core.pendingReady.Token = core.nextToken
	}
	return core.pendingReady.Clone(), true
}

// Err returns the stable terminal core error, if one has occurred.
func (core *Core) Err() error { return core.terminalErr }

// Advance consumes the exact live Ready batch after owner-ordered persistence,
// bounded handoff, and any later application work have succeeded.
func (core *Core) Advance(token ReadyToken) error {
	if core.terminalErr != nil {
		return core.terminalErr
	}
	if !core.hasPending || token == 0 || token != core.pendingReady.Token {
		return ErrAdvanceToken
	}
	if len(core.pendingReady.CommittedEntries) != 0 {
		lastApplied := core.pendingReady.CommittedEntries[len(core.pendingReady.CommittedEntries)-1].Index
		if err := core.log.AdvanceApplied(lastApplied); err != nil {
			return err
		}
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

// Progress returns an owned view of one voter's leader replication state.
func (core *Core) Progress(voterID uint16) (Progress, bool) {
	progress, ok := core.progress[voterID]
	return progress, ok
}

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
	core.deadlineExhausted = false
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
		response.Term > core.hardState.Term ||
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
	core.deadlineExhausted = false
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
	selfNext, ok := checkedNextIndex(newIndex)
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
	core.progress = make(map[uint16]Progress, len(core.voters.Voters()))
	core.progress[core.localID] = Progress{MatchIndex: newIndex, NextIndex: selfNext}
	core.quorumResponses = make(map[uint16]struct{}, len(core.voters.Voters())-1)
	core.heartbeatDeadline = saturatingTickAdd(core.now, core.heartbeatInterval)
	core.quorumDeadline = saturatingTickAdd(core.now, core.electionTimeoutMin)
	core.queueEntries([]Entry{entry})
	for _, voter := range core.voters.Voters() {
		if voter.ID == core.localID {
			continue
		}
		core.progress[voter.ID] = Progress{NextIndex: newIndex}
		if err := core.issueAppend(voter.ID); err != nil {
			return err
		}
	}
	return nil
}

func (core *Core) tickLeader(now uint64) error {
	if now >= core.quorumDeadline {
		if 1+len(core.quorumResponses) < core.voters.Majority() {
			core.now = now
			return core.stepDownWithoutTermChange(now)
		}
	}
	if now >= core.heartbeatDeadline {
		for _, voter := range core.voters.Voters() {
			if voter.ID == core.localID {
				continue
			}
			progress := core.progress[voter.ID]
			if !progress.SnapshotNeeded && progress.Generation == RequestGeneration(math.MaxUint64) {
				return ErrReplicationGenerationOverflow
			}
		}
	}

	core.now = now
	if now >= core.quorumDeadline {
		core.quorumResponses = make(map[uint16]struct{}, len(core.voters.Voters())-1)
		core.quorumDeadline = saturatingTickAdd(now, core.electionTimeoutMin)
	}
	if now < core.heartbeatDeadline {
		return nil
	}
	for _, voter := range core.voters.Voters() {
		if voter.ID == core.localID {
			continue
		}
		if err := core.issueAppend(voter.ID); err != nil {
			return err
		}
	}
	core.heartbeatDeadline = saturatingTickAdd(now, core.heartbeatInterval)
	return nil
}

func (core *Core) stepDownWithoutTermChange(now uint64) error {
	deadline, err := core.sampleDeadline(now)
	if err != nil {
		if !errors.Is(err, ErrDeadlineOverflow) {
			return err
		}
		deadline = math.MaxUint64
		core.deadlineExhausted = true
	} else {
		core.deadlineExhausted = false
	}
	core.role = RoleFollower
	core.leaderID = 0
	core.recentLeader = false
	core.progress = nil
	core.quorumResponses = nil
	core.electionDeadline = deadline
	core.failPendingProposals(ErrProposalFailed)
	return nil
}

func (core *Core) handleLeaderContact(leaderID uint16, term uint64) error {
	if term < core.hardState.Term {
		return nil
	}
	deadline, err := core.sampleDeadline(core.now)
	deadlineExhausted := false
	if err != nil {
		if !errors.Is(err, ErrDeadlineOverflow) {
			return err
		}
		deadline = math.MaxUint64
		deadlineExhausted = true
	}
	if term > core.hardState.Term {
		core.adoptHigherTerm(term)
	} else if core.role == RoleLeader && leaderID != core.localID {
		core.failPendingProposals(ErrProposalFailed)
	}
	core.role = RoleFollower
	core.leaderID = leaderID
	core.recentLeader = true
	core.preVotes = nil
	core.votes = nil
	core.progress = nil
	core.quorumResponses = nil
	core.electionDeadline = deadline
	core.deadlineExhausted = deadlineExhausted
	return nil
}

func (core *Core) handleAppendRequest(request AppendEntriesRequest) error {
	if request.Term < core.hardState.Term {
		conflictIndex, ok := checkedNextIndex(core.log.LastIndex())
		if !ok {
			conflictIndex = core.log.LastIndex()
		}
		core.queueAppendResponse(request, false, 0, ConflictHint{Index: conflictIndex}, DurabilityPrerequisite{})
		return nil
	}

	before := core.log.State()
	trialLog, err := NewLog(
		before.SnapshotIndex,
		before.SnapshotTerm,
		before.CommitIndex,
		before.AppliedIndex,
		before.Entries,
	)
	if err != nil {
		return err
	}
	hint, appendErr := trialLog.Append(request.PrevLogIndex, request.PrevLogTerm, request.Entries)
	if appendErr != nil &&
		!errors.Is(appendErr, ErrLogUnavailable) &&
		!errors.Is(appendErr, ErrLogCompacted) &&
		!errors.Is(appendErr, ErrLogMismatch) {
		return appendErr
	}

	hardStateChanged := request.Term > core.hardState.Term
	if err := core.handleLeaderContact(request.LeaderID, request.Term); err != nil {
		return err
	}
	if appendErr != nil {
		requires := DurabilityPrerequisite{HardState: hardStateChanged}
		core.queueAppendResponse(request, false, 0, hint, requires)
		return nil
	}
	core.log = trialLog

	lastVerified := request.PrevLogIndex
	if len(request.Entries) != 0 {
		lastVerified = request.Entries[len(request.Entries)-1].Index
	}
	previousCommit := core.log.CommitIndex()
	commitIndex := request.LeaderCommit
	if commitIndex > lastVerified {
		commitIndex = lastVerified
	}
	if commitIndex > core.log.CommitIndex() {
		if err := core.log.AdvanceCommit(commitIndex); err != nil {
			return err
		}
		core.hardState.CommitIndex = commitIndex
		core.recordHardState()
		if err := core.queueCommittedEntries(previousCommit, commitIndex); err != nil {
			return err
		}
		hardStateChanged = true
	}

	changed := changedLogEntries(before.Entries, core.log.State().Entries)
	requires := DurabilityPrerequisite{HardState: hardStateChanged}
	if len(changed) != 0 {
		core.queueEntries(changed)
		requires.EntriesThrough = changed[len(changed)-1].Index
	}
	core.queueAppendResponse(request, true, lastVerified, ConflictHint{}, requires)
	return nil
}

func (core *Core) queueAppendResponse(request AppendEntriesRequest, success bool, matchIndex uint64, hint ConflictHint, requires DurabilityPrerequisite) {
	core.queueMessage(request.LeaderID, AppendEntriesResponse{
		ResponderID:   core.localID,
		LeaderID:      request.LeaderID,
		Term:          core.hardState.Term,
		RequestTerm:   request.Term,
		Generation:    request.Generation,
		Success:       success,
		MatchIndex:    matchIndex,
		ConflictTerm:  hint.Term,
		ConflictIndex: hint.Index,
	}, requires)
}

func changedLogEntries(before, after []Entry) []Entry {
	shared := len(before)
	if len(after) < shared {
		shared = len(after)
	}
	firstChanged := shared
	for index := 0; index < shared; index++ {
		if !sameEntry(before[index], after[index]) {
			firstChanged = index
			break
		}
	}
	if firstChanged == shared && len(after) == len(before) {
		return nil
	}
	return cloneEntries(after[firstChanged:])
}

func sameEntry(left, right Entry) bool {
	return left.Index == right.Index &&
		left.Term == right.Term &&
		left.Kind == right.Kind &&
		bytes.Equal(left.command, right.command)
}

func (core *Core) handleAppendResponse(response AppendEntriesResponse) error {
	if core.role != RoleLeader ||
		response.LeaderID != core.localID ||
		response.RequestTerm != core.hardState.Term {
		return nil
	}
	progress, active := core.progress[response.ResponderID]
	if !active || response.Generation != progress.ActiveGeneration {
		return nil
	}
	if response.Success && response.MatchIndex != progress.activeMatchIndex {
		return nil
	}
	if response.Term > core.hardState.Term {
		progress.ActiveGeneration = 0
		progress.activeMatchIndex = 0
		core.progress[response.ResponderID] = progress
		core.quorumResponses[response.ResponderID] = struct{}{}
		core.adoptHigherTerm(response.Term)
		return nil
	}
	if response.Success {
		progress.ActiveGeneration = 0
		progress.activeMatchIndex = 0
		if response.MatchIndex > progress.MatchIndex {
			progress.MatchIndex = response.MatchIndex
		}
		nextIndex, ok := checkedNextIndex(progress.MatchIndex)
		if !ok {
			return ErrLogOverflow
		}
		if progress.NextIndex < nextIndex {
			progress.NextIndex = nextIndex
		}
		progress.SnapshotNeeded = false
		if progress.NextIndex <= core.log.LastIndex() && progress.Generation == RequestGeneration(math.MaxUint64) {
			return ErrReplicationGenerationOverflow
		}
		core.progress[response.ResponderID] = progress
		core.quorumResponses[response.ResponderID] = struct{}{}
		if err := core.advanceLeaderCommit(); err != nil {
			return err
		}
		if progress.NextIndex <= core.log.LastIndex() {
			return core.issueAppend(response.ResponderID)
		}
		return nil
	}

	progress.ActiveGeneration = 0
	progress.activeMatchIndex = 0
	nextIndex := response.ConflictIndex
	if response.ConflictTerm != 0 {
		if lastIndex, ok := core.log.LastIndexOfTerm(response.ConflictTerm); ok {
			var nextOK bool
			nextIndex, nextOK = checkedNextIndex(lastIndex)
			if !nextOK {
				return ErrLogOverflow
			}
		}
	}
	maximumNext, ok := checkedNextIndex(core.log.LastIndex())
	if !ok {
		return ErrLogOverflow
	}
	if nextIndex > maximumNext {
		nextIndex = maximumNext
	}
	minimumNext, ok := checkedNextIndex(progress.MatchIndex)
	if !ok {
		return ErrLogOverflow
	}
	if nextIndex < minimumNext {
		nextIndex = minimumNext
	}
	progress.NextIndex = nextIndex
	if nextIndex <= core.log.SnapshotIndex() {
		progress.SnapshotNeeded = true
		core.progress[response.ResponderID] = progress
		core.quorumResponses[response.ResponderID] = struct{}{}
		return nil
	}
	progress.SnapshotNeeded = false
	if progress.Generation == RequestGeneration(math.MaxUint64) {
		return ErrReplicationGenerationOverflow
	}
	core.progress[response.ResponderID] = progress
	core.quorumResponses[response.ResponderID] = struct{}{}
	return core.issueAppend(response.ResponderID)
}

func (core *Core) issueAppend(peerID uint16) error {
	progress, ok := core.progress[peerID]
	if !ok || peerID == core.localID || progress.SnapshotNeeded {
		return nil
	}
	if progress.Generation == RequestGeneration(math.MaxUint64) {
		return ErrReplicationGenerationOverflow
	}
	minimumNext, ok := checkedNextIndex(progress.MatchIndex)
	if !ok {
		return ErrLogOverflow
	}
	if progress.NextIndex < minimumNext {
		progress.NextIndex = minimumNext
	}
	maximumNext, ok := checkedNextIndex(core.log.LastIndex())
	if !ok {
		return ErrLogOverflow
	}
	if progress.NextIndex > maximumNext {
		progress.NextIndex = maximumNext
	}
	if progress.NextIndex <= core.log.SnapshotIndex() {
		progress.SnapshotNeeded = true
		progress.ActiveGeneration = 0
		core.progress[peerID] = progress
		return nil
	}

	prevIndex := progress.NextIndex - 1
	prevTerm, err := core.log.Term(prevIndex)
	if err != nil {
		return err
	}
	generation := progress.Generation + 1
	request := AppendEntriesRequest{
		LeaderID: core.localID, Term: core.hardState.Term, Generation: generation,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		LeaderCommit: core.log.CommitIndex(),
	}
	for index := progress.NextIndex; index <= core.log.LastIndex(); index++ {
		if len(request.Entries) == int(core.appendLimits.MaxAppendEntries) {
			break
		}
		entry, err := core.log.Entry(index)
		if err != nil {
			return err
		}
		candidate := request
		candidate.Entries = append(cloneEntries(request.Entries), entry)
		if _, _, err := EncodeRPC(candidate, core.appendLimits); err != nil {
			if errors.Is(err, ErrRPCTooLarge) {
				break
			}
			return err
		}
		request.Entries = candidate.Entries
		if index == math.MaxUint64 {
			break
		}
	}
	matchIndex := prevIndex
	if len(request.Entries) != 0 {
		matchIndex = request.Entries[len(request.Entries)-1].Index
	}
	requires := DurabilityPrerequisite{}
	if len(request.Entries) != 0 && readyContainsEntry(core.pendingReady.Entries, request.Entries[len(request.Entries)-1].Index) {
		requires.EntriesThrough = request.Entries[len(request.Entries)-1].Index
	}
	progress.Generation = generation
	progress.ActiveGeneration = generation
	progress.activeMatchIndex = matchIndex
	core.progress[peerID] = progress
	core.queueMessage(peerID, request, requires)
	return nil
}

func (core *Core) advanceLeaderCommit() error {
	currentCommit := core.log.CommitIndex()
	for candidate := core.log.LastIndex(); candidate > currentCommit; candidate-- {
		term, err := core.log.Term(candidate)
		if err != nil {
			return err
		}
		if term != core.hardState.Term {
			continue
		}
		replicas := 0
		for _, voter := range core.voters.Voters() {
			progress := core.progress[voter.ID]
			if progress.MatchIndex >= candidate {
				replicas++
			}
		}
		if replicas < core.voters.Majority() {
			continue
		}
		if err := core.log.AdvanceCommit(candidate); err != nil {
			return err
		}
		core.hardState.CommitIndex = candidate
		core.recordHardState()
		if err := core.queueCommittedEntries(currentCommit, candidate); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func readyContainsEntry(entries []Entry, index uint64) bool {
	for _, entry := range entries {
		if entry.Index == index {
			return true
		}
	}
	return false
}

func (core *Core) queueCommittedEntries(previousCommit, commitIndex uint64) error {
	index, ok := checkedNextIndex(previousCommit)
	if !ok {
		return ErrLogOverflow
	}
	for index <= commitIndex {
		entry, err := core.log.Entry(index)
		if err != nil {
			return err
		}
		core.pendingReady.CommittedEntries = append(core.pendingReady.CommittedEntries, entry)
		core.resolveCommittedProposal(entry)
		core.hasPending = true
		if index == math.MaxUint64 {
			break
		}
		index++
	}
	return nil
}

func (core *Core) resolveCommittedProposal(entry Entry) {
	proposalID, ok := core.proposalAtIndex[entry.Index]
	if !ok {
		return
	}
	proposal, ok := core.pendingProposals[proposalID]
	if !ok {
		delete(core.proposalAtIndex, entry.Index)
		return
	}
	if sameEntry(proposal, entry) {
		core.pendingReady.CommittedProposals = append(core.pendingReady.CommittedProposals, CommittedProposal{
			ID: proposalID, Entry: proposal.Clone(),
		})
	} else {
		core.pendingReady.FailedProposals = append(core.pendingReady.FailedProposals, FailedProposal{
			ID: proposalID, Entry: proposal.Clone(), Err: ErrProposalFailed,
		})
	}
	delete(core.pendingProposals, proposalID)
	delete(core.proposalAtIndex, entry.Index)
	core.removePendingProposalOrder(proposalID)
}

func (core *Core) removePendingProposalOrder(proposalID ProposalID) {
	for index, pendingID := range core.pendingProposalOrder {
		if pendingID != proposalID {
			continue
		}
		copy(core.pendingProposalOrder[index:], core.pendingProposalOrder[index+1:])
		core.pendingProposalOrder = core.pendingProposalOrder[:len(core.pendingProposalOrder)-1]
		return
	}
}

func (core *Core) failPendingProposals(reason error) {
	for _, proposalID := range core.pendingProposalOrder {
		proposal, ok := core.pendingProposals[proposalID]
		if !ok {
			continue
		}
		core.pendingReady.FailedProposals = append(core.pendingReady.FailedProposals, FailedProposal{
			ID: proposalID, Entry: proposal.Clone(), Err: reason,
		})
		delete(core.proposalAtIndex, proposal.Index)
		delete(core.pendingProposals, proposalID)
		core.hasPending = true
	}
	core.pendingProposalOrder = nil
}

func saturatingTickAdd(now, interval uint64) uint64 {
	if now > math.MaxUint64-interval {
		return math.MaxUint64
	}
	return now + interval
}

func (core *Core) adoptHigherTerm(term uint64) {
	core.failPendingProposals(ErrProposalFailed)
	core.hardState.Term = term
	core.hardState.VotedFor = 0
	core.role = RoleFollower
	core.leaderID = 0
	core.recentLeader = false
	core.preVotes = nil
	core.votes = nil
	core.progress = nil
	core.quorumResponses = nil
	if core.now > math.MaxUint64-core.electionTimeoutMin {
		core.electionDeadline = math.MaxUint64
		core.deadlineExhausted = true
	}
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
