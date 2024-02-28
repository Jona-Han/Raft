package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// If candidate is stale then reject
	if args.Term < rf.currentTerm { // Candidate is of lesser term
		reply.VoteGranted = false
		return
	}

	// Else if this is stale then become follower and check to grant vote, no matter what state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.transitionToFollower()
		rf.votedFor = -1
	}

	// Else if this hasn't voted for anything this term or has already tried to vote for the candidate,
	// then check to grant vote
	if rf.votedFor < 0 || rf.votedFor == args.CandidateID {
		if rf.passesElectionRestriction(args) {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		}
	}
	rf.persist()
}

func (rf *Raft) passesElectionRestriction(args *RequestVoteArgs) bool {
	thisLastIndex := len(rf.log) - 1
	thisLastLog := rf.log[thisLastIndex]

	// If this last log entry term > candidate's, return false
	if thisLastLog.Term > args.LastLogTerm {
		return false
	}

	// If have same term, and this' length > candidate's, return false
	if (thisLastLog.Term == args.LastLogTerm) && thisLastIndex > args.LastLogIndex {
		return false
	}

	//else return true
	return true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If stale, then reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Candidate and got appendEntries from new leader with same term
	if rf.isCandidate() && args.Term == rf.currentTerm {
		rf.transitionToFollower()
	} else if args.Term > rf.currentTerm { // Any state and got appendEntries from new leader
		rf.currentTerm = args.Term
		rf.transitionToFollower()
		rf.votedFor = -1
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for _, newEntry := range args.Entries {
		if newEntry.Index < len(rf.log) && rf.log[newEntry.Index].Term != newEntry.Term {
			rf.log = rf.log[:newEntry.Index]
			break
		}
	}
	// 4. Append any new entries not already in the log
	for _, newEntry := range args.Entries {
		if newEntry.Index >= len(rf.log) {
			// Append the new entry to the log
			rf.log = append(rf.log, newEntry)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) == 0 {
			// Heartbeat with no entries
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
		} else {
			rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.updateLastHeartbeat()
	rf.persist()
}
