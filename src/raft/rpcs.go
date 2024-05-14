package raft

import "fmt"

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

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
	changed := false

	// Else if this is stale then become follower and check to grant vote, no matter what state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.currentState = Follower
		rf.votedFor = -1
		changed = true
	}

	// If this hasn't voted for anything this term or has already tried to vote for the candidate,
	// then check to grant vote
	if (rf.votedFor < 0 ||
		rf.votedFor == args.CandidateID) &&
		rf.passesElectionRestriction(args) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		changed = true
	}
	if changed {
		rf.persist()
	}
}

func (rf *Raft) passesElectionRestriction(args *RequestVoteArgs) bool {
	thisLastLog := rf.log[len(rf.log)-1]

	// If this last log entry term > candidate's, return false
	if thisLastLog.Term > args.LastLogTerm {
		return false
	}

	// If have same term, and this' length > candidate's, return false
	if (thisLastLog.Term == args.LastLogTerm) && thisLastLog.Index > args.LastLogIndex {
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

	XTerm  int
	XIndex int
	XLen   int

	NeedsSnapshot bool
	XIsShort bool
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

	rf.heartbeat = true

	// Candidate and got appendEntries from new leader with same term
	if rf.currentState == Candidate && args.Term == rf.currentTerm {
		rf.currentState = Follower
	} else if args.Term > rf.currentTerm { // Any state and got appendEntries from new leader
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = -1
		rf.persist()
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	snapshotIndex := rf.log[0].Index
	snapshotTerm := rf.log[0].Term
	lastLogIndex := rf.log[len(rf.log)-1].Index


	//  These are possible cases:
	//  case 0) if !(0 <= args.PrevIndex <= rf.XIndex + len(rf.logs)), the given args.PrevIndex is out our search space. Then, we tell the leader that our log is short, and ask for a prevLogIndex set to reply.XLen+1. In the next round, we decide about the APE.
	//	case 1) if the range is in our search space, there is a match if any of these happens:
	// 		1. if prevLogIdx == -1, i.e. args.PrevIndex is at rf.XIndex, it is a match if args.PrevIndex == rf.XIndex && args.PrevTerm == rf.XTerm
	//		2. if prevLogIdx >=  0, i.e. args.PrevIndex is in the tail, it is a match if args.PrevIndex == rf.logs[prevLogIdx].Index && args.PrevTerm == rf.logs[prevLogIdx].Term
	// 	case 2) if we couldn't find a match, then we have to reject the APE. But, we have to fill reply properly
	//      - find the largest index j such that Term(j) != Term(prevLogIdx), where j<prevLogIdx. Therefore, the j's range is [-2,-1,...,prevLogIdx-1]:
	//			1. if prevLogIdx == -1 we need a new snapshot because we cannot find such a j, meaning there is no search space for that (see 1.1)
	// 			2. if j==-2: we need a new snapshot because we couldn't find such a j, i.e. we searched but such j is before our last snapshot
	// 			3. if j>=-1: set the reply.XIndex=rf.logs[j+1].Index, i.e. we could find the j that satisfies case 2.
	prevLogIdx := args.PrevLogIndex - snapshotIndex - 1 // 6 0 - 1 = 0
	if !(args.PrevLogIndex >= snapshotIndex && args.PrevLogIndex <= lastLogIndex) {
		// case 0) claim that the log is shorter
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XIsShort = true
		reply.XLen = lastLogIndex
		return
	} else if !(prevLogIdx == -1 && args.PrevLogIndex == snapshotIndex && args.PrevLogTerm == snapshotTerm) && 
		!(prevLogIdx >= 0 && args.PrevLogIndex == rf.log[prevLogIdx+1].Index && args.PrevLogTerm == rf.log[prevLogIdx+1].Term) {
		// if it is not case 1), so we are in case 2)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLen = lastLogIndex
		fmt.Printf("2.1")

		if prevLogIdx == -1 {
			// case 2.1
			reply.NeedsSnapshot = true
			return
		}

		j := prevLogIdx - 1
		for j >= 0 {
			if (j >= 1 && rf.log[j].Term != rf.log[prevLogIdx].Term) || (j == 0 && snapshotTerm != args.PrevLogTerm) {
				break
			}
			j -= 1
		}

		if j == -1 {
			// case 2.2
			reply.NeedsSnapshot = true
			return
		}

		// case 2.3
		reply.XIndex = rf.log[j+1].Index
		reply.XTerm = rf.log[j+1].Term

		return
	}


	// case 1)
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	for _, newEntry := range args.Entries {
		if newEntry.Index <= lastLogIndex &&
			rf.log[newEntry.Index-snapshotIndex].Term != newEntry.Term {
			rf.log = rf.log[:newEntry.Index-snapshotIndex]
			break
		}
	}
	// Append any new entries not already in the log
	lastLogIndex = rf.log[len(rf.log)-1].Index
	for _, newEntry := range args.Entries {
		if newEntry.Index > lastLogIndex {
			// Append the new entry to the log
			rf.log = append(rf.log, newEntry)
		}
	}
	if len(args.Entries) > 0 {
		// DPrintf("Follower %v: Finished call to AppendEntries with lastLog: %v", rf.me, rf.log[len(rf.log)-1])
		defer rf.persist()
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log) - 1].Index)
	}

	go func() {
		rf.applyQueue <- struct{}{}
	}()

	reply.Term = rf.currentTerm
	reply.Success = true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Success bool // if the snapshot installed successfully; for the leader
	Term    int  // current term of the follower
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	currSnapIndex := rf.log[0].Index
	currSnapTerm := rf.log[0].Term

	if currSnapIndex > args.LastIncludedIndex {
		reply.Success = false
		return
	} else if currSnapIndex == args.LastIncludedIndex && currSnapTerm == args.LastIncludedTerm {
		reply.Success = true
		return
	}
	defer rf.persist()

	rf.snapshot = args.Data

	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{
		Index: args.LastIncludedIndex,
		Term:  args.LastIncludedTerm,
	})

	idx := args.LastIncludedIndex - currSnapIndex
	if idx >= 0 && idx < len(rf.log) &&
	rf.log[idx].Index == args.LastIncludedIndex && rf.log[idx].Term == args.LastIncludedTerm {
		newLog = append(newLog, rf.log[idx+1:]...)
	}

	rf.log = newLog

	reply.Success = true

	go func() {
		rf.applyQueue <- struct{}{}
	}()
}
