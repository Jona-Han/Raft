package raft

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

	// If candidate is stale then reject
	if args.Term < rf.currentTerm { // Candidate is of lesser term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Else if this is stale then become follower and check to grant vote, no matter what state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.transitionToFollower(-1)
	}

	// If this hasn't voted for anything this term or has already tried to vote for the candidate,
	// then check to grant vote
	if (rf.votedFor < 0 ||
		rf.votedFor == args.CandidateID) &&
		rf.passesElectionRestriction(args) {
			reply.VoteGranted = true
			rf.transitionToFollower(args.CandidateID)
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
	XIndex int
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
	if args.Term > rf.currentTerm { // Any state and got appendEntries from new leader
		rf.currentTerm = args.Term
		rf.transitionToFollower(-1)
	} else if rf.currentState == Candidate {
		rf.transitionToFollower(rf.me)
	}

	rf.electionTimer.Reset(GetRandTimeout())

	reply.Term = args.Term
	reply.Success = false

	snapshotIndex := rf.log[0].Index
	if args.PrevLogIndex < snapshotIndex {
		args.PrevLogIndex = snapshotIndex
		cutAt := 0
		for i, entry := range args.Entries {
			if entry.Index <= args.PrevLogIndex {
				cutAt = i + 1
			} else {
				break
			}
		}
		if cutAt >= len(args.Entries) {
			args.Entries = []LogEntry{}
		} else {
			args.Entries = args.Entries[cutAt:]
		}
	} else if args.PrevLogIndex > snapshotIndex+len(rf.log)-1 {
		reply.XIndex = snapshotIndex + len(rf.log)
		reply.Success = false
		return
	} else if rf.log[args.PrevLogIndex-snapshotIndex].Term != args.PrevLogTerm {
		i := args.PrevLogIndex - snapshotIndex
		for i >= 1 && rf.log[i].Term == rf.log[i-1].Term {
			i--
		}
		reply.XIndex = i
		reply.Success = false
		return
	}

	reply.Success = true

	appendIndex := args.PrevLogIndex - snapshotIndex

	for idx, entry := range args.Entries {
		appendIndex++
		if appendIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
			appendIndex += len(args.Entries[idx:]) - 1
			break
		}
		if rf.log[appendIndex].Term != entry.Term {
			rf.log = append(rf.log[:appendIndex], args.Entries[idx:]...)
			appendIndex += len(args.Entries[idx:]) - 1
			break
		}
		rf.log[appendIndex] = entry
	}

	appendIndex += snapshotIndex

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, appendIndex)
	}

	if appendIndex > args.PrevLogIndex {
		rf.persist()
	}

	for rf.lastApplied < rf.commitIndex {
		idx := rf.lastApplied + 1  - snapshotIndex
		entry := rf.log[idx]
		rf.applyChQueue <- &ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			CommandTerm:   entry.Term,
		}
		rf.lastApplied++
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int 
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.transitionToFollower(-1)
	} else if rf.currentState == Candidate {
		rf.transitionToFollower(rf.me)
	}

	reply.Term = args.Term
	rf.currentTerm = args.Term

	currSnapIndex := rf.log[0].Index

	if currSnapIndex >= args.LastIncludedIndex {
		reply.Term = args.Term
		return
	}

	if rf.log[len(rf.log)-1].Index < args.LastIncludedIndex {
		rf.log = rf.log[:1]
	} else {
		rf.log = rf.log[args.LastIncludedIndex-currSnapIndex:]
	}
	rf.log[0] = LogEntry{
		Index: args.LastIncludedIndex,
		Term: args.LastIncludedTerm,
	}
	rf.snapshot = args.Data

	newSnapIndex := rf.log[0].Index
	if newSnapIndex > rf.lastApplied {
		rf.lastApplied = newSnapIndex
		rf.applyChQueue <- &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}
	if newSnapIndex > rf.commitIndex {
		rf.commitIndex = newSnapIndex
	}
	rf.persist()
}
