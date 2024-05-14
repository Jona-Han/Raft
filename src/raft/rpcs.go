package raft

import "time"

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
	lastLogIndex := rf.log[len(rf.log)-1].Index
	//My log is too short
	// if args.PrevLogIndex > lastLogIndex {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	reply.XIsShort = true
	// 	reply.XLen = lastLogIndex
	// 	return
	// } else if args.PrevLogIndex >= snapshotIndex && rf.log[args.PrevLogIndex-snapshotIndex].Term != args.PrevLogTerm {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	reply.XTerm = rf.log[args.PrevLogIndex-snapshotIndex].Term
	// 	reply.XLen = lastLogIndex

	// 	for _, entry := range rf.log {
	// 		if entry.Term == reply.XTerm {
	// 			reply.XIndex = entry.Index
	// 			break
	// 		}
	// 	}
	// 	return
	// }
	prevLogIdx := args.PrevLogIndex - snapshotIndex - 1
	if !(args.PrevLogIndex >= snapshotIndex && args.PrevLogIndex <= lastLogIndex) {
		// case 0) claim that the log is shorter
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XIsShort = true
		reply.XLen = lastLogIndex
		return
	} else if !(prevLogIdx == -1 && args.PrevLogIndex == snapshotIndex && args.PrevLogTerm == rf.log[0].Term) && 
		!(prevLogIdx >= 0 && args.PrevLogIndex == rf.log[prevLogIdx+1].Index && args.PrevLogTerm == rf.log[prevLogIdx+1].Term) {
		// if it is not case 1), so we are in case 2)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLen = lastLogIndex

		if prevLogIdx == -1 {
			// case 2.1
			reply.NeedsSnapshot = true
			return
		}

		j := prevLogIdx - 1
		for j >= 0 {
			if (j >= 1 && rf.log[j].Term != rf.log[prevLogIdx].Term) || (j == 0 && rf.log[0].Term != args.PrevLogTerm) {
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
	if len(args.Entries) != 0 {
		// DPrintf("Follower %v: Finished call to AppendEntries with lastLog: %v", rf.me, rf.log[len(rf.log)-1])
		rf.persist()
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log) - 1].Index)
	}
	rf.applyCond.Signal()

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
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.snapshot = args.Data

	// lastLogIndex := rf.log[len(rf.log)-1].Index
	// ssIndex := rf.log[0].Index
	// if args.LastIncludedIndex <= lastLogIndex && args.LastIncludedIndex >= ssIndex &&
	// 	rf.log[args.LastIncludedIndex-ssIndex].Index == args.LastIncludedIndex &&
	// 	rf.log[args.LastIncludedIndex-ssIndex].Term == args.LastIncludedTerm {

	// 	rf.log = rf.log[args.LastIncludedIndex-ssIndex:]
	// 	rf.persist()
	// 	return
	// }

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Index: args.LastIncludedIndex,
		Term:  args.LastIncludedTerm,
	})

	rf.persist()
	// DPrintf("%v: -- RPC handler InstallSnapshot -- %v, lastApplied: %v, commitIndex: %v", rf.me, rf.log[0].Index, rf.lastApplied, rf.commitIndex)

	go func() {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
			CommandValid:  false,
		}
		for !rf.killed() {
			rf.mu.Lock()
			if rf.lastApplied <= applyMsg.SnapshotIndex {
				rf.mu.Unlock()
				select {
				case rf.applyCh <- applyMsg:
					// DPrintf("Snapshot from server %v sent with index %v", rf.me, applyMsg.SnapshotIndex)
					rf.mu.Lock()
					rf.lastApplied = applyMsg.SnapshotIndex
					rf.mu.Unlock()
					return
				default:
					time.Sleep(10 * time.Millisecond)
				}
			} else {
				rf.mu.Unlock()
				return
			}
		}
	}()
}
