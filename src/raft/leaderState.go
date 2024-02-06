package raft

type LeaderState struct {
	rf *Raft

	nextIndex  map[int]int
	matchIndex map[int]int
}

func (ls *LeaderState) sendHeartbeats() {
	for i := range ls.rf.peers {
		if i != ls.rf.me {
			go ls.sendHeartbeat(i)
		}
	}
}

func (ls *LeaderState) sendHeartbeat(server int) {
	ls.rf.mu.Lock()
	if !ls.rf.isLeader() {
		// If not in leader state anymore, don't send heartbeat
		ls.rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term: ls.rf.currentTerm,
		LeaderId: ls.rf.me,
		Entries: make([]LogEntry, 0),
	}
	ls.rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := cs.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if !ok { return }

	ls.rf.mu.Lock()
	defer ls.rf.mu.Unlock()
	// If this is stale, then update term and convert to follower
	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.transitionToFollower()
		ls.rf.votedFor = -1
	}
}
