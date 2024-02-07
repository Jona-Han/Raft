package raft

import (
	"time"
)

type LeaderState struct {
	rf *Raft
	heartbeatsStarted bool
	// nextIndex  map[int]int
	// matchIndex map[int]int
}

// sendHeartbeats sends heartsbeats to all peers
func (ls *LeaderState) sendHeartbeats() {
	DPrintf("NEW LEADER YAY - %v", ls.rf.me)
	for ls.rf.killed() == false {
		ls.rf.mu.Lock()
		// If not in leader state anymore, don't send heartbeats
		if !ls.rf.isLeader() {
			ls.heartbeatsStarted = false
			ls.rf.mu.Unlock()
			return
		}
		ls.rf.mu.Unlock()
		
		for i := range ls.rf.peers {
			if i != ls.rf.me {
				go ls.sendHeartbeat(i)
			}
		}

		heartbeatSleepTime := 150
		time.Sleep(time.Duration(heartbeatSleepTime) * time.Millisecond)
	}
	ls.heartbeatsStarted = false
}

// sendHeartbeat sends a heartbeat RPC call to the server in param
func (ls *LeaderState) sendHeartbeat(server int) {
	ls.rf.mu.Lock()
	// If not in leader state anymore, don't send heartbeat
	if !ls.rf.isLeader() {
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
	ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

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
