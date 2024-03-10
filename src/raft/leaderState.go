package raft

import (
	"sync"
	"time"
)

type LeaderState struct {
	rf         *Raft
	nextIndex  map[int]int
	matchIndex map[int]int
	cond       *sync.Cond
	hbCond     *sync.Cond
}

func (ls *LeaderState) init() {
	lastLogIndex := ls.rf.log[len(ls.rf.log)-1].Index
	for key := range ls.rf.peers {
		ls.nextIndex[key] = lastLogIndex + 1
		ls.matchIndex[key] = 0
	}
}

func (ls *LeaderState) sendLogs() {
	lastLogIndex := ls.rf.log[len(ls.rf.log)-1].Index
	for key, value := range ls.nextIndex {
		if key != ls.rf.me && lastLogIndex >= value {
			go ls.sendLog(key)
		}
	}
}

func (ls *LeaderState) sendLog(server int) {
	// While logs are not up to date and successful, continue loop
	for !ls.rf.killed() {
		ls.rf.mu.Lock()
		if !ls.rf.isLeader() {
			defer ls.rf.mu.Unlock()
			return
		}

		nextIndex := ls.nextIndex[server]

		args := AppendEntriesArgs{
			Term:         ls.rf.currentTerm,
			LeaderId:     ls.rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  ls.rf.log[nextIndex-1].Term,
			Entries:      make([]LogEntry, len(ls.rf.log)-nextIndex),
			LeaderCommit: ls.rf.commitIndex,
		}
		copy(args.Entries, ls.rf.log[nextIndex:len(ls.rf.log)])
		// DPrintf("%v sending logs to %v", ls.rf.me, server)
		ls.rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ls.rf.mu.Lock()

		// If this is stale, then update term and convert to follower
		if reply.Term > ls.rf.currentTerm {
			ls.rf.currentTerm = reply.Term
			ls.rf.transitionToFollower()
			ls.rf.votedFor = -1
			ls.rf.persist()
			ls.rf.mu.Unlock()
			return
		}

		// If log's reply shows out of date, update which logs to send and retry
		if reply.Success {
			lastLogEntry := len(args.Entries) - 1
			if lastLogEntry >= 0 {
				ls.nextIndex[server] = args.Entries[lastLogEntry].Index + 1
				ls.matchIndex[server] = args.Entries[lastLogEntry].Index
				go ls.newCommitMajorityChecker()
			}
			ls.rf.mu.Unlock()
			return
		} else { //Failure
			//Log too short
			if reply.XIndex == 0 {
				ls.nextIndex[server] = max(1, reply.XLen)
			} else {
				XTermLastIndex := -1
				for idx, entry := range ls.rf.log {
					if entry.Term == reply.XTerm {
						XTermLastIndex = idx
						break
					}
				}

				if XTermLastIndex == -1 {
					ls.nextIndex[server] = reply.XIndex
				} else {
					ls.nextIndex[server] = XTermLastIndex
				}
			}
		}
		ls.rf.mu.Unlock()
	}
}

func (ls *LeaderState) newCommitMajorityChecker() {
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	ls.rf.mu.Lock()
	maxMatchIndex := 0
	for _, matchIndex := range ls.matchIndex {
		if matchIndex > maxMatchIndex {
			maxMatchIndex = matchIndex
		}
	}

	for n := maxMatchIndex; n > ls.rf.commitIndex; n-- {
		count := 1
		for server, matchIndex := range ls.matchIndex {
			if server != ls.rf.me && matchIndex >= n {
				count++
			}
		}
		if count > len(ls.matchIndex)/2 && ls.rf.log[n].Term == ls.rf.currentTerm {
			ls.rf.commitIndex = n
			break
		}
	}
	ls.rf.mu.Unlock()
}

// sendHeartbeats sends heartsbeats to all peers
func (ls *LeaderState) sendHeartbeats() {
	for !ls.rf.killed() {
		ls.rf.mu.Lock()

		if !ls.rf.isLeader() {
			ls.hbCond.Wait()
		}
		ls.rf.mu.Unlock()

		for i := range ls.rf.peers {
			if i != ls.rf.me {
				go ls.sendHeartbeat(i)
			}
		}

		heartbeatSleepTime := 100
		time.Sleep(time.Duration(heartbeatSleepTime) * time.Millisecond)
	}
}

// sendHeartbeat sends a heartbeat RPC call to the server in param
func (ls *LeaderState) sendHeartbeat(server int) {
	ls.rf.mu.Lock()
	// If not in leader state anymore, don't send heartbeat
	if !ls.rf.isLeader() {
		ls.rf.mu.Unlock()
		return
	}

	prevLogIndex := len(ls.rf.log) - 1
	prevLogTerm := ls.rf.log[prevLogIndex].Term

	args := AppendEntriesArgs{
		Term:         ls.rf.currentTerm,
		LeaderId:     ls.rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: ls.rf.commitIndex,
	}
	ls.rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if !ok {
		return
	}

	ls.rf.mu.Lock()
	defer ls.rf.mu.Unlock()
	// If this is stale, then update term and convert to follower
	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.transitionToFollower()
		ls.rf.votedFor = -1
		ls.rf.persist()
	}
}
