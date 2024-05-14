package raft

import (
	"sync"
	"time"
	"fmt"
)

type LeaderState struct {
	rf         *Raft
	nextIndex  map[int]int
	matchIndex map[int]int
	cond       *sync.Cond
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
	for server, value := range ls.nextIndex {
		if server != ls.rf.me && lastLogIndex >= value {
			go ls.sendLog(server, ls.rf.currentTerm)
		}
	}
}

func (ls *LeaderState) sendLog(server int, term int) {
	// While logs are not up to date and successful, continue loop
	for !ls.rf.killed() {
		ls.rf.mu.Lock()
		if ls.rf.currentState != Leader || ls.rf.currentTerm > term {
			defer ls.rf.mu.Unlock()
			return
		}

		nextIndex := ls.nextIndex[server]
		lastEntryIndex := ls.rf.log[len(ls.rf.log)-1].Index
		snapshotIndex := ls.rf.log[0].Index

		if nextIndex <= snapshotIndex && snapshotIndex != 0 {
			fmt.Printf("SENDING SNAPSHOT?")
			go ls.sendSnapshot(server)
			ls.rf.mu.Unlock()
			return
		}

		if lastEntryIndex < nextIndex {
			ls.nextIndex[server] = lastEntryIndex
			ls.rf.mu.Unlock()
			return
		}

		if nextIndex == 0 {
			fmt.Printf("CRITICAL error: nextIndex can't be 0")
		}

		args := AppendEntriesArgs{
			Term:         ls.rf.currentTerm,
			LeaderId:     ls.rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  ls.rf.log[nextIndex-snapshotIndex-1].Term,
			Entries:      make([]LogEntry, lastEntryIndex-nextIndex+1),
		}

		copy(args.Entries, ls.rf.log[nextIndex-snapshotIndex:])
		ls.rf.mu.Unlock()

		reply := AppendEntriesReply{}
		// DPrintf("LEADER: %v sending logs to server:%v prevLogIndex: %v, len(entries):%v, commitIndex: %v, entries: %v", ls.rf.me, server, args.PrevLogIndex, len(args.Entries), args.LeaderCommit, args.Entries)
		ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		ls.rf.mu.Lock()

		if ls.rf.killed() || ls.rf.currentState != Leader || ls.rf.currentTerm != args.Term {
			ls.rf.mu.Unlock()
			return
		}

		// If this is stale, then update term and convert to follower
		if reply.Term > ls.rf.currentTerm {
			ls.rf.currentTerm = reply.Term
			ls.rf.currentState = Follower
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

			go func() {
				ls.rf.applyQueue <- struct{}{}
			}()

			ls.rf.mu.Unlock()
			return
		} else { //Failure
			if reply.NeedsSnapshot {
				ls.nextIndex[server] = ls.rf.log[0].Index
			} else if reply.XIsShort { //Log too short
				ls.nextIndex[server] = reply.XLen+1
			} else {
				XTermLastIndex := -1
				for _, entry := range ls.rf.log {
					if entry.Term == reply.XTerm {
						XTermLastIndex = entry.Index
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

func (ls *LeaderState) sendSnapshot(server int) {
	DPrintf("%v: -- Leader -- Send snapshot to %v -- lastApplied: %v, commitIndex: %v", ls.rf.me, server, ls.rf.lastApplied, ls.rf.commitIndex)
	for !ls.rf.killed() {
		ls.rf.mu.Lock()
		args := InstallSnapshotArgs{
			Term:              ls.rf.currentTerm,
			LeaderId:          ls.rf.me,
			LastIncludedIndex: ls.rf.log[0].Index,
			LastIncludedTerm:  ls.rf.log[0].Term,
			Data:              ls.rf.snapshot,
		}
		reply := InstallSnapshotReply{}
		ls.rf.mu.Unlock()

		ok := ls.rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		ls.rf.mu.Lock()
		if reply.Term > ls.rf.currentTerm {
			ls.rf.currentTerm = reply.Term
			ls.rf.currentState = Follower
			ls.rf.votedFor = -1
			ls.rf.persist()
			return
		} else if reply.Term != ls.rf.currentTerm {
			return
		}
		ls.nextIndex[server] = args.LastIncludedIndex + 1
		ls.matchIndex[server] = args.LastIncludedIndex
		ls.rf.mu.Unlock()
		return
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

	snapshotIndex := ls.rf.log[0].Index
	for n := maxMatchIndex; n > ls.rf.commitIndex; n-- {
		count := 1
		for server, matchIndex := range ls.matchIndex {
			if server != ls.rf.me && matchIndex >= n {
				count++
			}
		}
		if count > len(ls.matchIndex)/2 &&
			ls.rf.log[n-snapshotIndex].Term == ls.rf.currentTerm {
			ls.rf.commitIndex = n
			break
		}
	}
	ls.rf.mu.Unlock()
}

// sendHeartbeats sends heartsbeats to all peers
func (ls *LeaderState) startSendingHeartbeats() {
	for !ls.rf.killed() {
		ls.rf.mu.Lock()

		if ls.rf.currentState != Leader {
			ls.rf.mu.Unlock()
			return
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
	if ls.rf.currentState != Leader {
		ls.rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:         ls.rf.currentTerm,
		LeaderId:     ls.rf.me,
		PrevLogIndex: ls.rf.log[len(ls.rf.log)-1].Index,
		PrevLogTerm:  ls.rf.log[len(ls.rf.log)-1].Term,
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
		ls.rf.currentState = Follower
		ls.rf.votedFor = -1
		ls.rf.persist()
	}
}
