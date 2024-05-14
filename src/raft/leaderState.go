package raft

// This file contains all attributes and functions related only to the raft
// server in its leader state.
// Includes replication of logs and heartbeats

import (
	"sync"
	"time"
)

type LeaderState struct {
	rf         *Raft			// this raft instance
	nextIndex  map[int]int		// maps the next entry to send to each follower (default length of leader log + 1)
	matchIndex map[int]int		// maps the last known replicated entry for each follower (default 0)
	cond       *sync.Cond		// cond for synchronization
}

// Initializes leader state and routines after winning an election
func (ls *LeaderState) init() {
	lastLogIndex := ls.rf.log[len(ls.rf.log)-1].Index
	for key := range ls.rf.peers {
		ls.nextIndex[key] = lastLogIndex + 1
		ls.matchIndex[key] = 0
	}
	go ls.startSendingHeartbeats(ls.rf.currentTerm)
	go ls.logSender(ls.rf.currentTerm)
}

// Subroutine that triggers the replication of logs to followers
func (ls *LeaderState) logSender(origTerm int) {
	for !ls.rf.killed() {
		ls.rf.mu.Lock()

		if ls.rf.currentState != Leader || ls.rf.currentTerm > origTerm {
			defer ls.rf.mu.Unlock()
			return
		}

		// Send logs
		ls.sendLogs()
		ls.cond.Wait()
		ls.rf.mu.Unlock()
	}
}

// Sends log to follower server if last log index is at least next index for that server
func (ls *LeaderState) sendLogs() {
	lastLogIndex := ls.rf.log[len(ls.rf.log)-1].Index
	for server, value := range ls.nextIndex {
		if server != ls.rf.me && lastLogIndex >= value {
			go ls.sendLog(server, ls.rf.currentTerm)
		}
	}
}

// Send logs to server
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

		// Don't have the required entry in log so send snapshot instead
		if nextIndex <= snapshotIndex && snapshotIndex != 0 {
			go ls.sendSnapshot(server, ls.rf.currentTerm)
			ls.rf.mu.Unlock()
			return
		}

		// Follower expects entry past my log so update nextIndex
		if lastEntryIndex < nextIndex {
			ls.nextIndex[server] = lastEntryIndex
			ls.rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:         term,
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

		if ls.rf.currentState != Leader || ls.rf.currentTerm != term || ls.rf.currentTerm > term {
			defer ls.rf.mu.Unlock()
			return
		}

		// If this is stale, then update term and convert to follower
		if reply.Term > ls.rf.currentTerm {
			ls.rf.currentTerm = reply.Term
			ls.rf.currentState = Follower
			ls.rf.votedFor = -1
			defer ls.rf.persist()
			defer ls.rf.mu.Unlock()
			return
		}
		
		// If log replication was a success
		if reply.Success {
			lastLogEntry := len(args.Entries) - 1
			if lastLogEntry >= 0 {
				ls.nextIndex[server] = args.Entries[lastLogEntry].Index + 1
				ls.matchIndex[server] = args.Entries[lastLogEntry].Index
				ls.checkForCommits()
			}

			go func() {
				ls.rf.applyQueue <- struct{}{}
			}()

			ls.rf.mu.Unlock()
			return
		} else { //Failure
			//  Case 0: follower asked the leader for a snapshot
			// 	Case 1: leader doesn't have XTerm:
			// 		nextIndex = XIndex
			//  Case 2: leader has XTerm:
			// 		nextIndex = leader's last entry for XTerm
			//  Case 3: follower's log is too short:
			// 		nextIndex = XLen
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

func (ls *LeaderState) sendSnapshot(server int, term int) {
	ls.rf.mu.Lock()
	if ls.rf.currentState != Leader || ls.rf.currentTerm > term {
		defer ls.rf.mu.Unlock()
		return
	}

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
		return
	}

	ls.rf.mu.Lock()
	defer ls.rf.mu.Unlock()
	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.currentState = Follower
		ls.rf.votedFor = -1
		defer ls.rf.persist()
		return
	} else if reply.Term != ls.rf.currentTerm {
		return
	}

	if reply.Success {
		ls.matchIndex[server] = max(ls.matchIndex[server], args.LastIncludedIndex)
		ls.nextIndex[server] = ls.matchIndex[server] + 1
	}
	return
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (ls *LeaderState) checkForCommits() {
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
}

// Subroutine that continuously sends heartsbeats to all peers
func (ls *LeaderState) startSendingHeartbeats(origTerm int) {
	for !ls.rf.killed() {
		ls.rf.mu.Lock()

		if ls.rf.currentState != Leader || ls.rf.currentTerm > origTerm {
			ls.rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:         origTerm,
			LeaderId:     ls.rf.me,
			PrevLogIndex: ls.rf.log[len(ls.rf.log)-1].Index,
			PrevLogTerm:  ls.rf.log[len(ls.rf.log)-1].Term,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: ls.rf.commitIndex,
		}

		ls.rf.mu.Unlock()

		for i := range ls.rf.peers {
			if i != ls.rf.me {
				go ls.sendHeartbeat(i, &args)
			}
		}

		heartbeatSleepTime := 200
		time.Sleep(time.Duration(heartbeatSleepTime) * time.Millisecond)
	}
}

// Sends a heartbeat RPC call to the server in param
func (ls *LeaderState) sendHeartbeat(server int, args *AppendEntriesArgs) {
	ls.rf.mu.Lock()
	// If not in leader state anymore, don't send heartbeat
	if ls.rf.currentState != Leader || ls.rf.currentTerm > args.Term {
		ls.rf.mu.Unlock()
		return
	}

	ls.rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := ls.rf.peers[server].Call("Raft.AppendEntries", args, &reply)

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
