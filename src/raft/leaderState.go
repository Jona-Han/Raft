package raft

// This file contains all attributes and functions related only to the raft
// server in its leader state.
// Includes replication of logs and heartbeats

import (
	"time"
)

type LeaderState struct {
	rf         *Raft			// this raft instance
	nextIndex  map[int]int		// maps the next entry to send to each follower (default length of leader log + 1)
	matchIndex map[int]int		// maps the last known replicated entry for each follower (default 0)
}

// Subroutine that triggers the replication of logs to followers
func (ls *LeaderState) logSender(server int) {
	for !ls.rf.killed() {
		term := <-ls.rf.leaderNotifyChan[server]
		
		ls.rf.mu.Lock()
		ls.matchIndex[server] = 0
		ls.nextIndex[server] = ls.rf.log[0].Index + len(ls.rf.log)
		ls.rf.mu.Unlock()

		for !ls.rf.killed() {
			ls.rf.mu.Lock()
			if term != ls.rf.currentTerm {
				break
			}
			ls.rf.mu.Unlock()
			go func(term int) {
				if term != ls.rf.currentTerm {
					return // fast exit
				}
				ls.rf.mu.Lock()
				defer ls.rf.mu.Unlock()

				matchIndex := ls.matchIndex[server]
				nextIndex := ls.nextIndex[server]
				snapshotIndex := ls.rf.log[0].Index
	
				if nextIndex == matchIndex || nextIndex <= snapshotIndex {
					ls.sendSnapshot(server)
				} else {
					ls.sendLog(server, term)
				}
			}(term)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Send logs to server
func (ls *LeaderState) sendLog(server int, term int) {
	// While logs are not up to date and successful, continue loop
	nextIndex := ls.nextIndex[server]
	snapshotIndex := ls.rf.log[0].Index

	entriesToSend := make([]LogEntry, 0)
	if nextIndex >= snapshotIndex+len(ls.rf.log) {
		//heartbeat
	} else {
		entriesToSend = append(entriesToSend, ls.rf.log[nextIndex-snapshotIndex:]...)
	}
	prevEntry := ls.rf.log[nextIndex-snapshotIndex-1]

	args := AppendEntriesArgs{
		Term:              term,
		LeaderId:          ls.rf.me,
		PrevLogIndex:      prevEntry.Index,
		PrevLogTerm:       prevEntry.Term,
		LeaderCommit: 	   ls.rf.commitIndex,
		Entries:           entriesToSend,
	}

	ls.rf.mu.Unlock()
	reply := AppendEntriesReply{}
	// fmt.Printf("S%d Sending log to %d. \n", ls.rf.me, server)
	ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	ls.rf.mu.Lock()

	if !ok {
		return
	}

	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.currentState = Follower
		ls.rf.votedFor = -1
		ls.rf.persist()
		return
	}
	if reply.Term < ls.rf.currentTerm {
		return
	}
	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		newNextIndex := newMatchIndex + 1
		if newMatchIndex > ls.matchIndex[server] {
			ls.matchIndex[server] = newMatchIndex
			ls.rf.applyMsgCh <- newMatchIndex
		}
		if newNextIndex > ls.nextIndex[server] {
			ls.nextIndex[server] = newNextIndex
		}
	} else {
		newNextIndex := reply.XIndex
		if newNextIndex >= ls.matchIndex[server]+1 || snapshotIndex > 0 {
			// if base index is zero we dont have snapshot, cant send install snapshot
			// if baseindex > 0 safe to make nextIndex < matchIndex(next time a snapshot will be installed)
			ls.nextIndex[server] = newNextIndex // safe
		}
	}

	
	// // Don't have the required entry in log so send snapshot instead
	// if nextIndex <= snapshotIndex && snapshotIndex != 0 {
	// 	go ls.sendSnapshot(server, ls.rf.currentTerm)
	// 	ls.rf.mu.Unlock()
	// 	return
	// }

	// // Follower expects entry past my log so update nextIndex
	// if lastEntryIndex < nextIndex {
	// 	ls.nextIndex[server] = lastEntryIndex
	// 	ls.rf.mu.Unlock()
	// 	return
	// }

	// args := AppendEntriesArgs{
	// 	Term:         term,
	// 	LeaderId:     ls.rf.me,
	// 	PrevLogIndex: nextIndex - 1,
	// 	PrevLogTerm:  ls.rf.log[nextIndex-snapshotIndex-1].Term,
	// 	Entries:      make([]LogEntry, lastEntryIndex-nextIndex+1),
	// }

	// copy(args.Entries, ls.rf.log[nextIndex-snapshotIndex:])
	// ls.rf.mu.Unlock()

	// reply := AppendEntriesReply{}
	// // DPrintf("LEADER: %v sending logs to server:%v prevLogIndex: %v, len(entries):%v, commitIndex: %v, entries: %v", ls.rf.me, server, args.PrevLogIndex, len(args.Entries), args.LeaderCommit, args.Entries)
	// ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	// if !ok {
	// 	time.Sleep(10 * time.Millisecond)
	// 	continue
	// }

	// ls.rf.mu.Lock()

	// if ls.rf.currentState != Leader || ls.rf.currentTerm != term || ls.rf.currentTerm > term {
	// 	defer ls.rf.mu.Unlock()
	// 	return
	// }

	// // If this is stale, then update term and convert to follower
	// if reply.Term > ls.rf.currentTerm {
		// ls.rf.currentTerm = reply.Term
		// ls.rf.currentState = Follower
		// ls.rf.votedFor = -1
		// defer ls.rf.persist()
	// 	defer ls.rf.mu.Unlock()
	// 	return
	// }
	
	// // If log replication was a success
	// if reply.Success {
	// 	lastLogEntry := len(args.Entries) - 1
	// 	if lastLogEntry >= 0 {
	// 		ls.nextIndex[server] = args.Entries[lastLogEntry].Index + 1
	// 		ls.matchIndex[server] = args.Entries[lastLogEntry].Index
	// 		ls.checkForCommits()
	// 	}

	// 	go func() {
	// 		ls.rf.applyMsgCh <- struct{}{}
	// 	}()

	// 	ls.rf.mu.Unlock()
	// 	return
	// } else { //Failure
	// 	//  Case 0: follower asked the leader for a snapshot
	// 	// 	Case 1: leader doesn't have XTerm:
	// 	// 		nextIndex = XIndex
	// 	//  Case 2: leader has XTerm:
	// 	// 		nextIndex = leader's last entry for XTerm
	// 	//  Case 3: follower's log is too short:
	// 	// 		nextIndex = XLen
	// 	if reply.NeedsSnapshot {
	// 		ls.nextIndex[server] = ls.rf.log[0].Index
	// 	} else if reply.XIsShort { //Log too short
	// 		ls.nextIndex[server] = reply.XLen+1
	// 	} else {
	// 		XTermLastIndex := -1
	// 		for _, entry := range ls.rf.log {
	// 			if entry.Term == reply.XTerm {
	// 				XTermLastIndex = entry.Index
	// 				break
	// 			}
	// 		}

	// 		if XTermLastIndex == -1 {
	// 			ls.nextIndex[server] = reply.XIndex
	// 		} else {
	// 			ls.nextIndex[server] = XTermLastIndex
	// 		}
	// 	}
	// }
}

func (ls *LeaderState) sendSnapshot(server int) {
	if ls.rf.currentState != Leader {
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

	ls.rf.mu.Lock()
	if !ok {
		return
	}

	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.currentState = Follower
		ls.rf.votedFor = -1
		ls.rf.persist()
	} else if reply.Term < ls.rf.currentTerm {
		// ignore
	} else if reply.Success {
		// ls.matchIndex[server] = max(ls.matchIndex[server], args.LastIncludedIndex)
		ls.nextIndex[server] = ls.matchIndex[server] + 1
	}
}