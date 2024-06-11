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
		if term != ls.rf.currentTerm || ls.rf.currentState != Leader {
			// ignore
			ls.rf.mu.Lock()
			continue
		}
		ls.matchIndex[server] = 0
		ls.nextIndex[server] = ls.rf.log[0].Index + len(ls.rf.log)
		ls.rf.mu.Unlock()

		for !ls.rf.killed() {
			ls.rf.mu.Lock()
			if term != ls.rf.currentTerm {
				ls.rf.mu.Unlock()
				break
			}
			ls.rf.mu.Unlock()
			go func(term int) {
				ls.rf.mu.Lock()
				defer ls.rf.mu.Unlock()
				if term != ls.rf.currentTerm {
					return // fast exit
				}

				matchIndex := ls.matchIndex[server]
				nextIndex := ls.nextIndex[server]
				snapshotIndex := ls.rf.log[0].Index
	
				if nextIndex == matchIndex || nextIndex <= snapshotIndex {
					ls.sendSnapshot(server, term)
				} else {
					ls.sendLog(server, term)
				}
			}(term)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// Send logs to server
func (ls *LeaderState) sendLog(server int, term int) {
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
	ok := ls.rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	ls.rf.mu.Lock()

	if !ok {
		return
	}

	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.transitionToFollower(-1)
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
		ls.nextIndex[server] = max(newNextIndex, ls.nextIndex[server])
	} else {
		newNextIndex := reply.XIndex
		if newNextIndex >= ls.matchIndex[server]+1 || snapshotIndex > 0 {
			ls.nextIndex[server] = newNextIndex
		}
	}
}

func (ls *LeaderState) sendSnapshot(server int, term int) {
	args := InstallSnapshotArgs{
		Term:              term,
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

	if ls.rf.currentTerm != term {
		return
	}

	if reply.Term > ls.rf.currentTerm {
		ls.rf.currentTerm = reply.Term
		ls.rf.transitionToFollower(-1)
	} else if reply.Term == ls.rf.currentTerm{
		ls.nextIndex[server] = ls.matchIndex[server] + 1
	}
}