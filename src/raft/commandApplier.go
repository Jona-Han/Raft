package raft

func (rf *Raft) commandApplier() {
	var lastApplied, commitIndex, XIndex int
	var start, end int
	var logCpy []LogEntry

	for !rf.killed() {
		<-rf.applyQueue

		rf.mu.Lock()
		if rf.killed() {
			defer rf.mu.Unlock()
			return
		}

		if len(rf.snapshot) > 0 && rf.lastApplied <= rf.log[0].Index {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.log[0].Index,
			}
		}

		logCpy = logCpy[:0] // empty the log

		lastApplied, commitIndex, XIndex = rf.lastApplied, rf.commitIndex, rf.log[0].Index

		if lastApplied < XIndex {
			lastApplied = max(lastApplied, XIndex)
		}
		if commitIndex < XIndex {
			commitIndex = max(commitIndex, XIndex)
		}
		start = lastApplied - XIndex + 1
		end = commitIndex - XIndex

		if start <= end {
			logCpy = make([]LogEntry, end-start+1)
			copy(logCpy, rf.log[start:end+1])
		}
		rf.mu.Unlock()

		if len(logCpy) > 0 {
			for _, entry := range logCpy {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
			}

			// check if we should update rf.appliedIndex
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, commitIndex)
			rf.mu.Unlock()
		}
	}
}