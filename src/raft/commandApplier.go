package raft

// Subroutine that continously checks if there are any new snapshot/log entries that
// should be sent to the service
func (rf *Raft) commandApplier() {
	var lastApplied, commitIndex, snapIndex int
	var start, end int
	var logCpy []LogEntry

	for !rf.killed() {
		<-rf.applyQueue

		rf.mu.Lock()
		// If last applied is less than current snapshot, then send snapshot
		if len(rf.snapshot) > 0 && rf.lastApplied <= rf.log[0].Index {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.log[0].Index,
			}
		}

		logCpy = logCpy[:0] // empty the log
		lastApplied, commitIndex, snapIndex = rf.lastApplied, rf.commitIndex, rf.log[0].Index

		// Check to update lastApplie and commitIndex
		lastApplied = max(lastApplied, snapIndex)
		commitIndex = max(commitIndex, snapIndex)

		start = lastApplied - snapIndex + 1
		end = commitIndex - snapIndex

		if start <= end {
			logCpy = make([]LogEntry, end-start+1)
			copy(logCpy, rf.log[start:end+1])
		}
		rf.mu.Unlock()

		// If there are new log entries committed, then send to service
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