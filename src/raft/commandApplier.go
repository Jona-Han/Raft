package raft

// Subroutine that continously checks if there are any new snapshot/log entries that
// should be sent to the service
func (rf *Raft) commandApplier() {
	for !rf.killed() {
		msg := <- rf.applyChQueue
		rf.applyCh <- *msg
	}
}

func (rf *Raft) commandCommitter() {
	for !rf.killed() {
		idx := <-rf.applyMsgCh

		rf.mu.Lock()
		if rf.currentState != Leader {
			rf.mu.Unlock()
			continue
		}
		snapIndex := rf.log[0].Index

		// If deleted or already committed
		if idx > snapIndex+len(rf.log)-1 || idx <= snapIndex {
			rf.mu.Unlock()
			continue
		}

		ci := rf.commitIndex
		for ci < idx {
			nextIndex := ci + 1

			req := (len(rf.peers)/2 + 1) - 1
			for i, _ := range rf.peers {
				if i != rf.me && rf.leaderState.matchIndex[i] >= nextIndex {
					req -= 1
				}
			}
			if req <= 0 {
				ci += 1
			} else {
				break
			}
		}

		// Figure 8 requirement - only update if in current term
		if rf.currentTerm == rf.log[ci-snapIndex].Term {
			rf.commitIndex = ci
		}

		// apply entries
		for rf.lastApplied < rf.commitIndex {
			entry := rf.log[rf.lastApplied-snapIndex+1]
			rf.applyChQueue <- &ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.Index,
				CommandTerm:   entry.Term,
			}

			rf.lastApplied += 1
		}

		rf.mu.Unlock()
	}
}