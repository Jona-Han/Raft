package raft

// subroutine that checks for heartbeat timeout and starts election
// if no heartbeat occurred between successive checks
func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.currentState == Leader {
			rf.electionTimer.Stop()
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm += 1
		rf.currentState = Candidate
		rf.votedFor = rf.me

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
			LastLogIndex: rf.log[len(rf.log)-1].Index,
		}

		rf.electionTimer.Reset(GetRandTimeout())
		rf.mu.Unlock()
		go rf.sendVotes(args)
	}
}

func (rf *Raft) sendVotes(args *RequestVoteArgs) {
	voteChan := make(chan bool, len(rf.peers))
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go rf.sendRequestVote(voteChan, idx, args)
		}
	}

	votesReceived := 1
	votesRequired := len(rf.peers)/2 + 1

	for i := 0; i < len(rf.peers)-1; i++ {
		vote := <-voteChan
		if vote == true {
			votesReceived++
		}
		if votesReceived == votesRequired {
			break
		}
	}

	if votesReceived >= votesRequired {
		rf.mu.Lock()
		if args.Term == rf.currentTerm {
			rf.electionTimer.Stop()
			rf.currentState = Leader
			rf.votedFor = rf.me
			rf.persist()
			for i, _ := range rf.peers {
				if i != rf.me {
					rf.leaderNotifyChan[i] <- rf.currentTerm
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendRequestVote(voteChan chan bool, server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}

	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)

	if ok && reply.VoteGranted {
		voteChan <- true
	} else {
		voteChan <- false
		if ok && reply.Term > args.Term {
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.transitionToFollower(-1)
			}
			rf.mu.Unlock()
		}
	}
}