package raft

// This file contains all attributes and functions related only to the raft
// server in its candidate state.
// Includes election checks


type CandidateState struct {
	rf                *Raft		// this raft instance
	numOfVotes        int		// number of votes received during an election
}

// Initiates an election for a raft server
func (cs *CandidateState) startElection() {
	cs.rf.currentState = Candidate
	cs.rf.currentTerm += 1
	cs.rf.votedFor = cs.rf.me
	cs.numOfVotes = 1
	cs.rf.persist()

	args := RequestVoteArgs{
		Term:         cs.rf.currentTerm,
		LastLogIndex: cs.rf.log[len(cs.rf.log)-1].Index,
		LastLogTerm:  cs.rf.log[len(cs.rf.log)-1].Term,
		CandidateID:  cs.rf.me,
	}

	for i, _ := range cs.rf.peers {
		if i != cs.rf.me {
			go cs.sendRequestVote(i, &args)
		}
	}
}

// isElected returns true if the current raft server has gained the majority of votes
func (cs *CandidateState) isElected() bool {
	isElected := cs.numOfVotes >= len(cs.rf.peers)/2+1
	return isElected
}


// sends a request vote rpc call to the server in params
// requests a vote from
func (cs *CandidateState) sendRequestVote(server int, args *RequestVoteArgs) {
	// for !cs.rf.killed() {
		cs.rf.mu.Lock()
		// If not a candidate or not in the expected voting term, stop vote request
		if cs.rf.currentState != Candidate || cs.rf.currentTerm != args.Term {
			cs.rf.mu.Unlock()
			return
		}
		cs.rf.mu.Unlock()

		reply := RequestVoteReply{}
		ok := cs.rf.peers[server].Call("Raft.RequestVote", args, &reply)

		if !ok {
			return
		}

		cs.rf.mu.Lock()
		defer cs.rf.mu.Unlock()

		// If this is stale, then convert to follower
		if reply.Term > cs.rf.currentTerm {
			cs.rf.currentTerm = reply.Term
			cs.rf.currentState = Follower
			cs.rf.votedFor = -1
			cs.rf.persist()

		// Else if vote granted and this is still the same election then update numOfvotes
		} else if reply.VoteGranted && 
		cs.rf.currentState == Candidate && 
		args.Term == cs.rf.currentTerm {
			cs.numOfVotes += 1
			if cs.isElected() {
				// fmt.Printf("S%d I'm elected. %d\n", cs.rf.me, cs.rf.currentTerm)
				cs.rf.currentState = Leader
				for i, _ := range cs.rf.peers {
					if i != cs.rf.me {
						cs.rf.leaderNotifyChan[i] <- cs.rf.currentTerm
					}
				}
			}
		}
	// }
}
