package raft

import (
	"time"
)

type CandidateState struct {
	rf                *Raft
	numOfVotes        int
}

// startElection initiates an election for a raft server
func (cs *CandidateState) startElection() {
	cs.rf.mu.Lock()

	if cs.rf.currentState == Leader || cs.rf.killed() {
		cs.rf.mu.Unlock()
		return
	}

	cs.rf.heartbeat = true
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
	cs.rf.mu.Unlock()

	for i := range cs.rf.peers {
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


// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
func (cs *CandidateState) sendRequestVote(server int, args *RequestVoteArgs) {
	for !cs.rf.killed() {
		cs.rf.mu.Lock()
		// If not a candidate or not in the expected voting term, stop vote request
		if cs.rf.currentState != Candidate || cs.rf.currentTerm != args.Term || cs.rf.killed() {
			cs.rf.mu.Unlock()
			return
		}
		cs.rf.mu.Unlock()

		reply := RequestVoteReply{}
		ok := cs.rf.peers[server].Call("Raft.RequestVote", args, &reply)

		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		cs.rf.mu.Lock()
		defer cs.rf.mu.Unlock()

		// If this is stale, then convert to follower
		if reply.Term > cs.rf.currentTerm {
			cs.rf.currentTerm = reply.Term
			cs.rf.currentState = Follower
			cs.rf.votedFor = -1
			cs.rf.persist()
			return
		}

		// Else if vote granted and this is still the same election then update numOfvotes
		if cs.rf.currentState == Candidate && args.Term == cs.rf.currentTerm {
			if reply.VoteGranted {
				cs.numOfVotes += 1
				if cs.isElected() {
					cs.rf.leaderState.init()
					cs.rf.currentState = Leader
					go cs.rf.leaderState.startSendingHeartbeats()
				}
			}
		}
		return
	}
}
