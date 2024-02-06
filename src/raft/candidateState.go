package raft

import (
	"time"
)

type CandidateState struct {
	rf                *Raft
	numOfVotes        int
	electionStartTime time.Time
	electionTimeout   int
}

func (cs *CandidateState) startElection() {
	cs.rf.mu.Lock()

	cs.rf.currentTerm += 1
	cs.rf.votedFor = cs.rf.me
	cs.numOfVotes = 1
	cs.electionStartTime = time.Now()

	currentTerm := cs.rf.currentTerm

	cs.rf.mu.Unlock()

	for i := range cs.rf.peers {
		if i != cs.rf.me {
			go cs.sendRequestVote(i, currentTerm)
		}
	}
}

func (cs *CandidateState) isElected() (bool) {
	isElected := cs.numOfVotes > (len(cs.rf.peers) / 2 + 1)
	return isElected
}

func (cs *CandidateState) timedOut() (bool) {
	timedOut := time.Since(cs.electionStartTime) > time.Duration(cs.electionTimeout)*time.Millisecond
	return timedOut
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
//
func (cs *CandidateState) sendRequestVote(server int) {
	// Check if still in candidate state
	cs.rf.mu.Lock()
	if !cs.rf.isCandidate() {
		// If not in candidate state anymore, don't send request votes
		cs.rf.mu.Unlock()
		return
	}

	args := RequestVoteArgs{
		Term:        cs.rf.currentTerm,
		CandidateID: cs.rf.me,
	}
	cs.rf.mu.Unlock()

	reply := RequestVoteReply{}
	ok := cs.rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	if !ok return


	cs.rf.mu.Lock()
	defer cs.rf.mu.Unlock()

	// If this is stale, then convert to follower
	if reply.Term > cs.rf.currentTerm {
		cs.rf.currentTerm = reply.Term
		cs.rf.transitionToFollower()
		cs.rf.votedFor = -1
	}

	// Else if vote granted, if still a candidate, then update numOfvotes
	else if (cs.rf.isCandidate()) {
		if reply.VoteGranted {
			cs.numOfVotes++
			if cs.isElected() {
				cs.rf.transitionToLeader()
				go cs.rf.sendHeartbeats()
			}
		}
	}
}
