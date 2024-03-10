package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "cpsc416/labgob"
	"cpsc416/labgob"
	"cpsc416/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentState   string
	candidateState CandidateState
	followerState  FollowerState
	leaderState    LeaderState

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	snapshot []byte
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeader()
	rf.mu.Unlock()
	return term, isLeader
}

// Initializes initial state in raft struct
func (rf *Raft) init() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.transitionToFollower()
	rf.candidateState = CandidateState{
		rf:              rf,
		numOfVotes:      0,
		electionTimeout: 300,
	}
	rf.followerState = FollowerState{
		rf:                rf,
		lastHeartbeatTime: time.Now(),
		heartbeatTimeout:  300,
	}
	rf.leaderState = LeaderState{
		rf:         rf,
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
		cond:       sync.NewCond(&rf.mu),
		hbCond:     sync.NewCond(&rf.mu),
	}

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)

	rf.log[0] = LogEntry{
		Index: 0,
		Term:  0,
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()

	if rf.snapshot != nil {
		rf.persister.Save(raftstate, rf.snapshot)
	} else {
		rf.persister.Save(raftstate, nil)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("Error decoding persist")
		return
	}

	//success
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log[0].Index {
		// Ignore if index is not greater than current snapshotIndex
		return
	}

	// Update snapshot data
	rf.snapshot = snapshot
	// rf.snapshotIndex = index
	// rf.snapshotTerm = rf.log[index-rf.log[0].Index].Term // Adjust for potential offset in log indexing

	rf.log[0] = LogEntry{
		Index: index,
		Term:  rf.log[index-rf.log[1].Index].Term,
	}

	// Discard log entries before index
	rf.log = rf.log[index-rf.log[0].Index:]

	// Persist state and snapshot
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm

	//If not leader, just return false
	if !rf.isLeader() {
		return index, term, false
	}

	//Else add command
	newEntry := LogEntry{
		Index:   index,
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, newEntry)
	rf.leaderState.cond.Signal()
	rf.persist()
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.leaderState.cond.Broadcast()
	rf.leaderState.hbCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	ms := 300 + (rand.Int63() % 150)
	time.Sleep(time.Duration(ms) * time.Millisecond)

	for !rf.killed() {
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.isFollower() {
			// If the server is a follower, check if it has not received any heartbeat for a timeout.
			if rf.followerState.timedOut() {
				rf.transitionToCandidate()
				go rf.candidateState.startElection()
			}
		} else if rf.isCandidate() {
			//Check for timeout
			if rf.candidateState.timedOut() {
				go rf.candidateState.startElection()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.logSender()
	go rf.commandApplier(applyCh)
	go rf.leaderState.sendHeartbeats()

	return rf
}

func (rf *Raft) commandApplier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			applyCh <- applyMsg
		}

		rf.mu.Unlock()
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) logSender() {
	for !rf.killed() {
		rf.mu.Lock()

		if !rf.isLeader() {
			rf.leaderState.cond.Wait()
		}

		// Send logs
		rf.leaderState.sendLogs()
		rf.leaderState.cond.Wait()
		rf.mu.Unlock()
	}
}

// UpdateLastHeartbeat updates the last heartbeat time for the follower state.
func (rf *Raft) updateLastHeartbeat() {
	rf.followerState.lastHeartbeatTime = time.Now()
}

// TransitionToFollower transitions Raft to the follower state.
func (rf *Raft) transitionToFollower() {
	rf.currentState = "follower"
}

// TransitionToCandidate transitions Raft to the candidate state.
func (rf *Raft) transitionToCandidate() {
	rf.currentState = "candidate"
}

// TransitionToLeader transitions Raft to the leader state and starts sending heartbeats if not already started.
func (rf *Raft) transitionToLeader() {
	rf.leaderState.init()
	rf.currentState = "leader"
	rf.leaderState.hbCond.Signal()
}

// IsFollower returns true if Raft is in the follower state.
func (rf *Raft) isFollower() bool {
	return rf.currentState == "follower"
}

// IsLeader returns true if Raft is in the leader state.
func (rf *Raft) isLeader() bool {
	return rf.currentState == "leader"
}

// IsCandidate returns true if Raft is in the candidate state.
func (rf *Raft) isCandidate() bool {
	return rf.currentState == "candidate"
}
