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
	"log"

	// "cpsc416/labgob"
	"cpsc416/labgob"
	"cpsc416/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer sends an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

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

	logger *Logger

	currentState   int
	candidateState CandidateState
	leaderState    LeaderState

	leaderNotifyChan []chan int

	heartbeat bool				// keeps track of the last heartbeat

	currentTerm int				// current term at this Raft
	votedFor    int				// the peer this Raft voted for during the last election
	log         []LogEntry		// the logs of the current Raft

	commitIndex int				// index of highest log entry known to be commited (initalized to be 0)
	lastApplied int				// index of highes log entry known to be applied to the SM (initalized to be 0)

	snapshot  []byte			// latest snapshot, snapshot index at log[0].Index, term at log[0].Term
	
	applyCh   chan ApplyMsg		// channel to pass results to the server
	applyChQueue chan *ApplyMsg
	applyMsgCh chan int	// channel to signal commandApplier
}

// Returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.currentState == Leader
	rf.mu.Unlock()
	return term, isLeader
}

// Initializes initial state in raft struct
func (rf *Raft) init() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentState = Follower
	rf.candidateState = CandidateState{
		rf:              rf,
		numOfVotes:      0,
	}
	rf.leaderState = LeaderState{
		rf:         rf,
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
	}

	rf.heartbeat = false
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderNotifyChan = make([]chan int, len(rf.peers))
	rf.log = make([]LogEntry, 1)
	rf.applyMsgCh = make(chan int, 1000)
	rf.applyChQueue = make(chan *ApplyMsg, 10000)
	rf.snapshot = nil

	rf.log[0] = LogEntry{
		Index: 0,
		Term:  0,
	}
}

// Saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)
}

// Restores previously persisted state.
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	rf.snapshot = rf.persister.ReadSnapshot()

	if data == nil || len(data) < 1 {
		rf.snapshot = nil
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {

		log.Fatal("Error decoding persist")
		return
	}

	//success
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logs
	rf.lastApplied = rf.log[0].Index
	rf.commitIndex = rf.log[0].Index
}

// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft now trims its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	snapIndex := rf.log[0].Index

	if index <= snapIndex {
		return
	}

	if index > rf.log[len(rf.log)-1].Index {
		log.Fatal("Snapshot index past the end of")
	}
	var reapplyLogs []LogEntry

	if rf.lastApplied > index {
		reapplyLogs = append(reapplyLogs, rf.log[index+1-snapIndex:rf.lastApplied-snapIndex+1]...)
	}

	// Update snapshot data
	rf.snapshot = snapshot
	rf.log = rf.log[index-snapIndex:]
	rf.persist()

	rf.applyChQueue <- &ApplyMsg{
		SnapshotValid: true,
		Snapshot:		rf.snapshot,
		SnapshotTerm:	rf.log[0].Term,
		SnapshotIndex:	rf.log[0].Index,
	}

	for _, entry := range reapplyLogs {
		rf.applyChQueue <- &ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  entry.Index,
			CommandTerm:   entry.Term,
		}
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.leaderState.matchIndex[i] < rf.log[0].Index {
				rf.leaderState.matchIndex[i] = rf.log[0].Index
			}
			if rf.leaderState.nextIndex[i] <= rf.leaderState.matchIndex[i] {
				rf.leaderState.nextIndex[i] = rf.leaderState.matchIndex[i] + 1
			}
		}
	}
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

	index := rf.log[len(rf.log)-1].Index + 1
	term := rf.currentTerm

	//If not leader, just return false
	if rf.currentState != Leader {
		return -1, -1, false
	}

	//Else add command
	newEntry := LogEntry{
		Index:   index,
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyMsgCh <- 5
}

// returns whether the raft instance has been killed by the tester
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// subroutine that checks for heartbeat timeout and starts election
// if no heartbeat occurred between successive checks
func (rf *Raft) ticker() {
	for !rf.killed() {
		ms := 150 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.currentState != Leader && !rf.heartbeat {
			rf.candidateState.startElection()
		}
		rf.heartbeat = false
		rf.mu.Unlock()
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
	rf.applyCh = applyCh

	logger, err := NewLogger(me)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}
	rf.logger = logger


	rf.init()
	for i, _ := range peers {
		if i != me {
			rf.leaderNotifyChan[i] = make(chan int, 1000)
			go rf.leaderState.logSender(i)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist()

	// start goroutines
	go rf.ticker()
	go rf.commandApplier()
	go rf.commandCommitter()

	return rf
}