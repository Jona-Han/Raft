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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "cpsc416/labgob"
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
	Command string
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentState string
	candidateState CandidateState
	followerState FollowerState
	leaderState LeaderState

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex       int
	lastApplied       int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeader()
	rf.mu.Unlock()
	return term, isLeader
}

// Initializes initial state in raft struct
func (rf *Raft) init() {
	rf.transitionToFollower()
	rf.candidateState = CandidateState{ 
		rf: rf,
		numOfVotes: 0,
		electionTimeout: 400,
	}
	rf.followerState = FollowerState{ 
		rf: rf,
		lastHeartbeatTime: time.Now(),
		heartbeatTimeout: 300,
	}
	rf.leaderState = LeaderState{ 
		rf: rf,
		heartbeatsStarted: false,
	}

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 0)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If candidate is stale then reject
	if args.Term < rf.currentTerm { // Candidate is of lesser term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Else if this is stale then become follower and grant vote, no matter what state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.transitionToFollower()
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// Else if this hasn't voted for anything this term or has already tried to vote for the candidate, then grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// Else this has voted for someone else already this term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If stale, then reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Candidate and got appendEntries from new leader with same term
	if rf.isCandidate() && args.Term == rf.currentTerm {
		rf.transitionToFollower()
	} else if args.Term > rf.currentTerm { // Any state and got appendEntries from new leader
		rf.currentTerm = args.Term
		rf.transitionToFollower()
		rf.votedFor = -1
	}

	// Log entries and commit index handling here in the future

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.updateLastHeartbeat()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
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
			} else if rf.candidateState.isElected() {
				rf.transitionToLeader()
			}
		} else if rf.isLeader() {
			// Send heartbeat AppendEntries RPCs to all followers.
			rf.transitionToLeader()
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 250)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
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
	if rf.currentState != "leader" && rf.leaderState.heartbeatsStarted == false {
		rf.leaderState.heartbeatsStarted = true
		rf.currentState = "leader"
		go rf.leaderState.sendHeartbeats()
	}
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

