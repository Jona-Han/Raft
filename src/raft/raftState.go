package raft

type RaftState interface {
	StartElection()
	TimedOut()
}