package raft

type Snapshot struct {
	lastIncludedIndex int
	lastIncludedTerm  int
}
