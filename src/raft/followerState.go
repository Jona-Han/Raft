package raft

import (
	"time"
)

type FollowerState struct {
	rf *Raft
	lastHeartbeatTime time.Time
	heartbeatTimeout  int
}

func (fs *FollowerState) timedOut() (bool) {
	timedOut := time.Since(fs.lastHeartbeatTime) > time.Duration(fs.heartbeatTimeout)*time.Millisecond
	return timedOut
}
