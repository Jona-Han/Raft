package raft

import "log"
import "math/rand"
import "time"

// Debugging
const Debug = false
const Timeout = time.Millisecond * 300

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func GetRandTimeout() time.Duration {
	return Timeout + time.Duration(rand.Float64()*float64(Timeout))
}
