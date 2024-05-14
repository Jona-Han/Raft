package raft

import (
	"fmt"
)

type LogTopic int

const (
	LogTopicSyncEntries LogTopic = iota
	LogTopicRejectAppendEntry
	LogTopicMatchPrevApe
	LogTopicTruncateLogApe
	LogTopicLogUpdateApe
	LogTopicAppendEntryRpc
	LogTopicUpdateCommitIdxApe
	LogTopicAppendingEntryApe
	LogTopicCommittingEntriesApe
	LogTopicStartCmd
	LogTopicElection
	LogTopicHeartbeat
	LogTopicSnapshots
	LogTopicInstallSnapshotRPC
	LogTopicSendInstallSnapshot
)
const TermColor string = "\x1b[0m"

type Logger struct {
	sid int
}

func NewLogger(sId int) (*Logger, error) {
	logger := &Logger{
		sid: sId,
	}
	return logger, nil
}

func (l *Logger) Log(topic LogTopic, message string) {
	topicStr, _ := l.topicToString(topic)
	color := l.sIdToColor(l.sid)

	// Center the topic string within 35 characters
	leftAlgn := fmt.Sprintf("S%d [%s]", l.sid, topicStr)

	if Debug {
		fmt.Printf("%s%-22s:%s %s\n", color, leftAlgn, "\x1b[0m", message)
	}
}

func (l *Logger) sIdToColor(sId int) string {
	switch {
	case sId == 0:
		return "\x1b[31m" // Red color
	case sId == 1:
		return "\x1b[32m" // Green color
	case sId == 2:
		return "\x1b[33m" // Yellow color
	case sId == 3:
		return "\x1b[34m" // Blue color
	case sId == 4:
		return "\x1b[35m" // Magenta color
	case sId == 5:
		return "\x1b[36m" // Cyan color
	case sId == 6:
		return "\x1b[37m" // White color
	case sId == 7:
		return "\x1b[91m" // Light red color
	case sId == 8:
		return "\x1b[92m" // Light green color
	case sId == 9:
		return "\x1b[93m" // Light yellow color
	default:
		rotatingColors := []string{"\x1b[94m", "\x1b[95m", "\x1b[96m", "\x1b[97m"}
		return rotatingColors[sId%4] // Rotate between the available colors
	}

}
func (l *Logger) topicToString(topic LogTopic) (string, string) {
	switch topic {
	case LogTopicSyncEntries:
		return "SYNC_ENT", "\x1b[34m" // Blue color
	case LogTopicRejectAppendEntry:
		return "APNDE_REJ", "\x1b[31m" // Red color
	case LogTopicAppendingEntryApe:
		return "APNDING", "\x1b[32m" // Green color
	case LogTopicCommittingEntriesApe:
		return "CMT", "\x1b[33m" // Yellow color
	case LogTopicUpdateCommitIdxApe:
		return "U_CMT_IDX", "\x1b[35m" // Magenta color
	case LogTopicMatchPrevApe:
		return "APNDE_PREV_MATCH", "\x1b[36m" // Cyan color
	case LogTopicTruncateLogApe:
		return "APNDE_TRUNC_LOG", "\x1b[37m" // White color
	case LogTopicLogUpdateApe:
		return "APNDE_LOG_UPDATE", "\x1b[91m" // Light red color
	case LogTopicAppendEntryRpc:
		return "APNDE_RPC", "\x1b[92m" // Light green color
	case LogTopicStartCmd:
		return "START_RPC", "\x1b[93m" // Light yellow color
	case LogTopicElection:
		return "ELECTION", "\x1b[94m" // Light blue color
	case LogTopicHeartbeat:
		return "HEART_BEAT", "\x1b[95m" // Light magenta color
	case LogTopicSnapshots:
		return "SNAPSHOT_RPC", "\x1b[94m" // Light blue color
	case LogTopicInstallSnapshotRPC:
		return "INSTALL_SNAPSHOT_RPC", "\x1b[94m"
	case LogTopicSendInstallSnapshot:
		return "SENDING_INSTALL_SNP", "\x1b[94m"
	default:
		return "MISC", "\x1b[97m" // Bright white color
	}
}
