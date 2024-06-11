[![Go](https://github.com/Jona-Han/Raft/actions/workflows/go.yml/badge.svg)](https://github.com/Jona-Han/Raft/actions/workflows/go.yml)

# Raft consensus algorithm


This project implements the Raft consensus algorithm, a replicated state machine protocol designed to ensure consistency across distributed systems. Raft organizes client requests into a log sequence and ensures all replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state, ensuring all live replicas maintain identical service states.

It is based on the paper [Raft: In Search of an Understandable Consensus Algorithm](./docs/RAFT-ATC2014-Extended.pdf)


## Overview of raft protocol

### 1. Cluster
A Raft cluster consists of multiple servers. Each server can be in one of three states:
- **Leader**: Handles client requests and manages log replication.
- **Follower**: Passively replicates logs and responds to requests from the leader.
- **Candidate**: Seeks to become the leader through an election process.

### 2. Terms
Time is divided into terms, which are sequentially numbered periods. Each term begins with an election, and either a leader is elected or a new term begins if the election fails.

### 3. Log
The log is a sequence of entries where each entry contains a command for the state machine. The log is replicated across all servers to ensure consistency.

## Raft Protocol Phases

### 1. Leader Election
A new leader is elected if the current leader fails. The election process involves the following steps:
- A server becomes a candidate and increments its term.
- The candidate requests votes from other servers.
- Servers grant their vote if they haven't voted in the current term and the candidate's log is at least as up-to-date as their own.
- If a candidate receives a majority of votes, it becomes the leader.

### 2. Log Replication
The leader is responsible for replicating log entries to the followers:
- The leader appends the client command to its log.
- The leader sends `AppendEntries` RPCs to followers to replicate the entry.
- Followers append the entry to their logs and respond to the leader.
- Once an entry is replicated on a majority of servers, it is considered committed.

### 3. Safety and Consistency
Raft ensures safety by adhering to the following principles:
- **Election Safety**: At most one leader can be elected in a given term.
- **Leader Completeness**: A leader must have all committed entries from previous terms.
- **Log Matching**: If two logs contain an entry with the same index and term, then the logs are identical up to that entry.
- **State Machine Safety**: If a server has applied a log entry to its state machine, no other server can apply a different command for the same log index.

## Raft Communication

### RPCs Used in Raft
- **RequestVote RPC**: Initiated by candidates to gather votes.
- **AppendEntries RPC**: Used by the leader to replicate log entries and send heartbeats.
- **InstallSnapshot RPC**: Used by leader to replicate snapshot to followers

## Handling Failures

### Leader Failure
If the leader crashes or becomes unreachable, the followers will eventually start a new election. The system continues to function as long as a majority of the servers are operational.

### Network Partitions
Raft can handle network partitions by ensuring that only one leader exists at any given time, and that committed entries are preserved and eventually propagated once the network is restored.

## Implementation Details

### Persistence
Raft servers must persist their state (current term, voted for, log entries) to stable storage before responding to RPCs. This ensures that state is not lost in the event of a crash.

### Snapshotting
To prevent the log from growing indefinitely, Raft uses snapshots to compact the log. The state machine state is periodically saved, and the log entries before the snapshot can be discarded.

## Getting Started

### Dependencies

* Go programming language (version 1.15 or later)
* Git for version control
* Operating System: Linux, macOS, or Windows 10

### Installing

1. **Clone the Repository:**
    ```sh
    git clone https://github.com/Jona-Han/raft.git
    cd raft-consensus-algorithm
    ```

2. **Navigate to the Raft Directory:**
    ```sh
    cd src/raft
    ```

3. **Install Dependencies:**
    Ensure Go is installed and properly configured on your system.

### Executing program

1. **Running Specific Test Suites:**
    ```sh
    go test -run TestInitialElection2A --count 10
    go test -run TestReElection2A --count 10
    go test -run 2A --count 5
    go test -run 2A -race
    ```

2. **Running the Full Suite:**
    ```sh
    go test
    ```
