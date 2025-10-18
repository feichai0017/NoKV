package peer

import myraft "github.com/feichai0017/NoKV/raft"

// ApplyFunc consumes committed raft log entries and applies them to the user
// state machine (LSM, MVCC, etc).
type ApplyFunc func(entries []myraft.Entry) error
