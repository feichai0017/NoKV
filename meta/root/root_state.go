package root

import rootstate "github.com/feichai0017/NoKV/meta/root/state"

type State = rootstate.State
type Snapshot = rootstate.Snapshot

// CloneSnapshot returns a detached copy of one rooted metadata snapshot.
func CloneSnapshot(snapshot Snapshot) Snapshot { return rootstate.CloneSnapshot(snapshot) }
