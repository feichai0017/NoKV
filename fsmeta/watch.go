package fsmeta

import (
	"context"
	"errors"
)

var ErrWatchOverflow = errors.New("fsmeta: watch backlog overflow")

// WatchEventSource identifies the raftstore command source that made a key
// visible to MVCC readers.
type WatchEventSource uint8

const (
	WatchEventSourceCommit WatchEventSource = iota + 1
	WatchEventSourceResolveLock
)

// WatchCursor is a per-region raft apply cursor. It is the watch resume key.
type WatchCursor struct {
	RegionID uint64
	Term     uint64
	Index    uint64
}

// WatchRequest subscribes to one fsmeta key prefix.
type WatchRequest struct {
	Mount              MountID
	RootInode          InodeID
	KeyPrefix          []byte
	DescendRecursively bool
	ResumeCursor       WatchCursor
	BackPressureWindow uint32
}

// WatchEvent reports that one key became visible at CommitVersion.
type WatchEvent struct {
	Cursor        WatchCursor
	CommitVersion uint64
	Source        WatchEventSource
	Key           []byte
}

// WatchSubscription is one live fsmeta watch stream.
type WatchSubscription interface {
	Events() <-chan WatchEvent
	Ack(WatchCursor)
	Close()
	Err() error
}

// Watcher owns live watch subscriptions.
type Watcher interface {
	Subscribe(context.Context, WatchRequest) (WatchSubscription, error)
}

// WatchPrefix returns the byte prefix a WatchRequest should match.
func WatchPrefix(req WatchRequest) ([]byte, error) {
	if len(req.KeyPrefix) > 0 {
		if req.Mount != "" || req.RootInode != 0 {
			return nil, ErrInvalidRequest
		}
		return append([]byte(nil), req.KeyPrefix...), nil
	}
	if err := validateMountID(req.Mount); err != nil {
		return nil, err
	}
	if err := validateInodeID(req.RootInode); err != nil {
		return nil, err
	}
	if req.DescendRecursively {
		// Dentry keys are parent-inode scoped, not path-prefix scoped. Recursive
		// subtree watch needs a directory-tree index and is deferred.
		return nil, ErrInvalidRequest
	}
	return EncodeDentryPrefix(req.Mount, req.RootInode)
}
