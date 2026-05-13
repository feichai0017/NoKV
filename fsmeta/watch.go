package fsmeta

import "context"

// WatchEventSource identifies the raftstore command source that made a key
// visible to MVCC readers.
type WatchEventSource uint8

const (
	WatchEventSourceCommit WatchEventSource = iota + 1
	WatchEventSourceResolveLock
	WatchEventSourcePerasVisible
)

// ApplyEvent is the fsmeta watch router's input event. Runtime adapters convert
// their storage-engine apply notifications into this neutral shape before
// publishing through fsmeta/exec/watch.Router.
type ApplyEvent struct {
	RegionID      uint64
	Term          uint64
	Index         uint64
	Source        WatchEventSource
	CommitVersion uint64
	Keys          [][]byte
}

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
	ReadyCursor() WatchCursor
	Ack(WatchCursor)
	Close()
	Err() error
}

// Watcher owns live watch subscriptions.
type Watcher interface {
	Subscribe(context.Context, WatchRequest) (WatchSubscription, error)
}

// SnapshotPublisher records published subtree snapshot epochs into an authority
// layer such as meta/root. Implementations must not store per-dentry data.
type SnapshotPublisher interface {
	PublishSnapshotSubtree(context.Context, SnapshotSubtreeToken) error
	RetireSnapshotSubtree(context.Context, SnapshotSubtreeToken) error
}

// SnapshotPublisherFunc adapts a function into SnapshotPublisher.
type SnapshotPublisherFunc func(context.Context, SnapshotSubtreeToken) error

func (f SnapshotPublisherFunc) PublishSnapshotSubtree(ctx context.Context, token SnapshotSubtreeToken) error {
	if f == nil {
		return nil
	}
	return f(ctx, token)
}

func (f SnapshotPublisherFunc) RetireSnapshotSubtree(context.Context, SnapshotSubtreeToken) error {
	return nil
}

// WatchPrefix returns an explicit byte prefix a WatchRequest should match.
// Public callers cannot derive storage prefixes from a string mount name; the
// runtime boundary must first resolve rooted mount admission and then call
// WatchPrefixForMount.
func WatchPrefix(req WatchRequest) ([]byte, error) {
	if len(req.KeyPrefix) > 0 {
		if req.Mount != "" || req.RootInode != 0 {
			return nil, ErrInvalidRequest
		}
		return append([]byte(nil), req.KeyPrefix...), nil
	}
	return nil, ErrInvalidRequest
}

// WatchPrefixForMount returns the dentry prefix for a resolved mount identity.
func WatchPrefixForMount(req WatchRequest, mount MountIdentity) ([]byte, error) {
	if len(req.KeyPrefix) > 0 {
		return WatchPrefix(req)
	}
	if err := validateMountIdentityForRequest(mount, req.Mount); err != nil {
		return nil, err
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
	return EncodeDentryPrefix(mount, req.RootInode)
}
