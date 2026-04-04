package file

import (
	"errors"
	"os"
	"path/filepath"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

type fileCheckpointStore struct {
	fs      vfs.FS
	workdir string
}

// LoadCheckpoint reads the rooted compact state image from
// root.checkpoint.binpb.
//
// On-disk format:
//   - one binary protobuf blob encoded as metapb.RootCheckpoint
//
// Compatibility note:
//   - older workdirs may still contain a checkpoint encoded as metapb.RootState
//     without descriptor materialization; that legacy payload is still accepted
//     here so storage recovery stays monotonic even though the canonical format
//     is now RootCheckpoint.
func (s fileCheckpointStore) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	path := filepath.Join(s.workdir, CheckpointFileName)
	data, err := s.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootstorage.Checkpoint{
				Snapshot: rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
			}, nil
		}
		return rootstorage.Checkpoint{}, err
	}
	if len(data) == 0 {
		return rootstorage.Checkpoint{
			Snapshot: rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
		}, nil
	}
	var pbCheckpoint metapb.RootCheckpoint
	if err := proto.Unmarshal(data, &pbCheckpoint); err != nil {
		return rootstorage.Checkpoint{}, err
	}
	if pbCheckpoint.State == nil && len(pbCheckpoint.Descriptors) == 0 {
		var pbState metapb.RootState
		if err := proto.Unmarshal(data, &pbState); err == nil {
			return rootstorage.Checkpoint{
				Snapshot: rootstate.Snapshot{
					State:       metacodec.RootStateFromProto(&pbState),
					Descriptors: make(map[uint64]descriptor.Descriptor),
				},
			}, nil
		}
	}
	snapshot, logOffset := metacodec.RootSnapshotFromProto(&pbCheckpoint)
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	return rootstorage.Checkpoint{Snapshot: snapshot, TailOffset: int64(logOffset)}, nil
}

// SaveCheckpoint publishes a new rooted checkpoint atomically.
//
// Write path:
//   - marshal metapb.RootCheckpoint
//   - write to <file>.tmp
//   - fsync temp file
//   - rename over the final path
//   - fsync parent directory
func (s fileCheckpointStore) SaveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	payload, err := proto.Marshal(metacodec.RootSnapshotToProto(checkpoint.Snapshot, uint64(checkpoint.TailOffset)))
	if err != nil {
		return err
	}
	path := filepath.Join(s.workdir, CheckpointFileName)
	tmp := path + ".tmp"
	f, err := s.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeAll(f, payload); err != nil {
		_ = f.Close()
		_ = s.fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = s.fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = s.fs.Remove(tmp)
		return err
	}
	if err := s.fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(s.fs, s.workdir)
}

func fileSize(f vfs.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
