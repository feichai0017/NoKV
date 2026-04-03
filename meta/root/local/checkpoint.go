package local

import (
	"errors"
	"os"
	"path/filepath"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

func loadCheckpoint(fs vfs.FS, workdir string) (rootpkg.Snapshot, int64, error) {
	path := filepath.Join(workdir, CheckpointFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootpkg.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0, nil
		}
		return rootpkg.Snapshot{}, 0, err
	}
	if len(data) == 0 {
		return rootpkg.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0, nil
	}
	var pbCheckpoint metapb.RootCheckpoint
	if err := proto.Unmarshal(data, &pbCheckpoint); err != nil {
		return rootpkg.Snapshot{}, 0, err
	}
	if pbCheckpoint.State == nil && len(pbCheckpoint.Descriptors) == 0 {
		var pbState metapb.RootState
		if err := proto.Unmarshal(data, &pbState); err == nil {
			return rootpkg.Snapshot{
				State:       metacodec.RootStateFromProto(&pbState),
				Descriptors: make(map[uint64]descriptor.Descriptor),
			}, 0, nil
		}
	}
	snapshot, logOffset := metacodec.RootSnapshotFromProto(&pbCheckpoint)
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	return snapshot, int64(logOffset), nil
}

func persistCheckpoint(fs vfs.FS, workdir string, snapshot rootpkg.Snapshot, logOffset uint64) error {
	payload, err := proto.Marshal(metacodec.RootSnapshotToProto(snapshot, logOffset))
	if err != nil {
		return err
	}
	path := filepath.Join(workdir, CheckpointFileName)
	tmp := path + ".tmp"
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeAll(f, payload); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(fs, workdir)
}

func currentLogSize(fs vfs.FS, workdir string) (int64, error) {
	info, err := fs.Stat(filepath.Join(workdir, LogFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func fileSize(f vfs.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
