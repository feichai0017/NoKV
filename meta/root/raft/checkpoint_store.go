package rootraft

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

const checkpointFileName = "root-raft-checkpoint.pb"

// FileCheckpointStore persists compact metadata-root checkpoints in one local
// protobuf file.
type FileCheckpointStore struct {
	fs      vfs.FS
	workdir string
}

var _ CheckpointStore = (*FileCheckpointStore)(nil)

func OpenFileCheckpointStore(workdir string, fs vfs.FS) (*FileCheckpointStore, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("meta/root/raft: checkpoint workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	return &FileCheckpointStore{fs: fs, workdir: workdir}, nil
}

func (s *FileCheckpointStore) Load() (Checkpoint, error) {
	if s == nil {
		return Checkpoint{}, nil
	}
	data, err := s.fs.ReadFile(filepath.Join(s.workdir, checkpointFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, nil
		}
		return Checkpoint{}, err
	}
	if len(data) == 0 {
		return Checkpoint{}, nil
	}
	return decodeCheckpoint(data)
}

func (s *FileCheckpointStore) Save(cp Checkpoint) error {
	if s == nil {
		return nil
	}
	data, err := encodeCheckpoint(cp)
	if err != nil {
		return err
	}
	path := filepath.Join(s.workdir, checkpointFileName)
	tmp := path + ".tmp"
	f, err := s.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
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

func encodeCheckpoint(cp Checkpoint) ([]byte, error) {
	pb := &metapb.RootCheckpoint{State: stateToRootPB(cp.State)}
	if len(cp.Descriptors) > 0 {
		pb.Descriptors = make([]*metapb.RegionDescriptor, 0, len(cp.Descriptors))
		for _, desc := range cp.Descriptors {
			pb.Descriptors = append(pb.Descriptors, metacodec.DescriptorToProto(desc))
		}
	}
	return proto.Marshal(pb)
}

func decodeCheckpoint(data []byte) (Checkpoint, error) {
	var pb metapb.RootCheckpoint
	if err := proto.Unmarshal(data, &pb); err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{}
	if pb.State != nil {
		cp.State = stateFromRootPB(pb.State)
	}
	if len(pb.Descriptors) > 0 {
		cp.Descriptors = make(map[uint64]descriptor.Descriptor, len(pb.Descriptors))
		for _, desc := range pb.Descriptors {
			if desc == nil {
				continue
			}
			runtime := metacodec.DescriptorFromProto(desc)
			cp.Descriptors[runtime.RegionID] = runtime
		}
	}
	return cp, nil
}

func stateToRootPB(state rootpkg.State) *metapb.RootState {
	return &metapb.RootState{
		ClusterEpoch:    state.ClusterEpoch,
		MembershipEpoch: state.MembershipEpoch,
		PolicyVersion:   state.PolicyVersion,
		LastCommitted:   &metapb.RootCursor{Term: state.LastCommitted.Term, Index: state.LastCommitted.Index},
		IdFence:         state.IDFence,
		TsoFence:        state.TSOFence,
	}
}

func stateFromRootPB(pbState *metapb.RootState) rootpkg.State {
	if pbState == nil {
		return rootpkg.State{}
	}
	var cursor rootpkg.Cursor
	if pbState.LastCommitted != nil {
		cursor = rootpkg.Cursor{Term: pbState.LastCommitted.Term, Index: pbState.LastCommitted.Index}
	}
	return rootpkg.State{
		ClusterEpoch:    pbState.ClusterEpoch,
		MembershipEpoch: pbState.MembershipEpoch,
		PolicyVersion:   pbState.PolicyVersion,
		LastCommitted:   cursor,
		IDFence:         pbState.IdFence,
		TSOFence:        pbState.TsoFence,
	}
}
