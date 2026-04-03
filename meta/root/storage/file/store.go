package file

import (
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/vfs"
)

type store struct {
	checkpt fileCheckpointStore
	log     fileEventLog
}

func NewStore(fs vfs.FS, workdir string) rootstorage.Substrate {
	return store{
		checkpt: fileCheckpointStore{fs: fs, workdir: workdir},
		log:     fileEventLog{fs: fs, workdir: workdir},
	}
}

func (s store) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	return s.checkpt.LoadCheckpoint()
}

func (s store) SaveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	return s.checkpt.SaveCheckpoint(checkpoint)
}

func (s store) ReadCommitted(offset int64) (rootstorage.CommittedTail, error) {
	return s.log.ReadCommitted(offset)
}

func (s store) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	return s.log.AppendCommitted(records...)
}

func (s store) CompactCommitted(stream rootstorage.CommittedTail) error {
	return s.log.CompactCommitted(stream)
}

func (s store) InstallBootstrap(checkpoint rootstorage.Checkpoint, stream rootstorage.CommittedTail) error {
	if err := s.log.CompactCommitted(stream); err != nil {
		return err
	}
	return s.checkpt.SaveCheckpoint(checkpoint)
}

func (s store) Size() (int64, error) {
	return s.log.Size()
}
