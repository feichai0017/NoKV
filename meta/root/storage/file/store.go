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

func (s store) LoadCommitted(offset int64) ([]rootstorage.CommittedEvent, error) {
	return s.log.LoadCommitted(offset)
}

func (s store) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	return s.log.AppendCommitted(records...)
}

func (s store) CompactCommitted(records []rootstorage.CommittedEvent) error {
	return s.log.CompactCommitted(records)
}

func (s store) InstallBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	if err := s.log.CompactCommitted(records); err != nil {
		return err
	}
	return s.checkpt.SaveCheckpoint(checkpoint)
}

func (s store) Size() (int64, error) {
	return s.log.Size()
}
