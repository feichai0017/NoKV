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

func (s store) ReadCommitted(requestedOffset int64) (rootstorage.CommittedTail, error) {
	checkpoint, err := s.checkpt.LoadCheckpoint()
	if err != nil {
		return rootstorage.CommittedTail{}, err
	}
	startOffset := requestedOffset
	if startOffset < checkpoint.TailOffset {
		startOffset = checkpoint.TailOffset
	}
	tail, err := s.log.ReadCommitted(startOffset)
	if err != nil {
		return rootstorage.CommittedTail{}, err
	}
	tail.RequestedOffset = requestedOffset
	tail.StartOffset = startOffset
	return tail, nil
}

func (s store) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	return s.log.AppendCommitted(records...)
}

func (s store) CompactCommitted(stream rootstorage.CommittedTail) error {
	return s.log.CompactCommitted(stream)
}

func (s store) InstallBootstrap(observed rootstorage.ObservedCommitted) error {
	if err := s.log.CompactCommitted(observed.Tail); err != nil {
		return err
	}
	return s.checkpt.SaveCheckpoint(observed.Checkpoint)
}

func (s store) Size() (int64, error) {
	return s.log.Size()
}
