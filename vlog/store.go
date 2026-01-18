package vlog

import (
	"os"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
)

// SegmentStore defines the IO surface for a value-log segment.
// Locking methods protect concurrent access to active segments.
type SegmentStore interface {
	Read(ptr *kv.ValuePtr) ([]byte, error)
	Write(offset uint32, buf []byte) error
	DoneWriting(offset uint32) error
	SetReadOnly() error
	SetWritable() error
	Truncate(offset int64) error
	Init() error
	Bootstrap() error
	Sync() error
	Size() int64
	Seek(offset int64, whence int) (int64, error)
	FileName() string
	FD() *os.File
	Close() error

	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// SegmentStoreFactory constructs SegmentStore instances for the manager.
type SegmentStoreFactory interface {
	Open(fid uint32, path string, dir string, maxSize int64, readOnly bool) (SegmentStore, error)
	Create(fid uint32, path string, dir string, maxSize int64) (SegmentStore, error)
}

type logFileStore struct {
	lf *file.LogFile
}

type logFileFactory struct{}

// defaultSegmentStoreFactory returns the built-in log file store factory.
func defaultSegmentStoreFactory() SegmentStoreFactory {
	return logFileFactory{}
}

func (logFileFactory) Open(fid uint32, path string, dir string, maxSize int64, readOnly bool) (SegmentStore, error) {
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	lf := &file.LogFile{}
	if err := lf.Open(&file.Options{
		FID:      uint64(fid),
		FileName: path,
		Dir:      dir,
		Flag:     flag,
		MaxSz:    int(maxSize),
	}); err != nil {
		return nil, err
	}
	return newLogFileStore(lf), nil
}

func (f logFileFactory) Create(fid uint32, path string, dir string, maxSize int64) (SegmentStore, error) {
	store, err := f.Open(fid, path, dir, maxSize, false)
	if err != nil {
		return nil, err
	}
	if err := store.Bootstrap(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func newLogFileStore(lf *file.LogFile) *logFileStore {
	return &logFileStore{lf: lf}
}

func (s *logFileStore) Read(ptr *kv.ValuePtr) ([]byte, error) {
	return s.lf.Read(ptr)
}

func (s *logFileStore) Write(offset uint32, buf []byte) error {
	return s.lf.Write(offset, buf)
}

func (s *logFileStore) DoneWriting(offset uint32) error {
	return s.lf.DoneWriting(offset)
}

func (s *logFileStore) SetReadOnly() error {
	return s.lf.SetReadOnly()
}

func (s *logFileStore) SetWritable() error {
	return s.lf.SetWritable()
}

func (s *logFileStore) Truncate(offset int64) error {
	return s.lf.Truncate(offset)
}

func (s *logFileStore) Init() error {
	return s.lf.Init()
}

func (s *logFileStore) Bootstrap() error {
	return s.lf.Bootstrap()
}

func (s *logFileStore) Sync() error {
	return s.lf.Sync()
}

func (s *logFileStore) Size() int64 {
	return s.lf.Size()
}

func (s *logFileStore) Seek(offset int64, whence int) (int64, error) {
	return s.lf.Seek(offset, whence)
}

func (s *logFileStore) FileName() string {
	return s.lf.FileName()
}

func (s *logFileStore) FD() *os.File {
	return s.lf.FD()
}

func (s *logFileStore) Close() error {
	return s.lf.Close()
}

func (s *logFileStore) Lock() {
	s.lf.Lock.Lock()
}

func (s *logFileStore) Unlock() {
	s.lf.Lock.Unlock()
}

func (s *logFileStore) RLock() {
	s.lf.Lock.RLock()
}

func (s *logFileStore) RUnlock() {
	s.lf.Lock.RUnlock()
}
