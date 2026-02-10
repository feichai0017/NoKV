//go:build linux
// +build linux

package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/feichai0017/NoKV/utils/mmap"
	"github.com/pkg/errors"
)

// MmapFile represents an mmapd file and includes both the buffer to the data and the file descriptor.
type MmapFile struct {
	Data []byte
	Fd   *os.File
}

// OpenMmapFileUsing os
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var rerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// If file is empty, truncate it to sz.
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}

	// fmt.Printf("Mmaping file: %s with writable: %v filesize: %d\n", fd.Name(), writable, fileSize)
	buf, err := mmap.Mmap(fd, writable, fileSize) // Mmap up to file size.
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}

	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go func() {
			_ = SyncDir(dir)
		}()
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, rerr
}

// OpenMmapFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In both cases, it would mmap
// the file to maxSz and returned it. In case the file is created, z.NewFile is
// returned.
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	// fmt.Printf("opening file %s with flag: %v\n", filename, flag)
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	// if the sst file layer has been opened, use its original size
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	return OpenMmapFileUsing(fd, maxSz, writable)
}

type mmapReader struct {
	Data   []byte
	offset int
}

// Read is part of the exported receiver API.
func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

// NewReader creates a new value for the API.
func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if m == nil || m.Data == nil || sz < 0 || off < 0 {
		return nil, io.EOF
	}
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// View returns a direct slice over the mmap'd region without copying.
// Use this only when the caller owns the file lifetime (e.g., building SSTables).
func (m *MmapFile) View(off, sz int) ([]byte, error) {
	if m == nil || m.Data == nil || sz < 0 || off < 0 {
		return nil, io.EOF
	}
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// Slice returns the slice at the given offset.
func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

// AllocateSlice allocates a slice of the given size at the given offset.
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	// If the file is too small, double its size or increase it by 1GB, whichever is smaller.
	if start+sz > len(m.Data) {
		const oneGB = 1 << 30
		growBy := len(m.Data)
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}

	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

const oneGB = 1 << 30

// AppendBuffer appends data into the mmap region, growing the mapping if needed.
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		newSize := size + growBy
		if newSize < end {
			newSize = end
		}
		if err := m.Truncature(int64(newSize)); err != nil {
			return err
		}
	}
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

// Sync is part of the exported receiver API.
func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

// SyncAsync flushes dirty pages asynchronously.
func (m *MmapFile) SyncAsync() error {
	if m == nil {
		return nil
	}
	return mmap.MsyncAsync(m.Data)
}

// SyncAsyncRange flushes a range asynchronously.
func (m *MmapFile) SyncAsyncRange(off, n int64) error {
	if m == nil {
		return nil
	}
	return mmap.MsyncAsyncRange(m.Data, off, n)
}

// Remap remaps the file with the requested writability.
func (m *MmapFile) Remap(writable bool) error {
	if m == nil || m.Fd == nil {
		return fmt.Errorf("mmap file remap: nil receiver")
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return err
	}
	fi, err := m.Fd.Stat()
	if err != nil {
		return err
	}
	buf, err := mmap.Mmap(m.Fd, writable, fi.Size())
	if err != nil {
		return err
	}
	m.Data = buf
	return nil
}

// Delete is part of the exported receiver API.
func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

// Close would close the file. It would also truncate the file if maxSz >= 0.
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

// SyncDir is part of the exported package API.
func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

// Truncature compatible interface
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.Fd.Name(), err)
	}

	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}

// ReName compatible interface
func (m *MmapFile) ReName(name string) error {
	return nil
}
