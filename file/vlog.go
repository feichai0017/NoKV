package file

import (
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

type LogFile struct {
	Lock sync.RWMutex
	FID  uint32
	size uint32
	f    *MmapFile
	ro   bool
}

func (lf *LogFile) Open(opt *Options) error {
	var err error
	lf.FID = uint32(opt.FID)
	lf.Lock = sync.RWMutex{}
	flag := opt.Flag
	if flag == 0 {
		flag = os.O_CREATE | os.O_RDWR
	}
	lf.f, err = OpenMmapFile(opt.FileName, flag, opt.MaxSz)
	utils.Panic2(nil, err)
	fi, err := lf.f.Fd.Stat()
	if err != nil {
		return utils.WarpErr("Unable to run file.Stat", err)
	}
	// Load the current file size.
	sz := fi.Size()
	utils.CondPanic(sz > math.MaxUint32, fmt.Errorf("file size: %d greater than %d",
		uint32(sz), uint32(math.MaxUint32)))
	lf.size = uint32(sz)
	lf.ro = flag == os.O_RDONLY
	// TODO: consider reserving a header region for metadata.
	return nil
}

// Acquire lock on mmap/file if you are calling this
func (lf *LogFile) Read(p *kv.ValuePtr) (buf []byte, err error) {
	offset := p.Offset
	// Do not convert size to uint32, because the lf.fmap can be of size
	// 4GB, which overflows the uint32 during conversion to make the size 0,
	// causing the read to fail with ErrEOF. See issue #585.
	size := int64(len(lf.f.Data))
	valsz := p.Len
	lfsz := atomic.LoadUint32(&lf.size)
	if int64(offset) >= size || int64(offset+valsz) > size ||
		// Ensure that the read is within the file's actual size. It might be possible that
		// the offset+valsz length is beyond the file's actual size. This could happen when
		// dropAll and iterations are running simultaneously.
		int64(offset+valsz) > int64(lfsz) {
		err = io.EOF
	} else {
		buf, err = lf.f.Bytes(int(offset), int(valsz))
	}
	return buf, err
}

func (lf *LogFile) DoneWriting(offset uint32) error {
	// Sync before acquiring lock. (We call this from write() and thus know we have shared access
	// to the fd.)
	// Prefer async flush to reduce stall; follow up with sync after truncation.
	_ = lf.f.SyncAsyncRange(0, int64(offset))
	if err := lf.f.Sync(); err != nil {
		return errors.Wrapf(err, "Unable to sync value log: %q", lf.FileName())
	}

	// Writes must hold the lock for consistency.
	lf.Lock.Lock()
	defer lf.Lock.Unlock()

	// TODO: Confirm if we need to run a file sync after truncation.
	// Truncation must run after unmapping, otherwise Windows would crap itself.
	if err := lf.f.Truncature(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.FileName())
	}

	// Reinitialize the log file. This will mmap the entire file.
	if err := lf.Init(); err != nil {
		return errors.Wrapf(err, "failed to initialize file %s", lf.FileName())
	}

	// Drop freshly written pages from page cache; cold segments rely on OS
	// cache rather than user cache.
	_ = lf.f.Advise(utils.AccessPatternDontNeed)

	// Previously we used to close the file after it was written and reopen it in read-only mode.
	// We no longer open files in read-only mode. We keep all vlog files open in read-write mode.
	return nil
}
func (lf *LogFile) Write(offset uint32, buf []byte) (err error) {
	if lf.ro {
		return fmt.Errorf("logfile %s is read-only", lf.FileName())
	}
	err = lf.f.AppendBuffer(offset, buf)
	if err == nil {
		atomic.StoreUint32(&lf.size, offset+uint32(len(buf)))
	}
	return err
}
func (lf *LogFile) Truncate(offset int64) error {
	if err := lf.f.Truncature(offset); err != nil {
		return err
	}
	if offset < 0 {
		offset = 0
	}
	if offset > math.MaxUint32 {
		offset = math.MaxUint32
	}
	atomic.StoreUint32(&lf.size, uint32(offset))
	return nil
}
func (lf *LogFile) Close() error {
	return lf.f.Close()
}

func (lf *LogFile) Size() int64 {
	return int64(atomic.LoadUint32(&lf.size))
}

// Bootstrap initializes a new log file.
func (lf *LogFile) Bootstrap() error {
	if lf == nil {
		return fmt.Errorf("logfile bootstrap: nil receiver")
	}
	if lf.ro {
		return fmt.Errorf("logfile bootstrap: read-only file %s", lf.FileName())
	}
	// Reserve header region and ensure it is zeroed even when the file is preallocated.
	if kv.ValueLogHeaderSize > 0 {
		header := make([]byte, kv.ValueLogHeaderSize)
		if err := lf.Write(0, header); err != nil {
			return err
		}
	}
	return nil
}

func (lf *LogFile) Init() error {
	fstat, err := lf.f.Fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.FileName())
	}
	sz := fstat.Size()
	if sz == 0 {
		// File is empty. We don't need to mmap it. Return.
		return nil
	}
	utils.CondPanic(sz > math.MaxUint32, fmt.Errorf("[LogFile.Init] sz > math.MaxUint32"))
	lf.size = uint32(sz)
	return nil
}
func (lf *LogFile) FileName() string {
	return lf.f.Fd.Name()
}

func (lf *LogFile) Seek(offset int64, whence int) (ret int64, err error) {
	return lf.f.Fd.Seek(offset, whence)
}

func (lf *LogFile) FD() *os.File {
	return lf.f.Fd
}

// You must hold lf.lock to sync()
func (lf *LogFile) Sync() error {
	return lf.f.Sync()
}

// SetReadOnly remaps the file as read-only and advises the OS to drop pages.
func (lf *LogFile) SetReadOnly() error {
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	if lf.ro {
		return nil
	}
	if err := lf.f.Remap(false); err != nil {
		return err
	}
	_ = lf.f.Advise(utils.AccessPatternDontNeed)
	lf.ro = true
	return nil
}

// SetWritable remaps the file back to writable mode.
func (lf *LogFile) SetWritable() error {
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	if !lf.ro {
		return nil
	}
	if err := lf.f.Remap(true); err != nil {
		return err
	}
	lf.ro = false
	return nil
}
