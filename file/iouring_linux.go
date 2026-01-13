//go:build linux
// +build linux

package file

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ErrIOUringUnavailable is returned when io_uring is not supported or enabled.
var ErrIOUringUnavailable = errors.New("io_uring unavailable")

// IOUringOp describes the operation submitted to the ring.
type IOUringOp uint8

const (
	IOUringOpWrite IOUringOp = iota
	IOUringOpFsync
)

// IORequest describes a single async I/O request.
type IORequest struct {
	Op       IOUringOp
	FD       int
	Offset   int64
	Buffer   []byte
	UserData uint64
	Flags    uint32
}

// IOCompletion describes a completed async I/O request.
type IOCompletion struct {
	UserData uint64
	Result   int32
	Flags    uint32
}

// IOUring defines the async I/O interface used by WAL/Vlog writers.
type IOUring interface {
	Submit(req IORequest) error
	SubmitBatch(reqs []IORequest) error
	Wait(minComplete int) ([]IOCompletion, error)
	Close() error
}

// NewIOUring creates a new io_uring backend when supported.
func NewIOUring(entries int, flags uint32) (IOUring, error) {
	return newIOUring(entries, flags)
}

const (
	ioringOffSqRing = 0
	ioringOffCqRing = 0x8000000
	ioringOffSqes   = 0x10000000

	ioringEnterGetEvents = 1

	ioringFeatSingleMmap = 1 << 0

	ioringOpWritev = 2
	ioringOpFsync  = 3
)

type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        ioSqringOffsets
	cqOff        ioCqringOffsets
}

type ioSqringOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

type ioCqringOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	resv        [2]uint64
}

type ioUringSqe struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	opFlags     uint32
	userData    uint64
	bufIndex    uint16
	personality uint16
	spliceFDIn  int32
	addr2       uint64
	resv        [2]uint64
}

type ioUringCqe struct {
	userData uint64
	res      int32
	flags    uint32
}

type pendingReq struct {
	buf []byte
	iov *unix.Iovec
}

type ioUring struct {
	fd     int
	params ioUringParams

	sqRing []byte
	cqRing []byte
	sqes   []ioUringSqe

	sqHead        *uint32
	sqTail        *uint32
	sqRingMask    uint32
	sqRingEntries uint32
	sqFlags       *uint32
	sqDropped     *uint32
	sqArray       []uint32

	cqHead        *uint32
	cqTail        *uint32
	cqRingMask    uint32
	cqRingEntries uint32
	cqOverflow    *uint32
	cqes          []ioUringCqe

	sqesBacking []byte
	cqShared    bool

	mu      sync.Mutex
	pending map[uint64]pendingReq
}

// newIOUring creates a minimal io_uring backend for write/fsync.
func newIOUring(entries int, flags uint32) (IOUring, error) {
	if entries <= 0 {
		return nil, fmt.Errorf("io_uring: invalid entries %d", entries)
	}
	var params ioUringParams
	params.flags = flags
	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, errno
	}
	ringFD := int(fd)

	sqRingSize := params.sqOff.array + params.sqEntries*4
	cqRingSize := params.cqOff.cqes + params.cqEntries*uint32(unsafe.Sizeof(ioUringCqe{}))
	ringSize := sqRingSize
	sqRingOffset := int64(ioringOffSqRing)
	cqRingOffset := int64(ioringOffCqRing)
	if params.features&ioringFeatSingleMmap != 0 {
		if cqRingSize > ringSize {
			ringSize = cqRingSize
		}
		cqRingOffset = sqRingOffset
	}

	sqRing, err := unix.Mmap(ringFD, sqRingOffset, int(ringSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Close(ringFD)
		return nil, err
	}
	var cqRing []byte
	cqShared := false
	if cqRingOffset == sqRingOffset {
		cqRing = sqRing
		cqShared = true
	} else {
		cqRing, err = unix.Mmap(ringFD, cqRingOffset, int(cqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			_ = unix.Munmap(sqRing)
			_ = unix.Close(ringFD)
			return nil, err
		}
	}

	sqesSize := int(params.sqEntries) * int(unsafe.Sizeof(ioUringSqe{}))
	sqesMmap, err := unix.Mmap(ringFD, ioringOffSqes, sqesSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		if cqRingOffset != sqRingOffset {
			_ = unix.Munmap(cqRing)
		}
		_ = unix.Munmap(sqRing)
		_ = unix.Close(ringFD)
		return nil, err
	}

	sqRingPtr := unsafe.Pointer(&sqRing[0])
	cqRingPtr := unsafe.Pointer(&cqRing[0])
	sqesPtr := unsafe.Pointer(&sqesMmap[0])

	r := &ioUring{
		fd:          ringFD,
		params:      params,
		sqRing:      sqRing,
		cqRing:      cqRing,
		sqes:        unsafe.Slice((*ioUringSqe)(sqesPtr), int(params.sqEntries)),
		sqHead:      (*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.head))),
		sqTail:      (*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.tail))),
		sqFlags:     (*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.flags))),
		sqDropped:   (*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.dropped))),
		cqHead:      (*uint32)(unsafe.Add(cqRingPtr, uintptr(params.cqOff.head))),
		cqTail:      (*uint32)(unsafe.Add(cqRingPtr, uintptr(params.cqOff.tail))),
		cqOverflow:  (*uint32)(unsafe.Add(cqRingPtr, uintptr(params.cqOff.overflow))),
		pending:     make(map[uint64]pendingReq),
		sqesBacking: sqesMmap,
		cqShared:    cqShared,
	}
	r.sqRingMask = *(*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.ringMask)))
	r.sqRingEntries = *(*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.ringEntries)))
	r.cqRingMask = *(*uint32)(unsafe.Add(cqRingPtr, uintptr(params.cqOff.ringMask)))
	r.cqRingEntries = *(*uint32)(unsafe.Add(cqRingPtr, uintptr(params.cqOff.ringEntries)))
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Add(sqRingPtr, uintptr(params.sqOff.array))), int(params.sqEntries))
	r.cqes = unsafe.Slice((*ioUringCqe)(unsafe.Add(cqRingPtr, uintptr(params.cqOff.cqes))), int(params.cqEntries))
	return r, nil
}

func (r *ioUring) Submit(req IORequest) error {
	return r.SubmitBatch([]IORequest{req})
}

func (r *ioUring) SubmitBatch(reqs []IORequest) error {
	if len(reqs) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	queued := 0
	for _, req := range reqs {
		if err := r.queue(req); err != nil {
			if errors.Is(err, ErrIOUringFull) && queued > 0 {
				if err := r.enter(queued, 0); err != nil {
					return err
				}
				queued = 0
				if err := r.queue(req); err != nil {
					return err
				}
			} else {
				return err
			}
		}
		queued++
	}
	if queued > 0 {
		return r.enter(queued, 0)
	}
	return nil
}

func (r *ioUring) Wait(minComplete int) ([]IOCompletion, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	completions := r.drain()
	if minComplete <= 0 || len(completions) >= minComplete {
		return completions, nil
	}
	if err := r.enter(0, minComplete-len(completions)); err != nil {
		return completions, err
	}
	more := r.drain()
	completions = append(completions, more...)
	return completions, nil
}

func (r *ioUring) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	if r.sqesBacking != nil {
		if e := unix.Munmap(r.sqesBacking); e != nil {
			err = e
		}
		r.sqesBacking = nil
	}
	if r.cqRing != nil && !r.cqShared {
		if e := unix.Munmap(r.cqRing); e != nil && err == nil {
			err = e
		}
		r.cqRing = nil
	}
	if r.sqRing != nil {
		if e := unix.Munmap(r.sqRing); e != nil && err == nil {
			err = e
		}
		r.sqRing = nil
	}
	if r.fd != 0 {
		if e := unix.Close(r.fd); e != nil && err == nil {
			err = e
		}
		r.fd = 0
	}
	return err
}

// ErrIOUringFull indicates the submission queue is full.
var ErrIOUringFull = errors.New("io_uring submission queue full")

func (r *ioUring) queue(req IORequest) error {
	if r == nil {
		return ErrIOUringUnavailable
	}
	head := atomic.LoadUint32(r.sqHead)
	tail := atomic.LoadUint32(r.sqTail)
	if tail-head >= r.sqRingEntries {
		return ErrIOUringFull
	}
	idx := tail & r.sqRingMask
	sqe := &r.sqes[idx]
	*sqe = ioUringSqe{}

	switch req.Op {
	case IOUringOpWrite:
		if len(req.Buffer) == 0 {
			return fmt.Errorf("io_uring write: empty buffer")
		}
		iov := &unix.Iovec{Base: &req.Buffer[0], Len: uint64(len(req.Buffer))}
		r.pending[req.UserData] = pendingReq{buf: req.Buffer, iov: iov}
		sqe.opcode = ioringOpWritev
		sqe.fd = int32(req.FD)
		sqe.off = uint64(req.Offset)
		sqe.addr = uint64(uintptr(unsafe.Pointer(iov)))
		sqe.len = 1
	case IOUringOpFsync:
		sqe.opcode = ioringOpFsync
		sqe.fd = int32(req.FD)
	default:
		return fmt.Errorf("io_uring: unsupported op %d", req.Op)
	}
	sqe.flags = uint8(req.Flags)
	sqe.userData = req.UserData
	r.sqArray[idx] = idx
	atomic.StoreUint32(r.sqTail, tail+1)
	return nil
}

func (r *ioUring) enter(toSubmit int, minComplete int) error {
	flags := uint32(0)
	if minComplete > 0 {
		flags = ioringEnterGetEvents
	}
	_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (r *ioUring) drain() []IOCompletion {
	head := atomic.LoadUint32(r.cqHead)
	tail := atomic.LoadUint32(r.cqTail)
	if head == tail {
		return nil
	}
	completions := make([]IOCompletion, 0, tail-head)
	for head != tail {
		idx := head & r.cqRingMask
		cqe := r.cqes[idx]
		completions = append(completions, IOCompletion{
			UserData: cqe.userData,
			Result:   cqe.res,
			Flags:    cqe.flags,
		})
		delete(r.pending, cqe.userData)
		head++
	}
	atomic.StoreUint32(r.cqHead, head)
	return completions
}
