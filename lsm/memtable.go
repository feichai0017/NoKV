package lsm

import (
	"bytes"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/pkg/errors"
)

type MemTable = memTable

// memTable holds the active Skiplist and its WAL segment id.
type memTable struct {
	lsm        *LSM
	segmentID  uint32
	sl         *utils.Skiplist
	buf        *bytes.Buffer
	maxVersion uint64
	walSize    int64
}

// NewMemtable creates the active MemTable and switches WAL to the new segment.
func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	utils.Panic(lsm.wal.SwitchSegment(uint32(newFid), true))
	return &memTable{
		lsm:       lsm,
		segmentID: uint32(newFid),
		sl:        utils.NewSkiplist(int64(64 << 20)),
		buf:       &bytes.Buffer{},
		walSize:   0,
	}
}

func (m *memTable) close() error { return nil }

func (m *memTable) set(entry *utils.Entry) error {
	payload := wal.EncodeEntry(m.buf, entry)
	infos, err := m.lsm.wal.Append(payload)
	if err != nil {
		return err
	}
	if len(infos) > 0 {
		m.walSize += int64(infos[0].Length) + 8
	}
	m.sl.Add(entry)
	return nil
}

func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	vs := m.sl.Search(key)
	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}
	return e, nil
}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}

// recovery rebuilds memtables from existing WAL segments.
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	files, err := lsm.wal.ListSegments()
	if err != nil {
		utils.Panic(err)
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	for _, path := range files {
		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".wal") {
			continue
		}
		fid, err := strconv.ParseUint(strings.TrimSuffix(base, ".wal"), 10, 64)
		if err != nil {
			utils.Panic(err)
		}
		if fid > maxFid {
			maxFid = fid
		}
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })

	if len(fids) == 0 {
		lsm.levels.maxFID = maxFid
		return lsm.NewMemtable(), nil
	}

	tables := make([]*memTable, 0, len(fids))
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			continue
		}
		tables = append(tables, mt)
	}
	if len(tables) == 0 {
		lsm.levels.maxFID = maxFid
		return lsm.NewMemtable(), nil
	}

	lsm.levels.maxFID = maxFid
	active := tables[len(tables)-1]
	tables = tables[:len(tables)-1]
	utils.Panic(lsm.wal.SwitchSegment(active.segmentID, false))
	return active, tables
}

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	mt := &memTable{
		lsm:       lsm,
		segmentID: uint32(fid),
		sl:        utils.NewSkiplist(int64(1 << 20)),
		buf:       &bytes.Buffer{},
	}
	err := lsm.wal.ReplaySegment(uint32(fid), func(_ wal.EntryInfo, payload []byte) error {
		entry, err := wal.DecodeEntry(payload)
		if err != nil {
			return err
		}
		if ts := utils.ParseTs(entry.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		mt.sl.Add(entry)
		mt.walSize += int64(len(payload)) + 8
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "while updating skiplist")
	}
	return mt, nil
}

// reference counting helpers, delegate to skiplist.
func (mt *memTable) IncrRef() {
	mt.sl.IncrRef()
}

func (mt *memTable) DecrRef() {
	mt.sl.DecrRef()
}
