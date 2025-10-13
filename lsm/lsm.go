package lsm

import (
	"sync"

	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
)

// LSM _
type LSM struct {
	lock       sync.RWMutex
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	wal        *wal.Manager
	maxMemFID  uint32
}

// Options _
type Options struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int

	DiscardStatsCh *chan map[uint32]int64
}

// Close  _
func (lsm *LSM) Close() error {
	// wait for all api calls to finish
	lsm.closer.Close()
	// TODO need to lock to ensure concurrency safety
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// SetDiscardStatsCh updates the discard stats channel used during compaction.
func (lsm *LSM) SetDiscardStatsCh(ch *chan map[uint32]int64) {
	lsm.option.DiscardStatsCh = ch
	if lsm.levels != nil {
		lsm.levels.opt.DiscardStatsCh = ch
	}
}

// NewLSM _
func NewLSM(opt *Options, walMgr *wal.Manager) *LSM {
	lsm := &LSM{option: opt, wal: walMgr}
	// initialize levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// start the db recovery process to load the wal, if there is no recovery content, create a new memtable
	lsm.memTable, lsm.immutables = lsm.recovery()
	// initialize closer for resource recycling signal control
	lsm.closer = utils.NewCloser()
	return lsm
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levels.runCompacter(i)
	}
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// graceful shutdown
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	// check if the current memtable is full, if so, create a new memtable, and write the current memtable to immutables
	// otherwise, write to the current memtable
	if lsm.memTable.walSize+
		int64(utils.EstimateWalCodecSize(entry)) > lsm.option.MemTableSize {
		lsm.Rotate()
	}

	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	// check if there are immutables that need to be flushed, if so, flush them
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		// TODO there is a big problem here, should use reference counting to recycle
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		// TODO optimize this to save memory space, and limit the size of immut table to a fixed value
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	var (
		entry *utils.Entry
		err   error
	)
	// query from the memtable, first query the active table, then query the immutable table
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}

	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// query from the level manager
	return lsm.levels.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.Skiplist {
	return lsm.memTable.sl
}

func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemtable()
}

func (lsm *LSM) GetMemTables() ([]*memTable, func()) {
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()

	var tables []*memTable

	tables = append(tables, lsm.memTable)
	lsm.memTable.IncrRef()

	last := len(lsm.immutables) - 1
	for i := range lsm.immutables {
		tables = append(tables, lsm.immutables[last-i])
		lsm.immutables[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}

}
