package lsm

import (
	"errors"
	"sync"

	"github.com/feichai0017/NoKV/lsm/flush"
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
	flushMgr   *flush.Manager
	flushWG    sync.WaitGroup
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
	lsm.flushMgr.Close()
	lsm.flushWG.Wait()
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
	lsm.flushMgr = flush.NewManager()
	// initialize levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// start the db recovery process to load the wal, if there is no recovery content, create a new memtable
	lsm.memTable, lsm.immutables = lsm.recovery()
	lsm.startFlushWorkers(1)
	for _, mt := range lsm.immutables {
		lsm.submitFlush(mt)
	}
	// initialize closer for resource recycling signal control
	lsm.closer = utils.NewCloser()
	return lsm
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := range n {
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
	lsm.lock.RLock()
	active := lsm.memTable
	immutables := append([]*memTable(nil), lsm.immutables...)
	lsm.lock.RUnlock()

	if active != nil {
		if entry, err = active.Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}

	for i := len(immutables) - 1; i >= 0; i-- {
		if entry, err = immutables[i].Get(key); entry != nil && entry.Value != nil {
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
	lsm.lock.Lock()
	old := lsm.memTable
	lsm.immutables = append(lsm.immutables, old)
	lsm.memTable = lsm.NewMemtable()
	lsm.lock.Unlock()
	lsm.submitFlush(old)
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

func (lsm *LSM) submitFlush(mt *memTable) {
	if mt == nil {
		return
	}
	_, err := lsm.flushMgr.Submit(&flush.Task{SegmentID: mt.segmentID, Data: mt})
	utils.Panic(err)
}

func (lsm *LSM) startFlushWorkers(n int) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		lsm.flushWG.Add(1)
		go func() {
			defer lsm.flushWG.Done()
			for {
				task, ok := lsm.flushMgr.Next()
				if !ok {
					return
				}
				mt, _ := task.Data.(*memTable)
				if mt == nil {
					lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, errors.New("nil memtable"))
					continue
				}
				err := lsm.levels.flush(mt)
				if err != nil {
					lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, err)
					continue
				}
				lsm.flushMgr.Update(task.ID, flush.StageInstall, nil, nil)
				lsm.lock.Lock()
				for idx, imm := range lsm.immutables {
					if imm == mt {
						lsm.immutables = append(lsm.immutables[:idx], lsm.immutables[idx+1:]...)
						break
					}
				}
				lsm.lock.Unlock()
				_ = mt.close()
				lsm.flushMgr.Update(task.ID, flush.StageRelease, nil, nil)
			}
		}()
	}
}
