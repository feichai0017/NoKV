package lsm

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/utils"
)

// initLevelManager initialize the levelManager
func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm} // dereference lsm
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	// read the manifest file to build the manager
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

type levelManager struct {
	maxFID       uint64 // the maximum fid that has been allocated, as long as a memtable is created, it is considered allocated
	opt          *Options
	cache        *cache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
	lsm          *LSM
	compactState *compactStatus
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) iterators() []utils.Iterator {

	itrs := make([]utils.Iterator, 0, len(lm.levels))
	for _, level := range lm.levels {
		itrs = append(itrs, level.iterators()...)
	}
	return itrs
}

func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)
	// L0 layer query
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7 layer query
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}

func (lm *levelManager) loadCache() {

}
func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}
func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}

	manifest := lm.manifestFile.GetManifest()
	// compare the correctness of the manifest file
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	// load the index block of each sstable to build the cache
	lm.cache = newCache(lm.opt)
	// TODO initialize the index structure in the table, which is equivalent to loading all of them into memory, reducing one read disk, but increasing memory consumption
	var maxFID uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t) // 记录一个level的文件总大小
	}
	// sort each level
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// get the maximum fid value
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

// flush a sstable to L0 layer
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// allocate a fid
	fid := immutable.wal.Fid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	// build a builder
	builder := newTableBuiler(lm.opt)
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	// create a table object
	table := openTable(lm, sstName, builder)
	err = lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
	// panic if manifest write fails
	utils.Panic(err)
	// update the manifest file
	lm.levels[0].add(table)
	return
}

//--------- levelHandler ---------
type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}

func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].ss.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}
func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
}

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize
}

func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}

func (lh *levelHandler) subtractSize(t *table) {
	lh.totalSize -= t.Size()
	lh.totalStaleSize -= int64(t.StaleDataSize())
}

func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	// if it is the 0th layer file, handle it specially
	if lh.levelNum == 0 {
		// get the sst that may contain the key
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// Sort tables by keys.
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[j].ss.MinKey()) < 0
		})
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Search(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Search(key, &version); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) getTable(key []byte) *table {
	if len(lh.tables) > 0 && (bytes.Compare(key, lh.tables[0].ss.MinKey()) < 0 || bytes.Compare(key, lh.tables[len(lh.tables)-1].ss.MaxKey()) > 0) {
		return nil
	} else {
		for i := len(lh.tables) - 1; i >= 0; i-- {
			if bytes.Compare(key, lh.tables[i].ss.MinKey()) > -1 &&
				bytes.Compare(key, lh.tables[i].ss.MaxKey()) < 1 {
				return lh.tables[i]
			}
		}
	}
	return nil
}
func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (lh *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.left, lh.tables[i].ss.MaxKey()) <= 0
	})
	right := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.right, lh.tables[i].ss.MaxKey()) < 0
	})
	return left, right
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (lh *levelHandler) replaceTables(toDel, toAdd []*table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	lh.Lock() // We s.Unlock() below.

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}

	// Increase totalSize first.
	for _, t := range toAdd {
		lh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	// Assign tables.
	lh.tables = newTables
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[i].ss.MinKey()) < 0
	})
	lh.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDel)
}

// deleteTables remove tables idx0, ..., idx1-1.
func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	lh.tables = newTables

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(toDel)
}

func (lh *levelHandler) iterators() []utils.Iterator {
	lh.RLock()
	defer lh.RUnlock()
	topt := &utils.Options{IsAsc: true}
	if lh.levelNum == 0 {
		return iteratorsReversed(lh.tables, topt)
	}

	if len(lh.tables) == 0 {
		return nil
	}
	return []utils.Iterator{NewConcatIterator(lh.tables, topt)}
}