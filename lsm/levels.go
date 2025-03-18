package lsm

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/utils"
)

// initLevelManager 初始化函数
func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm} // 反引用
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	// 读取manifest文件构建管理器
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

type levelManager struct {
	maxFID       uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
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
	// L0层查询
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7层查询
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
	// 对比manifest 文件的正确性
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	// 逐一加载sstable 的index block 构建cache
	lm.cache = newCache(lm.opt)
	// TODO 初始化的时候index 结构放在了table中，相当于全部加载到了内存，减少了一次读磁盘，但增加了内存消耗
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
	// 对每一层进行排序
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// 得到最大的fid值
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// 分配一个fid
	fid := immutable.wal.Fid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	// 构建一个 builder
	builder := newTableBuiler(lm.opt)
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	// 创建一个 table 对象
	table := openTable(lm, sstName, builder)
	err = lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
	// manifest写入失败直接panic
	utils.Panic(err)
	// 更新manifest文件
	lm.levels[0].add(table)
	return
}

//--------- level处理器 -------
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
	// 如果是第0层文件则进行特殊处理
	if lh.levelNum == 0 {
		// TODO: logic...
		// 获取可能存在key的sst
		return lh.searchL0SST(key)
	} else {
		// TODO: logic...
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