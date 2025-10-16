package NoKV

import (
	stderrors "errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/hotring"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
	pkgerrors "github.com/pkg/errors"
)

type (
	// NoKV对外提供的功能集合
	CoreAPI interface {
		Set(data *utils.Entry) error
		Get(key []byte) (*utils.Entry, error)
		Del(key []byte) error
		NewIterator(opt *utils.Options) utils.Iterator
		Info() *Stats
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
	DB struct {
		sync.RWMutex
		opt              *Options
		dirLock          *utils.DirLock
		lsm              *lsm.LSM
		wal              *wal.Manager
		vlog             *valueLog
		stats            *Stats
		flushChan        chan flushTask // For flushing memtables.
		blockWrites      int32
		vhead            *utils.ValuePtr
		lastLoggedHead   utils.ValuePtr
		headLogDelta     uint32
		logRotates       int32
		isClosed         uint32
		orc              *oracle
		hot              *hotring.HotRing
		writeMetrics     *writeMetrics
		commitQueue      commitQueue
		commitLock       chan struct{}
		iterPool         *iteratorPool
		prefetchCh       chan prefetchRequest
		prefetchWG       sync.WaitGroup
		prefetchMu       sync.Mutex
		prefetchPend     map[string]struct{}
		prefetched       map[string]time.Time
		prefetchClamp    int32
		prefetchWarm     int32
		prefetchHot      int32
		prefetchCooldown time.Duration
	}

	commitQueue struct {
		sync.Mutex
		requests     []*commitRequest
		totalEntries int
		totalBytes   int64
	}

	commitRequest struct {
		req        *request
		done       chan error
		entryCount int
		size       int64
	}
)

var (
	head = []byte("!NoKV!head") // For storing value offset for replay.
)

type prefetchRequest struct {
	key string
	hot bool
}

const (
	defaultWriteBatchMaxCount = 64
	defaultWriteBatchMaxSize  = 1 << 20
	defaultWriteBatchDelay    = 2 * time.Millisecond
)

// Open DB
func Open(opt *Options) *DB {
	db := &DB{opt: opt, writeMetrics: newWriteMetrics()}
	db.headLogDelta = valueLogHeadLogInterval
	db.initWriteBatchOptions()

	if db.opt.BlockCacheSize < 0 {
		db.opt.BlockCacheSize = 0
	}
	if db.opt.BlockCacheSize == 0 {
		// Disable caches explicitly when set to zero, otherwise fall back to default.
		db.opt.BlockCacheHotFraction = 0
	} else if db.opt.BlockCacheHotFraction <= 0 || db.opt.BlockCacheHotFraction >= 1 {
		db.opt.BlockCacheHotFraction = 0.25
	}
	if db.opt.BloomCacheSize < 0 {
		db.opt.BloomCacheSize = 0
	}

	lock, err := utils.AcquireDirLock(opt.WorkDir)
	utils.Panic(err)
	db.dirLock = lock

	utils.Panic(db.runRecoveryChecks())

	wlog, err := wal.Open(wal.Config{
		Dir:         opt.WorkDir,
		SyncOnWrite: false,
	})
	utils.Panic(err)
	db.wal = wlog
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:                opt.WorkDir,
		MemTableSize:           opt.MemTableSize,
		SSTableMaxSz:           opt.SSTableMaxSz,
		BlockSize:              8 * 1024,
		BloomFalsePositive:     0, //0.01,
		BaseLevelSize:          10 << 20,
		LevelSizeMultiplier:    10,
		BaseTableSize:          5 << 20,
		TableSizeMultiplier:    2,
		NumLevelZeroTables:     15,
		MaxLevelNum:            7,
		NumCompactors:          1,
		IngestCompactBatchSize: 4,
		BlockCacheSize:         db.opt.BlockCacheSize,
		BlockCacheHotFraction:  db.opt.BlockCacheHotFraction,
		BloomCacheSize:         db.opt.BloomCacheSize,
	}, wlog)
	db.lsm.SetThrottleCallback(db.applyThrottle)
	recoveredVersion := db.lsm.MaxVersion()
	// 初始化vlog结构
	db.initVLog()
	db.lsm.SetDiscardStatsCh(&(db.vlog.lfDiscardStats.flushChan))
	// 初始化统计信息
	db.iterPool = newIteratorPool()
	db.stats = newStats(db)

	if opt.HotRingEnabled {
		db.hot = hotring.NewHotRing(opt.HotRingBits, nil)
		if opt.HotRingTopK <= 0 {
			opt.HotRingTopK = 16
		}
		db.prefetchClamp = 64
		db.prefetchWarm = 4
		db.prefetchHot = 16
		if db.prefetchHot <= db.prefetchWarm {
			db.prefetchHot = db.prefetchWarm + 4
		}
		db.prefetchCooldown = 15 * time.Second
		db.prefetchPend = make(map[string]struct{})
		db.prefetched = make(map[string]time.Time)
		db.prefetchCh = make(chan prefetchRequest, 128)
		db.prefetchWG.Add(1)
		go db.prefetchLoop()
	}

	db.orc = newOracle(*opt)
	db.orc.initCommitState(recoveredVersion)
	// 启动 sstable 的合并压缩过程
	go db.lsm.StartCompacter()
	// 准备vlog gc
	db.commitLock = make(chan struct{}, 1)
	db.flushChan = make(chan flushTask, 16)
	// 启动 info 统计过程
	db.stats.StartStats()
	return db
}

func (db *DB) runRecoveryChecks() error {
	if db == nil || db.opt == nil {
		return fmt.Errorf("recovery checks: options not initialized")
	}
	if err := manifest.Verify(db.opt.WorkDir); err != nil {
		if !stderrors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := wal.VerifyDir(db.opt.WorkDir); err != nil {
		return err
	}
	vlogDir := filepath.Join(db.opt.WorkDir, "vlog")
	cfg := vlogpkg.Config{
		Dir:      vlogDir,
		FileMode: utils.DefaultFileMode,
		MaxSize:  int64(db.opt.ValueLogFileSize),
	}
	if err := vlogpkg.VerifyDir(cfg); err != nil {
		if !stderrors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (db *DB) Close() error {

	if db.prefetchCh != nil {
		close(db.prefetchCh)
		db.prefetchWG.Wait()
		db.prefetchCh = nil
	}
	
	db.vlog.lfDiscardStats.closer.Close()
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.close(); err != nil {
		return err
	}
	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := db.stats.close(); err != nil {
		return err
	}
	
	if db.dirLock != nil {
		if err := db.dirLock.Release(); err != nil {
			return err
		}
		db.dirLock = nil
	}
	atomic.StoreUint32(&db.isClosed, 1)
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	e := utils.NewEntry(key, nil)
	err := db.Set(e)
	e.DecrRef()
	return err
}
func (db *DB) Set(data *utils.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 做一些必要性的检查
	// 如果value 大于一个阈值 则创建值指针，并将其写入vlog中
	var (
		vp  *utils.ValuePtr
		err error
	)
	data.Key = utils.KeyWithTs(data.Key, math.MaxUint32)
	// 如果value不应该直接写入LSM 则先写入 vlog文件，这时必须保证vlog具有重放功能
	// 以便于崩溃后恢复数据
	if !db.shouldWriteValueToLSM(data) {
		if vp, err = db.vlog.newValuePtr(data); err != nil {
			return err
		}
		data.Meta |= utils.BitValuePointer
		data.Value = vp.Encode()
	}
	if err := db.lsm.Set(data); err != nil {
		return err
	}
	if db.opt.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}
	return nil
}
func (db *DB) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}

	originKey := key
	// 添加时间戳用于查询
	key = utils.KeyWithTs(key, math.MaxUint32)

	var (
		entry *utils.Entry
		err   error
	)
	// 从LSM中查询entry，这时不确定entry是不是值指针
	if entry, err = db.lsm.Get(key); err != nil {
		return entry, err
	}
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	if entry != nil && utils.IsValuePtr(entry) {
		var vp utils.ValuePtr
		vp.Decode(entry.Value)
		result, cb, err := db.vlog.read(&vp)
		defer utils.RunCallback(cb)
		if err != nil {
			return nil, err
		}
		entry.Value = utils.SafeCopy(nil, result)
	}

	if isDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
		return nil, utils.ErrKeyNotFound
	}
	entry.Key = originKey
	db.recordRead(originKey)
	return entry, nil
}

// 判断是否过期 是可删除
func isDeletedOrExpiredByEntry(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&utils.BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

func (db *DB) Info() *Stats {
	// 读取stats结构，打包数据并返回
	return db.stats
}

// RunValueLogGC triggers a value log garbage collection.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	// Find head on disk
	headKey := utils.KeyWithTs(head, math.MaxUint64)
	val, err := db.lsm.Get(headKey)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = utils.NewEntry(headKey, []byte{})
		} else {
			return pkgerrors.Wrap(err, "Retrieving head from on-disk LSM")
		}
	}
	defer val.DecrRef()

	// 内部key head 一定是value ptr 不需要检查内容
	var head utils.ValuePtr
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	return db.vlog.runGC(discardRatio, &head)
}

func (db *DB) recordRead(key []byte) {
	if db == nil || db.hot == nil || len(key) == 0 {
		return
	}
	skey := string(key)
	if db.prefetchCh == nil {
		db.hot.Touch(skey)
		return
	}
	clamp := db.prefetchClamp
	if clamp <= 0 {
		clamp = db.prefetchHot
		if clamp <= 0 {
			clamp = db.prefetchWarm
		}
		if clamp <= 0 {
			clamp = 1
		}
	}
	count, _ := db.hot.TouchAndClamp(skey, clamp)
	if db.prefetchHot > 0 && count >= db.prefetchHot {
		db.enqueuePrefetch(skey, true)
		return
	}
	if db.prefetchWarm > 0 && count >= db.prefetchWarm {
		db.enqueuePrefetch(skey, false)
	}
}

func (db *DB) enqueuePrefetch(key string, hot bool) {
	if db == nil || db.prefetchCh == nil || key == "" {
		return
	}
	now := time.Now()
	db.prefetchMu.Lock()
	if expiry, ok := db.prefetched[key]; ok {
		if expiry.After(now) {
			db.prefetchMu.Unlock()
			return
		}
		delete(db.prefetched, key)
	}
	if _, pending := db.prefetchPend[key]; pending {
		db.prefetchMu.Unlock()
		return
	}
	db.prefetchPend[key] = struct{}{}
	db.prefetchMu.Unlock()

	req := prefetchRequest{key: key, hot: hot}
	select {
	case db.prefetchCh <- req:
	default:
		db.prefetchMu.Lock()
		delete(db.prefetchPend, key)
		db.prefetchMu.Unlock()
	}
}

func (db *DB) prefetchLoop() {
	defer db.prefetchWG.Done()
	for req := range db.prefetchCh {
		db.executePrefetch(req)
	}
}

func (db *DB) executePrefetch(req prefetchRequest) {
	if db == nil {
		return
	}
	key := req.key
	if key == "" {
		db.prefetchMu.Lock()
		delete(db.prefetchPend, key)
		db.prefetchMu.Unlock()
		return
	}
	if db.lsm != nil {
		internal := utils.KeyWithTs([]byte(key), math.MaxUint32)
		db.lsm.Prefetch(internal, req.hot)
	}
	db.prefetchMu.Lock()
	delete(db.prefetchPend, key)
	if db.prefetchCooldown > 0 {
		db.prefetched[key] = time.Now().Add(db.prefetchCooldown)
	} else {
		delete(db.prefetched, key)
	}
	db.prefetchMu.Unlock()
}

func (db *DB) shouldWriteValueToLSM(e *utils.Entry) bool {
	return int64(len(e.Value)) < db.opt.ValueThreshold
}

func (db *DB) sendToWriteCh(entries []*utils.Entry) (*request, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}
	var size int64
	count := int64(len(entries))
	for _, e := range entries {
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
	}
	if count >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.enqueueAt = time.Now()
	done := make(chan error, 1)
	req.doneCh = done
	req.IncrRef() // for db write

	cr := &commitRequest{
		req:        req,
		done:       done,
		entryCount: int(count),
		size:       size,
	}

	db.enqueueCommitRequest(cr)

	if db.tryBecomeCommitLeader() {
		db.processCommitQueue()
	}
	return req, nil
}

func (db *DB) applyThrottle(enable bool) {
	var val int32
	if enable {
		val = 1
	}
	prev := atomic.SwapInt32(&db.blockWrites, val)
	if prev == val {
		return
	}
	if enable {
		utils.Err(fmt.Errorf("write throttle enabled due to L0 backlog"))
	} else {
		utils.Err(fmt.Errorf("write throttle released"))
	}
}

// Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*utils.Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

func (db *DB) enqueueCommitRequest(cr *commitRequest) {
	if cr == nil {
		return
	}
	db.commitQueue.Lock()
	db.commitQueue.requests = append(db.commitQueue.requests, cr)
	db.commitQueue.totalEntries += cr.entryCount
	db.commitQueue.totalBytes += cr.size
	qLen := len(db.commitQueue.requests)
	qEntries := db.commitQueue.totalEntries
	qBytes := db.commitQueue.totalBytes
	db.commitQueue.Unlock()
	db.writeMetrics.updateQueue(qLen, qEntries, qBytes)
}

func (db *DB) tryBecomeCommitLeader() bool {
	if db.commitLock == nil {
		return false
	}
	select {
	case db.commitLock <- struct{}{}:
		return true
	default:
		return false
	}
}

func (db *DB) releaseCommitLeader() {
	if db.commitLock == nil {
		return
	}
	select {
	case <-db.commitLock:
	default:
	}
}

func (db *DB) processCommitQueue() {
	for {
		db.commitQueue.Lock()
		if len(db.commitQueue.requests) == 0 {
			db.releaseCommitLeader()
			db.commitQueue.Unlock()
			return
		}
		reqs := db.commitQueue.requests
		db.commitQueue.requests = nil
		db.commitQueue.totalEntries = 0
		db.commitQueue.totalBytes = 0
		db.commitQueue.Unlock()
		db.writeMetrics.updateQueue(0, 0, 0)
		db.handleCommitRequests(reqs)
	}
}

func (db *DB) handleCommitRequests(reqs []*commitRequest) {
	if len(reqs) == 0 {
		return
	}

	requests := make([]*request, 0, len(reqs))
	var (
		totalEntries int
		totalSize    int64
		waitSum      int64
	)
	now := time.Now()
	for _, cr := range reqs {
		if cr == nil || cr.req == nil {
			continue
		}
		r := cr.req
		requests = append(requests, r)
		totalEntries += len(r.Entries)
		totalSize += cr.size
		if !r.enqueueAt.IsZero() {
			waitSum += now.Sub(r.enqueueAt).Nanoseconds()
			r.enqueueAt = time.Time{}
		}
	}

	if len(requests) == 0 {
		db.finishCommitRequests(reqs, nil)
		return
	}

	db.writeMetrics.recordBatch(len(requests), totalEntries, totalSize, waitSum)

	start := time.Now()
	if err := db.vlog.write(requests); err != nil {
		db.finishCommitRequests(reqs, err)
		return
	}
	db.writeMetrics.recordValueLog(time.Since(start))

	start = time.Now()
	if err := db.applyRequests(requests); err != nil {
		db.finishCommitRequests(reqs, err)
		return
	}
	db.writeMetrics.recordApply(time.Since(start))

	if db.opt.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			db.finishCommitRequests(reqs, err)
			return
		}
	}

	db.finishCommitRequests(reqs, nil)
}

func (db *DB) applyRequests(reqs []*request) error {
	for _, r := range reqs {
		if r == nil || len(r.Entries) == 0 {
			continue
		}
		if err := db.writeToLSM(r); err != nil {
			return pkgerrors.Wrap(err, "writeRequests")
		}
		db.Lock()
		db.updateHead(r.Ptrs)
		db.Unlock()
	}
	return nil
}

func (db *DB) finishCommitRequests(reqs []*commitRequest, err error) {
	for _, cr := range reqs {
		if cr == nil || cr.req == nil {
			continue
		}
		cr.req.Err = err
		switch {
		case cr.done != nil:
			cr.done <- err
			close(cr.done)
		case cr.req.doneCh != nil:
			cr.req.doneCh <- err
			close(cr.req.doneCh)
		}
	}
}

func (db *DB) initWriteBatchOptions() {
	if db.opt.WriteBatchMaxCount <= 0 {
		db.opt.WriteBatchMaxCount = defaultWriteBatchMaxCount
	}
	if db.opt.WriteBatchMaxSize <= 0 {
		db.opt.WriteBatchMaxSize = defaultWriteBatchMaxSize
	}
	if db.opt.WriteBatchDelay < 0 {
		db.opt.WriteBatchDelay = 0
	}
	if db.opt.WriteBatchDelay == 0 {
		db.opt.WriteBatchDelay = defaultWriteBatchDelay
	}
}

func (db *DB) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return pkgerrors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if db.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			entry.Meta = entry.Meta &^ utils.BitValuePointer
		} else {
			entry.Meta = entry.Meta | utils.BitValuePointer
			entry.Value = b.Ptrs[i].Encode()
		}
		db.lsm.Set(entry)
	}
	return nil
}


// 结构体
type flushTask struct {
	mt           *utils.Skiplist
	vptr         *utils.ValuePtr
	dropPrefixes [][]byte
}

func (db *DB) pushHead(ft flushTask) error {
	// Ensure we never push a zero valued head pointer.
	if ft.vptr.IsZero() {
		return stderrors.New("Head should not be zero")
	}

	fmt.Printf("Storing value log head: %+v\n", ft.vptr)
	val := ft.vptr.Encode()

	// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
	// commits.
	headTs := utils.KeyWithTs(head, uint64(time.Now().Unix()/1e9))
	e := utils.NewEntry(headTs, val)
	ft.mt.Add(e)
	e.DecrRef()
	return nil
}

func (db *DB) valueThreshold() int64 {
	return atomic.LoadInt64(&db.opt.ValueThreshold)
}

func (db *DB) IsClosed() bool {
	return atomic.LoadUint32(&db.isClosed) == 1
}
