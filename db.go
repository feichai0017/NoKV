package NoKV

import (
	"expvar"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/hotring"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/pkg/errors"
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
		opt         *Options
		lsm         *lsm.LSM
		wal         *wal.Manager
		vlog        *valueLog
		stats       *Stats
		flushChan   chan flushTask // For flushing memtables.
		writeCh     chan *request
		batchCh     chan *writeBatch
		applyCh 	chan *writeBatch
		writeCloser *utils.Closer
		blockWrites int32
		vhead       *utils.ValuePtr
		logRotates  int32
		isClosed    uint32
		orc         *oracle
		hot         *hotring.HotRing
	}
	
	writeBatch struct {
		reqs 	[]*request
		entries int
		size 	int64
	}
)

var (
	head = []byte("!NoKV!head") // For storing value offset for replay.
)


const (
	defaultWriteBatchMaxCount 	= 64
	defaultWriteBatchMaxSize 	= 1 << 20
	defaultWriteBatchDelay 		= 2 * time.Millisecond
)

// Open DB
// TODO 这里是不是要上一个目录锁比较好，防止多个进程打开同一个目录?
func Open(opt *Options) *DB {
	c := utils.NewCloser()
	db := &DB{opt: opt, writeCloser: c}
	db.initWriteBatchOptions()

	wlog, err := wal.Open(wal.Config{
		Dir: opt.WorkDir,
	})
	utils.Panic(err)
	db.wal = wlog
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0, //0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
	}, wlog)
	db.lsm.SetThrottleCallback(db.applyThrottle)
	// 初始化vlog结构
	db.initVLog()
	db.lsm.SetDiscardStatsCh(&(db.vlog.lfDiscardStats.flushChan))
	// 初始化统计信息
	db.stats = newStats(db)

	if opt.HotRingEnabled {
		db.hot = hotring.NewHotRing(opt.HotRingBits, nil)
		if opt.HotRingTopK <= 0 {
			opt.HotRingTopK = 16
		}
	}

	db.orc = newOracle(*opt)
	// 启动 sstable 的合并压缩过程
	go db.lsm.StartCompacter()
	// 准备vlog gc
	c.Add(1)
	db.writeCh = make(chan *request)
	db.batchCh = make(chan *writeBatch, 4)
	db.applyCh = make(chan *writeBatch, 4)
	db.flushChan = make(chan flushTask, 16)
	go db.doWrites(c)
	// 启动 info 统计过程
	db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	if db.writeCloser != nil {
		db.writeCloser.Close()
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
	atomic.StoreUint32(&db.isClosed, 1)
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	return db.Set(&utils.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
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
	return db.lsm.Set(data)
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
			val = &utils.Entry{
				Key:   headKey,
				Value: []byte{},
			}
		} else {
			return errors.Wrap(err, "Retrieving head from on-disk LSM")
		}
	}

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
	db.hot.Touch(string(key))
}

func (db *DB) shouldWriteValueToLSM(e *utils.Entry) bool {
	return int64(len(e.Value)) < db.opt.ValueThreshold
}

func (db *DB) sendToWriteCh(entries []*utils.Entry) (*request, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}
	var count, size int64
	for _, e := range entries {
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
		count++
	}
	if count >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	// TODO 尝试使用对象复用，后面entry对象也应该使用
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()     // for db write
	db.writeCh <- req // Handled in doWrites.
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

func (db *DB) doWrites(lc *utils.Closer) {
	defer lc.Done()
	lc.Add(2)
	go db.processValueLogBatches(lc)
	go db.applyBatches(lc)
	
	reqLen := new(expvar.Int)
	
	var (
		pending      []*request
		pendingCount int
		pendingSize  int64
		maxCount     = db.opt.WriteBatchMaxCount
		maxSize      = db.opt.WriteBatchMaxSize
		delay        = db.opt.WriteBatchDelay
		timer        *time.Timer
		timerActive  bool
	)
	resetTimer := func() {
		if delay <= 0 {
			return
		}
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		}
		timerActive = true
	}

	stopTimer := func() {
		if delay <= 0 || !timerActive || timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerActive = false
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := &writeBatch{
			reqs:    pending,
			entries: pendingCount,
			size:    pendingSize,
		}
		db.batchCh <- batch
		pending = nil
		pendingCount = 0
		pendingSize = 0
		reqLen.Set(0)
		stopTimer()
	}

	for {
		var timerC <-chan time.Time
		if timerActive && timer != nil {
			timerC = timer.C
		}

		select {
		case r := <-db.writeCh:
			if r == nil {
				continue
			}
			pending = append(pending, r)
			for _, e := range r.Entries {
				pendingSize += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
			}
			pendingCount += len(r.Entries)
			reqLen.Set(int64(len(pending)))

			if len(pending) == 1 {
				resetTimer()
			}

			if (maxCount > 0 && pendingCount >= maxCount) || (maxSize > 0 && pendingSize >= maxSize) || len(pending) >= 3*utils.KVWriteChCapacity {
				flush()
			}
		case <-timerC:
			timerActive = false
			flush()
		case <-lc.CloseSignal:
			stopTimer()
			flush()
			close(db.batchCh)
			return
		}
	}
}

// getMemtables returns the current memtables and get references.
func (db *DB) getMemTables() ([]*lsm.MemTable, func()) {
	ml, fn := db.lsm.GetMemTables()
	rm := make([]*lsm.MemTable, len(ml))
	for _, mt := range ml {
		rm = append(rm, mt)
	}
	return rm, fn
}

// writeRequests is called serially by only one goroutine.
func (db *DB) applyWriteBatch(batch *writeBatch) error {
	if batch == nil || len(batch.reqs) == 0 {
		return nil
	}

	done := func(err error) {
		completeRequests(batch.reqs, err)
	}
	var count int
	for _, b := range batch.reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err := db.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		db.Lock()
		db.updateHead(b.Ptrs)
		db.Unlock()
	}
	done(nil)
	return nil
}

func (db *DB) processValueLogBatches(lc *utils.Closer) {
	defer lc.Done()
	for batch := range db.batchCh {
		if len(batch.reqs) == 0 {
			continue
		}
		if err := db.vlog.write(batch.reqs); err != nil {
			completeRequests(batch.reqs, err)
			continue
		}
		db.applyCh <- batch
	}
	close(db.applyCh)
}

func (db *DB) applyBatches(lc *utils.Closer) {
	defer lc.Done()
	for batch := range db.applyCh {
		if err := db.applyWriteBatch(batch); err != nil {
			utils.Err(fmt.Errorf("applyWriteBatch: %v", err))
		}
	}
}

func completeRequests(reqs []*request, err error) {
	for _, r := range reqs {
		if r == nil {
			continue
		}
		r.Err = err
		r.Wg.Done()
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
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
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
func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.Entries = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
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
		return errors.New("Head should not be zero")
	}

	fmt.Printf("Storing value log head: %+v\n", ft.vptr)
	val := ft.vptr.Encode()

	// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
	// commits.
	headTs := utils.KeyWithTs(head, uint64(time.Now().Unix()/1e9))
	ft.mt.Add(&utils.Entry{
		Key:   headTs,
		Value: val,
	})
	return nil
}

func (db *DB) valueThreshold() int64 {
	return atomic.LoadInt64(&db.opt.ValueThreshold)
}

func (db *DB) IsClosed() bool {
	return atomic.LoadUint32(&db.isClosed) == 1
}
