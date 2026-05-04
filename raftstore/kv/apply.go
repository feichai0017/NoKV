package kv

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/percolator/mvcc"
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
	"github.com/feichai0017/NoKV/utils"
)

const defaultLatchSlots = 512

// Apply executes a RaftCmdRequest against the provided DB. The returned
// response mirrors the request ordering. Mutation commands are expected to
// arrive through the replicated raft log.
func Apply(db txnstore.Store, latches *latch.Manager, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("kv: nil raft command")
	}
	if latches == nil {
		latches = latch.NewManager(defaultLatchSlots)
	}
	resp := &raftcmdpb.RaftCmdResponse{Header: req.Header}
	for _, r := range req.Requests {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case raftcmdpb.CmdType_CMD_GET:
			result, keyErr, err := handleGet(db, r.GetGet())
			if err != nil {
				return nil, err
			}
			if keyErr != nil {
				result.Error = keyErr
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Get{Get: result}})
		case raftcmdpb.CmdType_CMD_PREWRITE:
			result := &kvrpcpb.PrewriteResponse{Errors: percolator.Prewrite(db, latches, r.GetPrewrite())}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Prewrite{Prewrite: result}})
		case raftcmdpb.CmdType_CMD_COMMIT:
			err := percolator.Commit(db, latches, r.GetCommit())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{Error: err}}})
		case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
			err := percolator.BatchRollback(db, latches, r.GetBatchRollback())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackResponse{Error: err}}})
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			count, err := percolator.ResolveLock(db, latches, r.GetResolveLock())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{ResolvedLocks: count, Error: err}}})
		case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
			result := percolator.CheckTxnStatus(db, latches, r.GetCheckTxnStatus())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_CheckTxnStatus{CheckTxnStatus: result}})
		case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
			result := percolator.TxnHeartBeat(db, latches, r.GetTxnHeartBeat())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TxnHeartBeat{TxnHeartBeat: result}})
		case raftcmdpb.CmdType_CMD_FSMETA_CREATE:
			err := percolator.ApplyFSMetaCreate(db, latches, r.GetFsmetaCreate())
			result := &kvrpcpb.FSMetaCreateResponse{Error: err}
			if err == nil {
				result.AppliedKeys = uint64(len(r.GetFsmetaCreate().GetMutations()))
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_FsmetaCreate{FsmetaCreate: result}})
		case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
			result, err := applyMVCCMaintenance(db, r.GetMvccMaintenance())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_MvccMaintenance{MvccMaintenance: result}})
		case raftcmdpb.CmdType_CMD_SCAN:
			result, err := handleScan(db, r.GetScan())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Scan{Scan: result}})
		default:
			return nil, fmt.Errorf("kv: unsupported command %v", r.GetCmdType())
		}
	}
	return resp, nil
}

func applyMVCCMaintenance(db txnstore.Store, req *kvrpcpb.MVCCMaintenanceRequest) (*kvrpcpb.MVCCMaintenanceResponse, error) {
	entries, keyErr := buildMVCCMaintenanceEntries(req)
	if keyErr != nil {
		return &kvrpcpb.MVCCMaintenanceResponse{Error: keyErr}, nil
	}
	if len(entries) == 0 {
		return &kvrpcpb.MVCCMaintenanceResponse{}, nil
	}
	defer func() {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
	}()
	// ApplyInternalEntries is the raft apply batch boundary for MVCC
	// maintenance. NoKV's DB implementation maps it to one atomic LSM batch;
	// if another Store implementation reports an error after partial
	// persistence, the caller retries the whole tombstone batch and relies on
	// tombstones being idempotent.
	if err := db.ApplyInternalEntries(entries); err != nil {
		return nil, err
	}
	return &kvrpcpb.MVCCMaintenanceResponse{AppliedEntries: uint64(len(entries))}, nil
}

func buildMVCCMaintenanceEntries(req *kvrpcpb.MVCCMaintenanceRequest) ([]*kv.Entry, *kvrpcpb.KeyError) {
	if req == nil || len(req.GetTombstones()) == 0 {
		return nil, nil
	}
	entries := make([]*kv.Entry, 0, len(req.GetTombstones()))
	for i, tombstone := range req.GetTombstones() {
		if tombstone == nil {
			continue
		}
		cf, ok := maintenanceColumnFamily(tombstone.GetColumnFamily())
		if !ok {
			releaseEntries(entries)
			return nil, maintenanceAbort("invalid column family")
		}
		if len(tombstone.GetKey()) == 0 {
			releaseEntries(entries)
			return nil, maintenanceAbort("empty key")
		}
		entry := kv.NewInternalEntry(cf, tombstone.GetKey(), tombstone.GetVersion(), nil, kv.BitDelete, 0)
		if entry == nil {
			releaseEntries(entries)
			return nil, maintenanceAbort(fmt.Sprintf("entry %d build failed", i))
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func releaseEntries(entries []*kv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func maintenanceColumnFamily(cf kvrpcpb.InternalEntryTombstone_ColumnFamily) (kv.ColumnFamily, bool) {
	switch cf {
	case kvrpcpb.InternalEntryTombstone_DEFAULT:
		return kv.CFDefault, true
	case kvrpcpb.InternalEntryTombstone_WRITE:
		return kv.CFWrite, true
	default:
		return 0, false
	}
}

func maintenanceAbort(msg string) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Abort: msg}
}

// NewApplier wraps Apply into a reusable function suitable for store command
// execution wiring.
func NewApplier(db txnstore.Store, latches *latch.Manager) func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if latches == nil {
		latches = latch.NewManager(defaultLatchSlots)
	}
	return func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return Apply(db, latches, req)
	}
}

func handleGet(db txnstore.Store, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, *kvrpcpb.KeyError, error) {
	if req == nil {
		return &kvrpcpb.GetResponse{NotFound: true}, nil, nil
	}
	reader := percolator.NewReader(db)
	lock, err := reader.GetLock(req.GetKey())
	if err != nil {
		return nil, nil, err
	}
	if lock != nil && req.GetVersion() >= lock.Ts {
		keyErr := &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, req.GetKey()),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		}}
		return &kvrpcpb.GetResponse{}, keyErr, nil
	}
	val, expiresAt, err := reader.GetValue(req.GetKey(), req.GetVersion())
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return &kvrpcpb.GetResponse{NotFound: true}, nil, nil
		}
		return nil, nil, err
	}
	return &kvrpcpb.GetResponse{Value: val, ExpiresAt: expiresAt}, nil, nil
}

func handleScan(db txnstore.Store, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	if req == nil {
		return &kvrpcpb.ScanResponse{}, nil
	}
	if req.GetReverse() {
		return nil, fmt.Errorf("kv: reverse scan not supported")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 1
	}
	readTs := req.GetVersion()
	if readTs == 0 {
		readTs = kv.MaxVersion
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()

	startKey := append([]byte(nil), req.GetStartKey()...)
	includeStart := req.GetIncludeStart()
	started := len(startKey) == 0

	resp := &kvrpcpb.ScanResponse{}
	iter.Seek(kv.InternalKey(kv.CFWrite, startKey, kv.MaxVersion))
	reader := percolator.NewReader(db)
	for iter.Valid() && len(resp.Kvs) < limit {
		item := iter.Item()
		if item == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		if entry == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			return nil, fmt.Errorf("kv: scan iterator expects internal key, got %x", entry.Key)
		}
		if cf != kv.CFWrite {
			// Since iterator is seeked into CFWrite range, encountering any non-write CF
			// means there are no more write records for subsequent keys.
			break
		}
		key := kv.SafeCopy(nil, userKey)
		if len(key) == 0 {
			iter.Next()
			continue
		}
		if !started {
			cmp := bytes.Compare(key, startKey)
			if cmp < 0 || (cmp == 0 && !includeStart) {
				advanceToNextUserKey(iter, key)
				continue
			}
			started = true
		}
		lock, err := reader.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && readTs >= lock.Ts {
			resp.Error = lockedError(key, lock)
			advanceToNextUserKey(iter, key)
			break
		}
		value, expiresAt, found, err := collectVisibleValue(db, iter, key, readTs)
		if err != nil {
			return nil, err
		}
		if found {
			resp.Kvs = append(resp.Kvs, &kvrpcpb.KV{
				Key:       key,
				Value:     value,
				Version:   readTs,
				ExpiresAt: expiresAt,
			})
		}
	}
	return resp, nil
}

func advanceToNextUserKey(iter index.Iterator, current []byte) {
	if iter == nil {
		return
	}
	for iter.Next(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil {
			continue
		}
		entry := item.Entry()
		if entry == nil {
			continue
		}
		_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			utils.CondPanicFunc(true, func() error {
				return fmt.Errorf("kv: advanceToNextUserKey expects internal key, got %x", entry.Key)
			})
			return
		}
		if !bytes.Equal(userKey, current) {
			return
		}
	}
}

func collectVisibleValue(db txnstore.Store, iter index.Iterator, key []byte, readTs uint64) ([]byte, uint64, bool, error) {
	for iter.Valid() {
		item := iter.Item()
		if item == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		if entry == nil {
			iter.Next()
			continue
		}
		cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			return nil, 0, false, fmt.Errorf("kv: collectVisibleValue expects internal key, got %x", entry.Key)
		}
		if cf != kv.CFWrite || !bytes.Equal(userKey, key) {
			return nil, 0, false, nil
		}
		if ts > readTs {
			iter.Next()
			continue
		}
		write, err := mvcc.DecodeWrite(entry.Value)
		if err != nil {
			return nil, 0, false, err
		}
		switch write.Kind {
		case kvrpcpb.Mutation_Lock, kvrpcpb.Mutation_Rollback:
			iter.Next()
			continue
		case kvrpcpb.Mutation_Delete:
			advanceToNextUserKey(iter, key)
			return nil, 0, false, nil
		default:
			var value []byte
			var expiresAt uint64
			if len(write.ShortValue) > 0 {
				value = write.ShortValue
				expiresAt = write.ExpiresAt
				if expiresAt > 0 && expiresAt <= uint64(time.Now().Unix()) {
					advanceToNextUserKey(iter, key)
					return nil, 0, false, nil
				}
			} else {
				entryVal, err := db.GetInternalEntry(kv.CFDefault, key, write.StartTs)
				if err != nil {
					if errors.Is(err, utils.ErrKeyNotFound) {
						iter.Next()
						continue
					}
					return nil, 0, false, err
				}
				if entryVal.IsDeletedOrExpired() {
					entryVal.DecrRef()
					advanceToNextUserKey(iter, key)
					return nil, 0, false, nil
				}
				value = kv.SafeCopy(nil, entryVal.Value)
				expiresAt = entryVal.ExpiresAt
				entryVal.DecrRef()
			}
			advanceToNextUserKey(iter, key)
			return value, expiresAt, true, nil
		}
	}
	return nil, 0, false, nil
}

func lockedError(key []byte, lock *mvcc.Lock) *kvrpcpb.KeyError {
	if lock == nil {
		return nil
	}
	return &kvrpcpb.KeyError{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, key),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		},
	}
}
