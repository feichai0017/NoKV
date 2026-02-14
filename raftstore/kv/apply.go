package kv

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/utils"
)

var defaultLatches = latch.NewManager(512)

// Apply executes a RaftCmdRequest against the provided DB. The returned
// response mirrors the request ordering. Only MVCC prewrite/commit operations
// are supported at the moment.
func Apply(db *NoKV.DB, req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("kv: nil raft command")
	}
	resp := &pb.RaftCmdResponse{Header: req.Header}
	for _, r := range req.Requests {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case pb.CmdType_CMD_GET:
			result, keyErr, err := handleGet(db, r.GetGet())
			if err != nil {
				return nil, err
			}
			if keyErr != nil {
				result.Error = keyErr
			}
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Get{Get: result}})
		case pb.CmdType_CMD_PREWRITE:
			result := &pb.PrewriteResponse{Errors: percolator.Prewrite(db, defaultLatches, r.GetPrewrite())}
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Prewrite{Prewrite: result}})
		case pb.CmdType_CMD_COMMIT:
			err := percolator.Commit(db, defaultLatches, r.GetCommit())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Commit{Commit: &pb.CommitResponse{Error: err}}})
		case pb.CmdType_CMD_BATCH_ROLLBACK:
			err := percolator.BatchRollback(db, defaultLatches, r.GetBatchRollback())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_BatchRollback{BatchRollback: &pb.BatchRollbackResponse{Error: err}}})
		case pb.CmdType_CMD_RESOLVE_LOCK:
			count, err := percolator.ResolveLock(db, defaultLatches, r.GetResolveLock())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_ResolveLock{ResolveLock: &pb.ResolveLockResponse{ResolvedLocks: count, Error: err}}})
		case pb.CmdType_CMD_CHECK_TXN_STATUS:
			result := percolator.CheckTxnStatus(db, defaultLatches, r.GetCheckTxnStatus())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_CheckTxnStatus{CheckTxnStatus: result}})
		case pb.CmdType_CMD_SCAN:
			result, err := handleScan(db, r.GetScan())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Scan{Scan: result}})
		default:
			return nil, fmt.Errorf("kv: unsupported command %v", r.GetCmdType())
		}
	}
	return resp, nil
}

// NewApplier wraps Apply into a reusable function suitable for store command
// execution wiring.
func NewApplier(db *NoKV.DB) func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	return func(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
		return Apply(db, req)
	}
}

func handleGet(db *NoKV.DB, req *pb.GetRequest) (*pb.GetResponse, *pb.KeyError, error) {
	if req == nil {
		return &pb.GetResponse{NotFound: true}, nil, nil
	}
	reader := percolator.NewReader(db)
	lock, err := reader.GetLock(req.GetKey())
	if err != nil {
		return nil, nil, err
	}
	if lock != nil && req.GetVersion() >= lock.Ts {
		keyErr := &pb.KeyError{Locked: &pb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, req.GetKey()),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		}}
		return &pb.GetResponse{}, keyErr, nil
	}
	val, err := reader.GetValue(req.GetKey(), req.GetVersion())
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return &pb.GetResponse{NotFound: true}, nil, nil
		}
		return nil, nil, err
	}
	return &pb.GetResponse{Value: val}, nil, nil
}

func handleScan(db *NoKV.DB, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if req == nil {
		return &pb.ScanResponse{}, nil
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
		readTs = math.MaxUint64
	}
	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer iter.Close()

	startKey := append([]byte(nil), req.GetStartKey()...)
	includeStart := req.GetIncludeStart()
	started := len(startKey) == 0

	resp := &pb.ScanResponse{}
	iter.Rewind()
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
		if entry.CF != kv.CFWrite {
			iter.Next()
			continue
		}
		key := kv.SafeCopy(nil, entry.Key)
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
		value, found, err := collectVisibleValue(db, iter, key, readTs)
		if err != nil {
			return nil, err
		}
		if found {
			resp.Kvs = append(resp.Kvs, &pb.KV{
				Key:     key,
				Value:   value,
				Version: readTs,
			})
		}
	}
	return resp, nil
}

func advanceToNextUserKey(iter utils.Iterator, current []byte) {
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
		if !bytes.Equal(entry.Key, current) {
			return
		}
	}
}

func collectVisibleValue(db *NoKV.DB, iter utils.Iterator, key []byte, readTs uint64) ([]byte, bool, error) {
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
		if !bytes.Equal(entry.Key, key) {
			return nil, false, nil
		}
		if entry.Version > readTs {
			iter.Next()
			continue
		}
		write, err := percolator.DecodeWrite(entry.Value)
		if err != nil {
			return nil, false, err
		}
		switch write.Kind {
		case pb.Mutation_Delete, pb.Mutation_Rollback:
			advanceToNextUserKey(iter, key)
			return nil, false, nil
		default:
			var value []byte
			if len(write.ShortValue) > 0 {
				value = kv.SafeCopy(nil, write.ShortValue)
			} else {
				entryVal, err := db.GetVersionedEntry(kv.CFDefault, key, write.StartTs)
				if err != nil {
					if errors.Is(err, utils.ErrKeyNotFound) {
						iter.Next()
						continue
					}
					return nil, false, err
				}
				value = kv.SafeCopy(nil, entryVal.Value)
			}
			advanceToNextUserKey(iter, key)
			return value, true, nil
		}
	}
	return nil, false, nil
}

func lockedError(key []byte, lock *percolator.Lock) *pb.KeyError {
	if lock == nil {
		return nil
	}
	return &pb.KeyError{
		Locked: &pb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, key),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		},
	}
}
