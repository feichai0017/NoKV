package mvcc

import (
	"bytes"
	"math"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
)

const lockColumnTs = math.MaxUint64

// Reader provides helper methods to inspect MVCC state within a DB instance.
type Reader struct {
	db *NoKV.DB
}

// NewReader constructs a Reader.
func NewReader(db *NoKV.DB) *Reader {
	return &Reader{db: db}
}

// GetLock returns the lock stored for the provided key, if any.
func (r *Reader) GetLock(key []byte) (*Lock, error) {
	entry, err := r.db.GetVersionedEntry(utils.CFLock, key, lockColumnTs)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer entry.DecrRef()
	if entry.Meta&utils.BitDelete > 0 || entry.Value == nil {
		return nil, nil
	}
	lock, err := DecodeLock(entry.Value)
	if err != nil {
		return nil, err
	}
	return &lock, nil
}

// MostRecentWrite returns the latest committed write for the specified key.
func (r *Reader) MostRecentWrite(key []byte) (*Write, uint64, error) {
	var result *Write
	var commitTs uint64
	if err := r.scanWrites(key, func(w Write, ts uint64) bool {
		if result == nil || ts > commitTs {
			copy := w
			result = &copy
			commitTs = ts
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if result == nil {
		return nil, 0, nil
	}
	return result, commitTs, nil
}

// GetWriteByStartTs returns the commit record for the provided key/startTs pair.
func (r *Reader) GetWriteByStartTs(key []byte, startTs uint64) (*Write, uint64, error) {
	var result *Write
	var commitTs uint64
	if err := r.scanWrites(key, func(w Write, ts uint64) bool {
		if w.StartTs == startTs {
			copy := w
			result = &copy
			commitTs = ts
			return false
		}
		if ts < startTs {
			return false
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if result == nil {
		return nil, 0, nil
	}
	return result, commitTs, nil
}

// GetValue reads the value visible at the provided read timestamp.
func (r *Reader) GetValue(key []byte, readTs uint64) ([]byte, error) {
	write, _, err := r.getWriteForRead(key, readTs)
	if err != nil {
		return nil, err
	}
	if write == nil {
		return nil, utils.ErrKeyNotFound
	}
	if write.Kind == pb.Mutation_Delete || write.Kind == pb.Mutation_Rollback {
		return nil, utils.ErrKeyNotFound
	}
	entry, err := r.db.GetVersionedEntry(utils.CFDefault, key, write.StartTs)
	if err != nil {
		return nil, err
	}
	defer entry.DecrRef()
	if entry.Meta&utils.BitDelete > 0 || entry.Value == nil {
		return nil, utils.ErrKeyNotFound
	}
	return utils.SafeCopy(nil, entry.Value), nil
}

func (r *Reader) getWriteForRead(key []byte, readTs uint64) (*Write, uint64, error) {
	var result *Write
	var commitTs uint64
	if err := r.scanWrites(key, func(w Write, ts uint64) bool {
		if ts <= readTs && (result == nil || ts > commitTs) {
			copy := w
			result = &copy
			commitTs = ts
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if result == nil {
		return nil, 0, nil
	}
	return result, commitTs, nil
}

func (r *Reader) scanWrites(key []byte, fn func(Write, uint64) bool) error {
	iter := r.db.NewIterator(&utils.Options{IsAsc: true})
	defer iter.Close()
	iter.Rewind()
	for iter.Valid() {
		item := iter.Item()
		if item == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		if entry.CF != utils.CFWrite {
			if bytes.Compare(entry.Key, key) > 0 {
				break
			}
			iter.Next()
			continue
		}
		cmp := bytes.Compare(entry.Key, key)
		if cmp > 0 {
			break
		}
		if cmp < 0 {
			iter.Next()
			continue
		}
		if entry.Meta&utils.BitDelete > 0 {
			iter.Next()
			continue
		}
		write, err := DecodeWrite(entry.Value)
		if err != nil {
			return err
		}
		if !fn(write, entry.Version) {
			break
		}
		iter.Next()
	}
	return nil
}
