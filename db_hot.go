package NoKV

import (
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.hotWrite == nil || len(key) == 0 {
		return nil
	}
	limit := db.opt.WriteHotKeyLimit
	if limit <= 0 {
		return nil
	}
	skey := cfHotKey(cf, key)
	if skey == "" {
		return nil
	}
	_, limited := db.hotWrite.TouchAndClamp(skey, limit)
	if !limited {
		return nil
	}
	db.hotWriteLimited.Add(1)
	return utils.ErrHotKeyWriteThrottle
}

func cfHotKey(cf kv.ColumnFamily, key []byte) string {
	if !cf.Valid() || cf == kv.CFDefault {
		return string(key)
	}
	buf := make([]byte, len(key)+1)
	buf[0] = byte(cf)
	copy(buf[1:], key)
	return string(buf)
}
