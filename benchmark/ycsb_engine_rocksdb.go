//go:build cgo && benchmark_rocksdb

package benchmark

/*
#cgo LDFLAGS: -lrocksdb -lz -lbz2 -lsnappy -lzstd -llz4
#include <stdlib.h>
#include "rocksdb/c.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"unsafe"
)

func newRocksDBEngine(opts ycsbEngineOptions) ycsbEngine {
	return &rocksdbEngine{opts: opts}
}

type rocksdbEngine struct {
	opts         ycsbEngineOptions
	db           *C.rocksdb_transactiondb_t
	options      *C.rocksdb_options_t
	txnDBOptions *C.rocksdb_transactiondb_options_t
	readOpts     *C.rocksdb_readoptions_t
	writeOpts    *C.rocksdb_writeoptions_t
	txnOptions   *C.rocksdb_transaction_options_t
	cache        *C.rocksdb_cache_t
}

func (e *rocksdbEngine) Name() string { return "RocksDB" }

func (e *rocksdbEngine) Open(clean bool) error {
	dir := e.opts.engineDir("rocksdb")
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return fmt.Errorf("rocksdb: ensure dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("rocksdb: mkdir: %w", err)
	}

	opts := C.rocksdb_options_create()
	e.options = opts
	C.rocksdb_options_set_create_if_missing(opts, 1)
	if mb := e.opts.MemtableMB; mb > 0 {
		C.rocksdb_options_set_write_buffer_size(opts, C.size_t(mb)<<20)
	}
	if conc := e.opts.BlockCacheMB; conc > 0 {
		e.cache = C.rocksdb_cache_create_lru(C.size_t(conc) << 20)
		bopts := C.rocksdb_block_based_options_create()
		C.rocksdb_block_based_options_set_block_cache(bopts, e.cache)
		C.rocksdb_options_set_block_based_table_factory(opts, bopts)
		C.rocksdb_block_based_options_destroy(bopts)
	}

	switch e.opts.RocksDBCompression {
	case "snappy":
		C.rocksdb_options_set_compression(opts, C.rocksdb_snappy_compression)
	case "zstd":
		C.rocksdb_options_set_compression(opts, C.rocksdb_zstd_compression)
	default:
		C.rocksdb_options_set_compression(opts, C.rocksdb_no_compression)
	}
	if mb := e.opts.SSTableMB; mb > 0 {
		C.rocksdb_options_set_target_file_size_base(opts, C.uint64_t(mb)<<20)
		C.rocksdb_options_set_target_file_size_multiplier(opts, 1)
	}

	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	var errStr *C.char
	txnOpts := C.rocksdb_transactiondb_options_create()
	e.txnDBOptions = txnOpts
	db := C.rocksdb_transactiondb_open(opts, txnOpts, cDir, &errStr)
	if errStr != nil {
		defer C.free(unsafe.Pointer(errStr))
		return errors.New(C.GoString(errStr))
	}
	e.db = db

	e.readOpts = C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_fill_cache(e.readOpts, 1)
	e.writeOpts = C.rocksdb_writeoptions_create()
	if e.opts.SyncWrites {
		C.rocksdb_writeoptions_set_sync(e.writeOpts, 1)
	}
	e.txnOptions = C.rocksdb_transaction_options_create()
	return nil
}

func (e *rocksdbEngine) Close() error {
	if e.db != nil {
		C.rocksdb_close(e.db)
		e.db = nil
	}
	if e.readOpts != nil {
		C.rocksdb_readoptions_destroy(e.readOpts)
		e.readOpts = nil
	}
	if e.writeOpts != nil {
		C.rocksdb_writeoptions_destroy(e.writeOpts)
		e.writeOpts = nil
	}
	if e.txnOptions != nil {
		C.rocksdb_transaction_options_destroy(e.txnOptions)
		e.txnOptions = nil
	}
	if e.options != nil {
		C.rocksdb_options_destroy(e.options)
		e.options = nil
	}
	if e.txnDBOptions != nil {
		C.rocksdb_transactiondb_options_destroy(e.txnDBOptions)
		e.txnDBOptions = nil
	}
	if e.cache != nil {
		C.rocksdb_cache_destroy(e.cache)
		e.cache = nil
	}
	return nil
}

func (e *rocksdbEngine) Read(key []byte, dst []byte) ([]byte, error) {
	if e.db == nil {
		return nil, fmt.Errorf("rocksdb not open")
	}
	return e.withTxnRead(func(txn *C.rocksdb_transaction_t) ([]byte, error) {
		var valLen C.size_t
		var errStr *C.char
		val := C.rocksdb_transaction_get(
			txn,
			e.readOpts,
			bytesPtr(key),
			C.size_t(len(key)),
			&valLen,
			&errStr,
		)
		if errStr != nil {
			defer C.free(unsafe.Pointer(errStr))
			return nil, errors.New(C.GoString(errStr))
		}
		if val != nil && valLen > 0 {
			goBytes := C.GoBytes(unsafe.Pointer(val), C.int(valLen))
			if cap(dst) < len(goBytes) {
				dst = make([]byte, len(goBytes))
			}
			dst = dst[:len(goBytes)]
			copy(dst, goBytes)
			C.rocksdb_free(unsafe.Pointer(val))
			return dst, nil
		}
		if val != nil {
			C.rocksdb_free(unsafe.Pointer(val))
		}
		return dst[:0], nil
	})
}

func (e *rocksdbEngine) Insert(key, value []byte) error {
	return e.putTxn(key, value)
}

func (e *rocksdbEngine) Update(key, value []byte) error {
	return e.putTxn(key, value)
}

func (e *rocksdbEngine) putTxn(key, value []byte) error {
	_, err := e.withTxn(func(txn *C.rocksdb_transaction_t) (int, []byte, error) {
		var errStr *C.char
		C.rocksdb_transaction_put(
			txn,
			bytesPtr(key),
			C.size_t(len(key)),
			bytesPtr(value),
			C.size_t(len(value)),
			&errStr,
		)
		if errStr != nil {
			defer C.free(unsafe.Pointer(errStr))
			return 0, nil, errors.New(C.GoString(errStr))
		}
		return 0, nil, nil
	})
	return err
}

func (e *rocksdbEngine) Scan(startKey []byte, count int) (int, error) {
	return e.withTxn(func(txn *C.rocksdb_transaction_t) (int, error) {
		it := C.rocksdb_transaction_create_iterator(txn, e.readOpts)
		defer C.rocksdb_iter_destroy(it)
		if len(startKey) > 0 {
			C.rocksdb_iter_seek(it, bytesPtr(startKey), C.size_t(len(startKey)))
		} else {
			C.rocksdb_iter_seek_to_first(it)
		}
		read := 0
		for read < count && C.rocksdb_iter_valid(it) != 0 {
			var vLen C.size_t
			val := C.rocksdb_iter_value(it, &vLen)
			if val != nil {
				C.rocksdb_free(unsafe.Pointer(val))
			}
			read++
			C.rocksdb_iter_next(it)
		}
		return read, nil
	})
}

func bytesPtr(b []byte) *C.char {
	if len(b) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}

func (e *rocksdbEngine) withTxnRead(fn func(txn *C.rocksdb_transaction_t) ([]byte, error)) ([]byte, error) {
	_, out, err := e.withTxn(func(txn *C.rocksdb_transaction_t) (int, []byte, error) {
		val, err := fn(txn)
		return 0, val, err
	})
	return out, err
}

func (e *rocksdbEngine) withTxn(fn func(txn *C.rocksdb_transaction_t) (int, []byte, error)) (int, []byte, error) {
	txn := C.rocksdb_transaction_begin(e.db, e.writeOpts, e.txnOptions, nil)
	defer C.rocksdb_transaction_destroy(txn)
	count, val, err := fn(txn)
	if err != nil {
		return count, val, err
	}
	var errStr *C.char
	C.rocksdb_transaction_commit(txn, &errStr)
	if errStr != nil {
		defer C.free(unsafe.Pointer(errStr))
		return count, val, errors.New(C.GoString(errStr))
	}
	return count, val, nil
}
