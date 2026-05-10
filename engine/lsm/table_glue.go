package lsm

import (
	"github.com/feichai0017/NoKV/engine/kv"
	cachepkg "github.com/feichai0017/NoKV/engine/lsm/cache"
	tablepkg "github.com/feichai0017/NoKV/engine/lsm/table"
)

// table is the engine-level alias for table.Table. Using the alias keeps
// existing lsm-package code (which deals with *table by name in dozens of
// places) compiling without a global rename, while the actual type lives
// in engine/lsm/table.
type table = tablepkg.Table

// Cache implements table.Runtime by exposing the levelManager's shared
// block + index cache. Returning nil disables caching for tables this
// runtime constructs.
func (lm *levelManager) Cache() *cachepkg.Cache {
	if lm == nil {
		return nil
	}
	return lm.cache
}

// Options implements table.Runtime by projecting the lsm-side Options struct
// onto the table-side subset. The translation is mechanical and stays in one
// place so the table package never needs to know about engine-level fields
// it does not consume.
func (lm *levelManager) Options() tablepkg.Options {
	if lm == nil || lm.opt == nil {
		return tablepkg.Options{}
	}
	o := lm.opt
	return tablepkg.Options{
		WorkDir:            o.WorkDir,
		FS:                 o.FS,
		SSTableMaxSize:     o.SSTableMaxSz,
		BlockSize:          int64(o.BlockSize),
		BloomFalsePositive: o.BloomFalsePositive,
		BlockCompression:   o.BlockCompression,
		PrefixExtractor:    func(b []byte) []byte { return o.PrefixExtractor(b) },
		ManifestSync:       o.ManifestSync,
	}
}

// tableOptionsFor builds the table.Options projection from a freestanding
// engine Options pointer. Used by builders that want to clone the runtime
// options for a one-off compaction without going through a levelManager.
func tableOptionsFor(o *Options) tablepkg.Options {
	if o == nil {
		return tablepkg.Options{}
	}
	out := tablepkg.Options{
		WorkDir:            o.WorkDir,
		FS:                 o.FS,
		SSTableMaxSize:     o.SSTableMaxSz,
		BlockSize:          int64(o.BlockSize),
		BloomFalsePositive: o.BloomFalsePositive,
		BlockCompression:   o.BlockCompression,
		ManifestSync:       o.ManifestSync,
	}
	if o.PrefixExtractor != nil {
		out.PrefixExtractor = func(b []byte) []byte { return o.PrefixExtractor(b) }
	}
	return out
}

// entryValueLen mirrors the table-package helper and is used by lsm code that
// wants the inline-value byte count without going through the AddKey wrapper.
func entryValueLen(e *kv.Entry) uint32 {
	if e == nil {
		return 0
	}
	return uint32(len(e.Value))
}
