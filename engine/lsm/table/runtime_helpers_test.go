package table

import (
	cachepkg "github.com/feichai0017/NoKV/engine/lsm/cache"
)

// testRuntime is a minimal Runtime implementation for table-package tests.
// It owns the Options snapshot and (optionally) a Cache.
type testRuntime struct {
	opts  Options
	cache *cachepkg.Cache
}

func newTestRuntime(opts Options) *testRuntime {
	c := cachepkg.New(cachepkg.Options{
		IndexBytes: 1 << 20,
		BlockBytes: 1 << 20,
	})
	return &testRuntime{opts: opts, cache: c}
}

func (r *testRuntime) Cache() *cachepkg.Cache { return r.cache }
func (r *testRuntime) Options() Options       { return r.opts }
