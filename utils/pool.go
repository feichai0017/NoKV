package utils

import (
	"expvar"

	"github.com/panjf2000/ants/v2"
)

// Pool wraps ants.Pool with lightweight metrics.
type Pool struct {
	p           *ants.Pool
	submitTotal *expvar.Int
	active      *expvar.Int
	size        int
}

// NewPool creates a pool with the given size. If size<=0, defaults to 1.
func NewPool(size int, name string) *Pool {
	if size <= 0 {
		size = 1
	}
	p, _ := ants.NewPool(size, ants.WithPreAlloc(true))
	return &Pool{
		p:           p,
		submitTotal: getOrCreateInt("NoKV.Pool." + name + ".Submit"),
		active:      getOrCreateInt("NoKV.Pool." + name + ".Active"),
		size:        size,
	}
}

// Submit runs fn in the pool.
func (pl *Pool) Submit(fn func()) error {
	if pl == nil || pl.p == nil || fn == nil {
		return nil
	}
	pl.submitTotal.Add(1)
	pl.active.Add(1)
	return pl.p.Submit(func() {
		defer pl.active.Add(-1)
		fn()
	})
}

// Release frees resources.
func (pl *Pool) Release() {
	if pl == nil || pl.p == nil {
		return
	}
	pl.p.Release()
}

// Size reports configured worker count.
func (pl *Pool) Size() int { return pl.size }

func getOrCreateInt(name string) *expvar.Int {
	if v := expvar.Get(name); v != nil {
		if iv, ok := v.(*expvar.Int); ok {
			return iv
		}
	}
	return expvar.NewInt(name)
}

// GetOrCreateInt is exported for reuse.
func GetOrCreateInt(name string) *expvar.Int {
	return getOrCreateInt(name)
}
