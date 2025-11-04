package NoKV

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/utils"
)

type iteratorContext struct {
	iters []utils.Iterator
}

type iteratorPool struct {
	pool  sync.Pool
	reuse uint64
}

func newIteratorPool() *iteratorPool {
	ip := &iteratorPool{}
	ip.pool.New = func() any { return nil }
	return ip
}

func (p *iteratorPool) get() *iteratorContext {
	if p == nil {
		return &iteratorContext{iters: make([]utils.Iterator, 0, 8)}
	}
	if v := p.pool.Get(); v != nil {
		if ctx, ok := v.(*iteratorContext); ok {
			atomic.AddUint64(&p.reuse, 1)
			ctx.reset()
			return ctx
		}
	}
	return &iteratorContext{iters: make([]utils.Iterator, 0, 8)}
}

func (p *iteratorPool) put(ctx *iteratorContext) {
	if p == nil || ctx == nil {
		return
	}
	ctx.reset()
	p.pool.Put(ctx)
}

func (p *iteratorPool) reused() uint64 {
	if p == nil {
		return 0
	}
	return atomic.LoadUint64(&p.reuse)
}

func (ctx *iteratorContext) reset() {
	if ctx == nil {
		return
	}
	ctx.iters = ctx.iters[:0]
}
