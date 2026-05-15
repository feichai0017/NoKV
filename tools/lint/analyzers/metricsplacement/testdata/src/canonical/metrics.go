package canonical

import "sync/atomic"

type Runtime struct{ counter atomic.Uint64 }

func (r *Runtime) recordAppend() { r.counter.Add(1) }
