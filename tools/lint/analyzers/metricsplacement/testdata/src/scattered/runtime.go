package scattered

import "sync/atomic"

type Runtime struct{ counter atomic.Uint64 }

func (r *Runtime) recordAppend() { // want `code_contract §9: recordAppend.*runtime.go.*`
	r.counter.Add(1)
}

// recordHealth is named recordX but only updates plain state (no atomic op),
// so the refined rule lets it pass — naming drift outside the metric remit.
func (r *Runtime) recordHealth(msg string) { _ = msg }

// recordLowercase is not a recordX (uppercase letter check fails); should not trip.
func recordlower() {}
