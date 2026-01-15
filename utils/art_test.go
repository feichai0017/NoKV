package utils

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
)

func TestARTGetLatest(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	versions := []uint64{3, 1, 2}
	values := [][]byte{[]byte("v3"), []byte("v1"), []byte("v2")}
	for i, ver := range versions {
		entry := kv.NewEntryWithCF(kv.CFDefault, []byte("k"), values[i])
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, ver)
		art.Add(entry)
		entry.DecrRef()
	}

	seekKey := kv.InternalKey(kv.CFDefault, []byte("k"), math.MaxUint64)
	vs := art.Search(seekKey)
	if string(vs.Value) != "v3" {
		t.Fatalf("expected latest value v3, got %q", string(vs.Value))
	}
}

func TestARTIteratorOrder(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	keys := [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("a")}
	vers := []uint64{2, 3, 1, 1}
	for i, k := range keys {
		entry := kv.NewEntryWithCF(kv.CFDefault, k, []byte("v"))
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, vers[i])
		art.Add(entry)
		entry.DecrRef()
	}

	it := art.NewIterator(nil)
	if it == nil {
		t.Fatalf("expected iterator")
	}
	defer it.Close()

	it.Rewind()
	var last []byte
	for ; it.Valid(); it.Next() {
		entry := it.Item().Entry()
		if entry == nil {
			t.Fatalf("nil entry")
		}
		if last != nil && CompareKeys(last, entry.Key) > 0 {
			t.Fatalf("iterator out of order: %q before %q", last, entry.Key)
		}
		last = entry.Key
	}

	seek := kv.InternalKey(kv.CFDefault, []byte("b"), math.MaxUint64)
	it.Seek(seek)
	if !it.Valid() {
		t.Fatalf("expected seek to be valid")
	}
	entry := it.Item().Entry()
	if entry == nil || !kv.SameKey(seek, entry.Key) {
		t.Fatalf("seek mismatch: got %v", entry)
	}
}

func TestARTConcurrentWriteIterate(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	var (
		stop  int32
		wg    sync.WaitGroup
		keys  = [][]byte{[]byte("k0"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}
		vers  = []uint64{1, 2, 3, 4, 5}
		errCh = make(chan error, 1)
	)

	report := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
			atomic.StoreInt32(&stop, 1)
		default:
		}
	}

	// Writers: continuously update a small keyset with different versions.
	for i := range 4 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for atomic.LoadInt32(&stop) == 0 {
				for j, k := range keys {
					entry := kv.NewEntryWithCF(kv.CFDefault, k, []byte("v"))
					entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, vers[(worker+j)%len(vers)])
					art.Add(entry)
					entry.DecrRef()
				}
			}
		}(i)
	}

	// Reader: iterate and validate ordering under concurrent writes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			it := art.NewIterator(nil)
			it.Rewind()
			for ; it.Valid(); it.Next() {
				item := it.Item()
				if item == nil {
					report(errors.New("nil entry during iteration"))
					break
				}
				entry := item.Entry()
				if entry == nil {
					report(errors.New("nil entry during iteration"))
					break
				}
				if len(entry.Key) == 0 {
					report(errors.New("empty key during iteration"))
					break
				}
			}
			_ = it.Close()
			runtime.Gosched()
		}
	}()

	time.Sleep(250 * time.Millisecond)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("%v", err)
	default:
	}
}
