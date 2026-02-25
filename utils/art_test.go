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
	"github.com/stretchr/testify/require"
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
	defer func() { _ = it.Close() }()

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
	wg.Go(func() {
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
	})

	time.Sleep(250 * time.Millisecond)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("%v", err)
	default:
	}
}

func TestARTPrefixMismatchAndNodeKinds(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	keys := [][]byte{[]byte("aa"), []byte("ab"), []byte("ba")}
	for i, k := range keys {
		entry := kv.NewEntryWithCF(kv.CFDefault, k, []byte("v"))
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, uint64(i+1))
		art.Add(entry)
		entry.DecrRef()
	}

	if art.MemSize() == 0 {
		t.Fatalf("expected MemSize to be non-zero")
	}

	for _, k := range keys {
		seek := kv.InternalKey(kv.CFDefault, k, math.MaxUint64)
		vs := art.Search(seek)
		if len(vs.Value) == 0 {
			t.Fatalf("expected value for key %q", k)
		}
	}

	art48 := NewART(DefaultArenaSize)
	defer art48.DecrRef()

	for i := range 20 {
		k := []byte{byte(i + 1), 'x'}
		entry := kv.NewEntryWithCF(kv.CFDefault, k, []byte("v"))
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, 1)
		art48.Add(entry)
		entry.DecrRef()
	}

	root48 := art48.tree.root.Load()
	if root48 == nil || root48.kind != artNode48Kind {
		t.Fatalf("expected node48 root, got %v", root48)
	}
	eq, gt := root48.findChild(art48.tree.arena, 0)
	if eq != nil || gt == nil {
		t.Fatalf("expected greater child lookup in node48")
	}

	it48 := art48.NewIterator(nil)
	artIt48, ok := it48.(*artIterator)
	if !ok {
		t.Fatalf("expected art iterator, got %T", it48)
	}
	child, nextIdx := artIt48.childForKey(root48, 5)
	if child == nil || nextIdx == 0 {
		t.Fatalf("expected child lookup in node48")
	}
	_ = artIt48.Close()

	art256 := NewART(DefaultArenaSize)
	defer art256.DecrRef()

	for i := range 60 {
		k := []byte{byte(i), 'y'}
		entry := kv.NewEntryWithCF(kv.CFDefault, k, []byte("v"))
		entry.Key = kv.InternalKey(kv.CFDefault, entry.Key, 1)
		art256.Add(entry)
		entry.DecrRef()
	}

	root256 := art256.tree.root.Load()
	if root256 == nil || root256.kind != artNode256Kind {
		t.Fatalf("expected node256 root, got %v", root256)
	}
	eq, gt = root256.findChild(art256.tree.arena, 10)
	if eq == nil || gt == nil {
		t.Fatalf("expected greater child lookup in node256")
	}

	it256 := art256.NewIterator(nil)
	artIt256, ok := it256.(*artIterator)
	if !ok {
		t.Fatalf("expected art iterator, got %T", it256)
	}
	child, nextIdx = artIt256.childForKey(root256, 10)
	if child == nil || nextIdx == 0 {
		t.Fatalf("expected child lookup in node256")
	}
	_ = artIt256.Close()
}

func TestARTDecrRefUnderflow(t *testing.T) {
	art := NewART(DefaultArenaSize)
	art.IncrRef() // ref = 2

	art.DecrRef() // ref = 1
	art.DecrRef() // ref = 0, normal release

	require.PanicsWithValue(t, "ART.DecrRef: refcount underflow (double release)", func() {
		art.DecrRef() // ref = -1, should panic
	})
}

func TestARTIteratorCloseIdempotent(t *testing.T) {
	art := NewART(DefaultArenaSize) // ref = 1
	it := art.NewIterator(nil)      // ref = 2
	require.NotNil(t, it)
	require.NoError(t, it.Close()) // ref = 1
	require.NoError(t, it.Close()) // still ref = 1
	require.Equal(t, int32(1), art.ref.Load())
	art.DecrRef() // ref = 0
}
