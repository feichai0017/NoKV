package index

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestARTEncodeComparablePrefixToGroupBoundaries(t *testing.T) {
	for _, n := range []int{8, 16} {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(i + 1)
		}
		got := make([]byte, artComparableEncodedLen(len(src)))
		want := make([]byte, artComparableEncodedLen(len(src)))
		gotN := artEncodeComparablePrefixTo(got, src)
		wantN := artEncodeOrderedBytes(want, src)
		require.Equal(t, wantN, gotN)
		require.Equal(t, want[:wantN], got[:gotN])
	}
}

func TestARTGetLatest(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	versions := []uint64{3, 1, 2}
	values := [][]byte{[]byte("v3"), []byte("v1"), []byte("v2")}
	for i, ver := range versions {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("k"), ver, values[i], 0, 0)
		art.Add(entry)
		entry.DecrRef()
	}

	seekKey := kv.InternalKey(kv.CFDefault, []byte("k"), math.MaxUint64)
	_, vs := art.Search(seekKey)
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
		entry := kv.NewInternalEntry(kv.CFDefault, k, vers[i], []byte("v"), 0, 0)
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
		if last != nil && utils.CompareInternalKeys(last, entry.Key) > 0 {
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
	if entry == nil || !kv.SameBaseKey(seek, entry.Key) {
		t.Fatalf("seek mismatch: got %v", entry)
	}
}

func TestARTIteratorReverseIterationAndSeek(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	keys := []string{"a", "c", "e", "g", "i"}
	for _, k := range keys {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte(k), 1, []byte("v_"+k), 0, 0)
		art.Add(entry)
		entry.DecrRef()
	}

	userKey := func(entry *kv.Entry) string {
		_, k, _, ok := kv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		return string(k)
	}

	it := art.NewIterator(&Options{IsAsc: false})
	require.NotNil(t, it)
	defer func() { _ = it.Close() }()

	it.Rewind()
	require.True(t, it.Valid())
	require.Equal(t, "i", userKey(it.Item().Entry()))

	it.Next()
	require.True(t, it.Valid())
	require.Equal(t, "g", userKey(it.Item().Entry()))

	it.Next()
	require.True(t, it.Valid())
	require.Equal(t, "e", userKey(it.Item().Entry()))

	// Seek("f") should land on "e" for descending iteration.
	it.Seek(kv.InternalKey(kv.CFDefault, []byte("f"), 1))
	require.True(t, it.Valid())
	require.Equal(t, "e", userKey(it.Item().Entry()))

	// Seek("z") should land on the largest key <= "z": "i".
	it.Seek(kv.InternalKey(kv.CFDefault, []byte("z"), 1))
	require.True(t, it.Valid())
	require.Equal(t, "i", userKey(it.Item().Entry()))

	// Seek("0") should invalidate (no key <= "0").
	it.Seek(kv.InternalKey(kv.CFDefault, []byte("0"), 1))
	require.False(t, it.Valid())
}

func TestARTPrefixAdjacentInternalKeys(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()

	shortKey := []byte("ready-fail-key")
	longKey := []byte("ready-fail-key-lag")
	for _, tc := range []struct {
		cf    kv.ColumnFamily
		key   []byte
		value string
	}{
		{cf: kv.CFDefault, key: shortKey, value: "short-default"},
		{cf: kv.CFDefault, key: longKey, value: "long-default"},
		{cf: kv.CFWrite, key: shortKey, value: "short-write"},
		{cf: kv.CFWrite, key: longKey, value: "long-write"},
	} {
		entry := kv.NewInternalEntry(tc.cf, tc.key, 1, []byte(tc.value), 0, 0)
		art.Add(entry)
		entry.DecrRef()
	}

	it := art.NewIterator(nil)
	require.NotNil(t, it)
	defer func() { _ = it.Close() }()

	var last []byte
	for it.Rewind(); it.Valid(); it.Next() {
		entry := it.Item().Entry()
		require.NotNil(t, entry)
		if last != nil {
			require.LessOrEqual(t, utils.CompareInternalKeys(last, entry.Key), 0)
		}
		last = entry.Key
	}

	for _, cf := range []kv.ColumnFamily{kv.CFDefault, kv.CFWrite} {
		seek := kv.InternalKey(cf, shortKey, math.MaxUint64)

		foundKey, vs := art.Search(seek)
		require.NotEmpty(t, vs.Value)
		require.True(t, kv.SameBaseKey(seek, foundKey))

		it.Seek(seek)
		require.True(t, it.Valid())
		item := it.Item().Entry()
		require.NotNil(t, item)
		require.True(t, kv.SameBaseKey(seek, item.Key))
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
					entry := kv.NewInternalEntry(kv.CFDefault, k, vers[(worker+j)%len(vers)], []byte("v"), 0, 0)
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
		entry := kv.NewInternalEntry(kv.CFDefault, k, uint64(i+1), []byte("v"), 0, 0)
		art.Add(entry)
		entry.DecrRef()
	}

	if art.MemSize() == 0 {
		t.Fatalf("expected MemSize to be non-zero")
	}

	for _, k := range keys {
		seek := kv.InternalKey(kv.CFDefault, k, math.MaxUint64)
		_, vs := art.Search(seek)
		if len(vs.Value) == 0 {
			t.Fatalf("expected value for key %q", k)
		}
	}

	art48 := NewART(DefaultArenaSize)
	defer art48.DecrRef()

	for i := range 20 {
		k := []byte{byte(i + 1), 'x'}
		entry := kv.NewInternalEntry(kv.CFDefault, k, 1, []byte("v"), 0, 0)
		art48.Add(entry)
		entry.DecrRef()
	}

	root48 := art48.tree.root.Load()
	if root48 == nil || root48.kind != artNode48Kind {
		t.Fatalf("expected node48 root, got %v", root48)
	}
	payload48 := root48.payloadPtr(art48.tree.arena)
	eq, gt := lookupGEPayload(art48.tree.arena, root48.kind, payload48, 0)
	if eq != nil || gt == nil {
		t.Fatalf("expected greater child lookup in node48")
	}

	child, pos := lookupExactPosPayload(art48.tree.arena, root48.kind, payload48, 5)
	if child == nil || pos < 0 {
		t.Fatalf("expected child lookup in node48")
	}

	art256 := NewART(DefaultArenaSize)
	defer art256.DecrRef()

	for i := range 60 {
		k := []byte{byte(i), 'y'}
		entry := kv.NewInternalEntry(kv.CFDefault, k, 1, []byte("v"), 0, 0)
		art256.Add(entry)
		entry.DecrRef()
	}

	root256 := art256.tree.root.Load()
	if root256 == nil || root256.kind != artNode256Kind {
		t.Fatalf("expected node256 root, got %v", root256)
	}
	payload256 := root256.payloadPtr(art256.tree.arena)
	eq, gt = lookupGEPayload(art256.tree.arena, root256.kind, payload256, 10)
	if eq == nil || gt == nil {
		t.Fatalf("expected greater child lookup in node256")
	}

	child, pos = lookupExactPosPayload(art256.tree.arena, root256.kind, payload256, 10)
	if child == nil || pos < 0 {
		t.Fatalf("expected child lookup in node256")
	}
}

func TestARTDecrRefUnderflow(t *testing.T) {
	art := NewART(DefaultArenaSize)
	art.IncrRef() // ref = 2

	art.DecrRef() // ref = 1
	art.DecrRef() // ref = 0, normal release

	require.Panics(t, func() {
		art.DecrRef() // ref already 0, should panic
	})
}

func TestARTIteratorCloseIdempotent(t *testing.T) {
	art := NewART(DefaultArenaSize) // ref = 1
	it := art.NewIterator(nil)      // ref = 2
	require.NotNil(t, it)
	require.NoError(t, it.Close()) // ref = 1
	require.NoError(t, it.Close()) // still ref = 1
	require.Equal(t, int32(1), art.Load())
	art.DecrRef() // ref = 0
}

func TestARTIteratorOutOfRangeDoesNotResurrect(t *testing.T) {
	art := NewART(DefaultArenaSize)
	defer art.DecrRef()
	for _, k := range []string{"a", "b", "c"} {
		e := kv.NewInternalEntry(kv.CFDefault, []byte(k), 1, []byte("v_"+k), 0, 0)
		art.Add(e)
		e.DecrRef()
	}

	it := art.NewIterator(&Options{IsAsc: true})
	require.NotNil(t, it)
	defer func() { _ = it.Close() }()

	it.Seek(kv.InternalKey(kv.CFDefault, []byte("z"), 1))
	require.False(t, it.Valid())
	it.Next()
	require.False(t, it.Valid())

	rit := art.NewIterator(&Options{IsAsc: false})
	require.NotNil(t, rit)
	defer func() { _ = rit.Close() }()
	rit.Seek(kv.InternalKey(kv.CFDefault, []byte("0"), 1))
	require.False(t, rit.Valid())
	rit.Next()
	require.False(t, rit.Valid())
}
