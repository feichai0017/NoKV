package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

func TestRequestDecrRefUnderflowPanics(t *testing.T) {
	req := &Request{}
	require.Panics(t, func() {
		req.DecrRef()
	})
}

func TestRequestResetAndLoadEntries(t *testing.T) {
	oldEntry := kv.NewEntry([]byte("old"), []byte("value"))
	req := &Request{
		Entries:    []*kv.Entry{oldEntry},
		Ptrs:       []kv.ValuePtr{{Len: 1}},
		PtrIdxs:    []int{1},
		PtrBuckets: []uint32{7},
		Err:        errors.New("boom"),
		EnqueueAt:  time.Now(),
	}
	req.Init(3)
	req.WG.Add(1)

	newEntries := []*kv.Entry{
		kv.NewEntry([]byte("k1"), []byte("v1")),
		kv.NewEntry([]byte("k2"), []byte("v2")),
	}
	req.LoadEntries(newEntries)

	require.Len(t, req.Entries, 2)
	require.Same(t, newEntries[0], req.Entries[0])
	require.Equal(t, newEntries[0].Key, req.Entries[0].Key)

	req.Reset()
	require.Empty(t, req.Entries)
	require.Empty(t, req.Ptrs)
	require.Empty(t, req.PtrIdxs)
	require.Empty(t, req.PtrBuckets)
	require.NoError(t, req.Err)
	require.True(t, req.EnqueueAt.IsZero())
	require.EqualValues(t, 0, req.Load())

	for _, entry := range append([]*kv.Entry{oldEntry}, newEntries...) {
		entry.DecrRef()
	}
}

func TestRequestWaitReleasesEntries(t *testing.T) {
	entry := kv.NewEntry([]byte("k"), []byte("v"))
	req := &Request{
		Entries: []*kv.Entry{entry},
		Err:     errors.New("commit failed"),
	}
	req.IncrRef()
	req.WG.Add(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(10 * time.Millisecond)
		req.WG.Done()
	}()

	err := req.Wait()
	<-done

	require.EqualError(t, err, "commit failed")
	require.Nil(t, req.Entries)
	require.Nil(t, req.Ptrs)
	require.Nil(t, req.PtrIdxs)
	require.Nil(t, req.PtrBuckets)
	require.EqualValues(t, 0, entry.Load())
}
