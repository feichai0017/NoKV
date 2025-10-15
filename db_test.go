package NoKV

import (
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	// 写入
	for i := range 50 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

	for i := range 40 {
		key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		if err := db.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// 迭代器
	iter := db.NewIterator(&utils.Options{
		Prefix: []byte("hello"),
		IsAsc:  false,
	})
	defer func() { _ = iter.Close() }()
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}
	t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
	// 删除
	if err := db.Del([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	for i := range 10 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}
}

func TestRequestLoadEntriesCopiesSlice(t *testing.T) {
	req := requestPool.Get().(*request)
	req.reset()
	defer func() {
		req.Entries = nil
		req.Ptrs = nil
		requestPool.Put(req)
	}()

	e1 := &utils.Entry{Key: []byte("a")}
	e2 := &utils.Entry{Key: []byte("b")}
	src := []*utils.Entry{e1, e2}
	req.loadEntries(src)

	if len(req.Entries) != len(src) {
		t.Fatalf("expected %d entries, got %d", len(src), len(req.Entries))
	}
	if &req.Entries[0] == &src[0] {
		t.Fatalf("request reused caller backing array")
	}
	src[0] = &utils.Entry{Key: []byte("z")}
	if string(req.Entries[0].Key) != "a" {
		t.Fatalf("entry data mutated with caller slice")
	}
}