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
	for i := 0; i < 50; i++ {
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

	for i := 0; i < 40; i++ {
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

	for i := 0; i < 10; i++ {
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