package utils

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := range len {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkiplist(1000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte("Val1"))
	list.Add(entry1)
	vs := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte("Val2"))
	list.Add(entry2)
	vs = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte(RandString(10))).Value)

	//Update a entry
	entry2_new := NewEntry(entry1.Key, []byte("Val1+1"))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkiplist(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := range maxTime {
		//number := rand.Intn(10000)
		key, val = RandString(10), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		list.Add(entry)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestDrawList(t *testing.T) {
	list := NewSkiplist(1000)
	n := 12
	for range n {
		index := strconv.Itoa(r.Intn(90)+10)
		key := index + RandString(8)
		entryRand := NewEntry([]byte(key), []byte(index))
		list.Add(entryRand)
	}
	list.Draw(true)
	fmt.Println(strings.Repeat("*", 30) + "分割线" + strings.Repeat("*", 30))
	list.Draw(false)
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkiplist(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return fmt.Appendf(nil, "Keykeykey%05d", i)
	}
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(t, key(i), v.Value)

		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkiplist(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return fmt.Appendf(nil, "keykeykey%05d", i)
	}
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Add(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(b, key(i), v.Value)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkiplist(100000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry1)
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry2)
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Update a entry
	entry2_new := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Add(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)

	iter := list.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s", iter.Item().Entry().Key, iter.Item().Entry().Value)
	}
}