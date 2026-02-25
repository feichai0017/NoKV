package cache

import "github.com/feichai0017/NoKV/utils"

// Filter is an encoded set of []byte keys.
type Filter []byte

type BloomFilter struct {
	bitmap Filter
	k      uint8
}

// MayContainKey _
func (f *BloomFilter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f *BloomFilter) MayContain(h uint32) bool {
	return utils.BloomMayContain(f.bitmap, h)
}

func (f *BloomFilter) Len() int32 {
	return int32(len(f.bitmap))
}

func (f *BloomFilter) InsertKey(k []byte) bool {
	return f.Insert(Hash(k))
}

func (f *BloomFilter) Insert(h uint32) bool {
	utils.BloomInsert(f.bitmap, h)
	return true
}

func (f *BloomFilter) AllowKey(k []byte) bool {
	if f == nil {
		return true
	}
	already := f.MayContainKey(k)
	if !already {
		f.InsertKey(k)
	}
	return already
}

func (f *BloomFilter) Allow(h uint32) bool {
	if f == nil {
		return true
	}
	already := f.MayContain(h)
	if !already {
		f.Insert(h)
	}
	return already
}

func (f *BloomFilter) reset() {
	if f == nil {
		return
	}
	for i := range f.bitmap {
		f.bitmap[i] = 0
	}
	if len(f.bitmap) > 0 {
		f.bitmap[len(f.bitmap)-1] = f.k
	}
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func newFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitsPerKey := bloomBitsPerKey(numEntries, falsePositive)
	return initFilter(numEntries, bitsPerKey)
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func bloomBitsPerKey(numEntries int, fp float64) int {
	return utils.BloomBitsPerKey(numEntries, fp)
}

func initFilter(numEntries int, bitsPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	bf.k = utils.BloomKForBitsPerKey(bitsPerKey)

	nBits := max(numEntries*int(bitsPerKey), 64)
	nBytes := (nBits + 7) / 8
	bf.bitmap = make([]byte, nBytes+1)
	bf.bitmap[nBytes] = bf.k
	return bf
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	return utils.Hash(b)
}
