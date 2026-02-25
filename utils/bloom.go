package utils

import "math"

const (
	bloomSeed = 0xbc9f1d34
	bloomM    = 0xc6a4a793
)

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	return BloomMayContain(f, h)
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(buildBloomFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	h := uint32(bloomSeed) ^ uint32(len(b))*bloomM
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= bloomM
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= bloomM
		h ^= h >> 24
	}
	return h
}

// BloomKForBitsPerKey maps bits-per-key to the number of bloom probes.
func BloomKForBitsPerKey(bitsPerKey int) uint8 {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := min(max(uint32(float64(bitsPerKey)*0.69), 1), 30)
	return uint8(k)
}

// BloomMayContain checks if hash may exist in the encoded bloom filter.
func BloomMayContain(filter []byte, h uint32) bool {
	if len(filter) < 2 {
		return false
	}
	k := filter[len(filter)-1]
	if k > 30 {
		return true
	}
	nBits := uint32(8 * (len(filter) - 1))
	delta := h>>17 | h<<15
	for range k {
		bitPos := h % nBits
		if filter[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// BloomInsert mutates filter to include hash.
func BloomInsert(filter []byte, h uint32) {
	if len(filter) < 2 {
		return
	}
	k := filter[len(filter)-1]
	if k > 30 {
		return
	}
	nBits := uint32(8 * (len(filter) - 1))
	delta := h>>17 | h<<15
	for range k {
		bitPos := h % nBits
		filter[bitPos/8] |= 1 << (bitPos % 8)
		h += delta
	}
}

func buildBloomFilter(keys []uint32, bitsPerKey int) []byte {
	k := BloomKForBitsPerKey(bitsPerKey)

	nBits := max(len(keys)*bitsPerKey, 64)
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	filter := make([]byte, nBytes+1)
	for _, h := range keys {
		delta := h>>17 | h<<15
		for range k {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}
	filter[nBytes] = k
	return filter
}
