package sstable

import (
	"fmt"
	"math"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
)

const maxUint32 = uint64(math.MaxUint32)

// MustBlockCacheKey returns a composite key (fid<<32 | blockIdx) used for block cache lookup.
// It panics when fid/blockIdx exceed 32-bit boundaries.
func MustBlockCacheKey(fid uint64, blockIdx int) uint64 {
	utils.CondPanicFunc(fid > maxUint32, func() error { return fmt.Errorf("table fid %d exceeds 32-bit limit", fid) })
	utils.CondPanicFunc(blockIdx < 0 || uint64(blockIdx) > maxUint32, func() error {
		return fmt.Errorf("invalid block index %d", blockIdx)
	})
	return (fid << 32) | uint64(uint32(blockIdx))
}

// KeyCount returns indexed key count, or 0 when index is nil.
func KeyCount(index *pb.TableIndex) uint32 {
	if index == nil {
		return 0
	}
	return index.GetKeyCount()
}

// MaxVersion returns maximum MVCC version in index, or 0 when index is nil.
func MaxVersion(index *pb.TableIndex) uint64 {
	if index == nil {
		return 0
	}
	return index.GetMaxVersion()
}

// HasBloomFilter reports whether table index contains a bloom filter payload.
func HasBloomFilter(index *pb.TableIndex) bool {
	return index != nil && len(index.GetBloomFilter()) > 0
}

// BlockOffset returns the i-th block offset.
// i==len(offsets) returns (nil, true) as a sentinel "past last block".
func BlockOffset(index *pb.TableIndex, i int) (*pb.BlockOffset, bool) {
	if index == nil {
		return nil, false
	}
	offsets := index.GetOffsets()
	if i < 0 || i > len(offsets) {
		return nil, false
	}
	if i == len(offsets) {
		return nil, true
	}
	return offsets[i], true
}

// SearchFirstBlockWithBaseKeyGT returns the first block index whose base key is > key.
// If none exists, returns len(offsets).
func SearchFirstBlockWithBaseKeyGT(offsets []*pb.BlockOffset, key []byte) int {
	lo, hi := 0, len(offsets)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if utils.CompareKeys(offsets[mid].GetKey(), key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// SeekCandidateBlock chooses the candidate block for a target key.
// For ascending seek it always returns a candidate.
// For descending seek, ok=false means there is no entry <= target.
func SeekCandidateBlock(offsets []*pb.BlockOffset, key []byte, asc bool) (blockIdx int, ok bool) {
	if len(offsets) == 0 {
		return 0, false
	}
	firstGT := SearchFirstBlockWithBaseKeyGT(offsets, key)
	if asc {
		if firstGT == 0 {
			return 0, true
		}
		return firstGT - 1, true
	}
	if firstGT == 0 {
		return 0, false
	}
	return firstGT - 1, true
}
