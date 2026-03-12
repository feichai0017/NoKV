package sstable

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
)

func TestMustBlockCacheKey(t *testing.T) {
	got := MustBlockCacheKey(7, 11)
	want := (uint64(7) << 32) | uint64(11)
	if got != want {
		t.Fatalf("block cache key mismatch: got=%d want=%d", got, want)
	}
}

func TestBlockOffset(t *testing.T) {
	idx := &pb.TableIndex{
		Offsets: []*pb.BlockOffset{
			{Key: []byte("a"), Offset: 0},
			{Key: []byte("c"), Offset: 10},
		},
	}
	if _, ok := BlockOffset(idx, -1); ok {
		t.Fatalf("negative index should fail")
	}
	if _, ok := BlockOffset(idx, 3); ok {
		t.Fatalf("index > len(offsets) should fail")
	}
	if off, ok := BlockOffset(idx, 2); !ok || off != nil {
		t.Fatalf("index == len(offsets) should return (nil, true)")
	}
	off, ok := BlockOffset(idx, 1)
	if !ok || off == nil || string(off.GetKey()) != "c" {
		t.Fatalf("unexpected offset result: ok=%v off=%v", ok, off)
	}
}

func TestSearchAndSeekCandidate(t *testing.T) {
	mk := func(user string) []byte {
		return kv.InternalKey(kv.CFDefault, []byte(user), kv.MaxVersion)
	}
	offsets := []*pb.BlockOffset{
		{Key: mk("a")},
		{Key: mk("d")},
		{Key: mk("m")},
	}

	if got := SearchFirstBlockWithBaseKeyGT(offsets, mk("0")); got != 0 {
		t.Fatalf("firstGT for key before first block: got=%d", got)
	}
	if got := SearchFirstBlockWithBaseKeyGT(offsets, mk("d")); got != 2 {
		t.Fatalf("firstGT for key at middle block: got=%d", got)
	}
	if got := SearchFirstBlockWithBaseKeyGT(offsets, mk("z")); got != len(offsets) {
		t.Fatalf("firstGT for key after last block: got=%d", got)
	}

	if blk, ok := SeekCandidateBlock(offsets, mk("0"), true); !ok || blk != 0 {
		t.Fatalf("ascending seek candidate mismatch: ok=%v blk=%d", ok, blk)
	}
	if blk, ok := SeekCandidateBlock(offsets, mk("f"), true); !ok || blk != 1 {
		t.Fatalf("ascending seek candidate mismatch: ok=%v blk=%d", ok, blk)
	}
	if _, ok := SeekCandidateBlock(offsets, mk("0"), false); ok {
		t.Fatalf("descending seek should not have candidate for key before first block")
	}
	if blk, ok := SeekCandidateBlock(offsets, mk("f"), false); !ok || blk != 1 {
		t.Fatalf("descending seek candidate mismatch: ok=%v blk=%d", ok, blk)
	}
}

func TestIndexMetaAccessors(t *testing.T) {
	if KeyCount(nil) != 0 {
		t.Fatalf("nil key count must be 0")
	}
	if MaxVersion(nil) != 0 {
		t.Fatalf("nil max version must be 0")
	}
	if HasBloomFilter(nil) {
		t.Fatalf("nil index cannot have bloom filter")
	}

	idx := &pb.TableIndex{
		KeyCount:    123,
		MaxVersion:  77,
		BloomFilter: []byte{1, 2, 3},
	}
	if KeyCount(idx) != 123 {
		t.Fatalf("key count mismatch")
	}
	if MaxVersion(idx) != 77 {
		t.Fatalf("max version mismatch")
	}
	if !HasBloomFilter(idx) {
		t.Fatalf("expected bloom filter")
	}
}
