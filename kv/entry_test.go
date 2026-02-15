package kv

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueStruct(t *testing.T) {
	v := ValueStruct{
		Value:     []byte("feichai's kv"),
		Meta:      2,
		ExpiresAt: 213123123123,
	}
	data := make([]byte, v.EncodedSize())
	v.EncodeValue(data)
	var decoded ValueStruct
	decoded.DecodeValue(data)
	assert.Equal(t, decoded, v)
}

func TestEntryEncodeDecodeRoundTrip(t *testing.T) {
	val := ValueStruct{
		Meta:      0x2,
		Value:     []byte("inline-value"),
		ExpiresAt: 42,
	}
	entry := &Entry{
		Key:       InternalKey(CFWrite, []byte("foo"), 99),
		Value:     encodeValueStruct(val),
		Meta:      val.Meta,
		ExpiresAt: val.ExpiresAt,
	}
	var buf bytes.Buffer
	n, err := EncodeEntryTo(&buf, entry)
	require.NoError(t, err)
	require.Equal(t, buf.Len(), n)

	decoded, recordLen, err := DecodeEntryFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	t.Cleanup(func() { decoded.DecrRef() })
	require.Equal(t, uint32(n), recordLen)

	assert.Equal(t, entry.Meta, decoded.Meta)
	assert.Equal(t, entry.ExpiresAt, decoded.ExpiresAt)
	assert.Equal(t, entry.Key, decoded.Key)
	assert.Equal(t, entry.Value, decoded.Value)

	cf, userKey, ts := SplitInternalKey(decoded.Key)
	assert.Equal(t, CFWrite, cf)
	assert.Equal(t, []byte("foo"), userKey)
	assert.Equal(t, uint64(99), ts)
}

func TestDecodeEntryFromEOFAndPartial(t *testing.T) {
	e := &Entry{
		Key:       InternalKey(CFDefault, []byte("bar"), 1),
		Value:     encodeValueStruct(ValueStruct{Value: []byte("baz")}),
		ExpiresAt: 5,
	}
	var buf bytes.Buffer
	_, err := EncodeEntryTo(&buf, e)
	require.NoError(t, err)

	decoded, _, err := DecodeEntryFrom(bytes.NewReader(nil))
	assert.Nil(t, decoded)
	assert.ErrorIs(t, err, io.EOF)

	truncated := buf.Bytes()[:buf.Len()-2]
	decoded, _, err = DecodeEntryFrom(bytes.NewReader(truncated))
	assert.Nil(t, decoded)
	assert.ErrorIs(t, err, ErrPartialEntry)
}

func TestEntryIterator(t *testing.T) {
	var buf bytes.Buffer
	for ts := uint64(1); ts <= 2; ts++ {
		e := &Entry{
			Key:       InternalKey(CFDefault, []byte("iter"), ts),
			Value:     encodeValueStruct(ValueStruct{Value: []byte{byte('a' + ts)}}),
			ExpiresAt: ts,
		}
		_, err := EncodeEntryTo(&buf, e)
		require.NoError(t, err)
	}

	it := NewEntryIterator(bytes.NewReader(buf.Bytes()))
	defer func() { require.NoError(t, it.Close()) }()

	count := 0
	for it.Next() {
		count++
		assert.Greater(t, it.RecordLen(), uint32(0))
		entry := it.Entry()
		require.NotNil(t, entry)
		cf, userKey, ts := SplitInternalKey(entry.Key)
		assert.Equal(t, CFDefault, cf)
		assert.Equal(t, []byte("iter"), userKey)
		assert.Equal(t, uint64(count), ts)
	}
	assert.Equal(t, 2, count)
	assert.ErrorIs(t, it.Err(), io.EOF)
}

func TestValuePtrEncodeDecode(t *testing.T) {
	ptr := ValuePtr{Len: 1, Offset: 2, Fid: 3, Bucket: 4}
	encoded := ptr.Encode()
	assert.Equal(t, valuePtrEncodedSize, len(encoded))
	assert.Equal(t, []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4}, encoded)

	var decoded ValuePtr
	decoded.Decode(encoded)
	assert.Equal(t, ptr, decoded)

	decoded.Len, decoded.Offset, decoded.Fid, decoded.Bucket = 9, 9, 9, 9
	decoded.Decode([]byte{1, 2}) // short buffer clears ptr
	assert.Equal(t, ValuePtr{}, decoded)
}

func TestEntryHelpers(t *testing.T) {
	e := NewEntry([]byte("k"), []byte("v"))
	defer e.DecrRef()
	if e.Entry() != e {
		t.Fatalf("expected Entry() to return receiver")
	}
	if e.CF != CFDefault {
		t.Fatalf("expected default CF, got %v", e.CF)
	}

	e2 := NewEntryWithCF(CFLock, []byte("lk"), []byte("lv"))
	defer e2.DecrRef()
	if e2.CF != CFLock {
		t.Fatalf("expected CFLock, got %v", e2.CF)
	}

	if !e2.WithColumnFamily(CFWrite).CF.Valid() {
		t.Fatalf("expected valid CF after WithColumnFamily")
	}

	if e2.IsDeletedOrExpired() {
		t.Fatalf("unexpected deleted/expired for live entry")
	}

	e2.Value = nil
	if !e2.IsDeletedOrExpired() {
		t.Fatalf("expected deleted entry")
	}

	e2.Value = []byte("lv")
	e2.WithTTL(1)
	if e2.ExpiresAt == 0 {
		t.Fatalf("expected ttl to set expiresAt")
	}

	if e2.EncodedSize() == 0 {
		t.Fatalf("expected encoded size > 0")
	}

	szInline := e2.EstimateSize(32)
	szPtr := e2.EstimateSize(1)
	if szInline >= szPtr {
		t.Fatalf("expected pointer estimate > inline estimate")
	}
}

func TestEntryDecrRefDetachedNoop(t *testing.T) {
	e := &Entry{Key: []byte("k"), Value: []byte("v")}
	e.DecrRef()
	require.Equal(t, []byte("k"), e.Key)
	require.Equal(t, []byte("v"), e.Value)
	require.Equal(t, int32(0), e.ref)
}

func TestValueHelpers(t *testing.T) {
	ptr := ValuePtr{Len: 2, Offset: 3, Fid: 4}
	if ptr.IsZero() {
		t.Fatalf("expected non-zero ValuePtr")
	}
	if ptr.Less(nil) {
		t.Fatalf("expected Less to be false for nil")
	}
	if !ptr.Less(&ValuePtr{Len: 5, Offset: 3, Fid: 4}) {
		t.Fatalf("expected ptr to be less than larger len")
	}

	entry := &Entry{Meta: BitValuePointer}
	if !IsValuePtr(entry) {
		t.Fatalf("expected IsValuePtr to be true")
	}

	u32 := uint32(0xAABBCCDD)
	if got := BytesToU32(U32ToBytes(u32)); got != u32 {
		t.Fatalf("expected round-trip u32, got %x", got)
	}
	u64 := uint64(0x1122334455667788)
	if got := BytesToU64(U64ToBytes(u64)); got != u64 {
		t.Fatalf("expected round-trip u64, got %x", got)
	}
	if BytesToU32Slice(nil) != nil {
		t.Fatalf("expected nil slice for empty input")
	}
	if U32SliceToBytes(nil) != nil {
		t.Fatalf("expected nil bytes for empty input")
	}
	u32s := []uint32{1, 2, 3}
	raw := U32SliceToBytes(u32s)
	back := BytesToU32Slice(raw)
	require.Equal(t, u32s, back)

	called := false
	RunCallback(nil)
	RunCallback(func() { called = true })
	if !called {
		t.Fatalf("expected callback to run")
	}

	if !IsDeletedOrExpired(BitDelete, 0) {
		t.Fatalf("expected deleted meta to be expired")
	}
	if IsDeletedOrExpired(0, 0) {
		t.Fatalf("expected non-expiring value to be live")
	}
	if !IsDeletedOrExpired(0, uint64(time.Now().Add(-time.Second).Unix())) {
		t.Fatalf("expected past ttl to be expired")
	}
	if IsDeletedOrExpired(0, uint64(time.Now().Add(time.Hour).Unix())) {
		t.Fatalf("expected future ttl to be live")
	}

	vs := &Entry{Meta: BitValuePointer, ExpiresAt: uint64(time.Now().Add(time.Hour).Unix())}
	if DiscardEntry(nil, vs) {
		t.Fatalf("expected pointer entry to be retained")
	}
	vs.Meta = 0
	if !DiscardEntry(nil, vs) {
		t.Fatalf("expected inline entry to be discarded")
	}
}

func encodeValueStruct(v ValueStruct) []byte {
	buf := make([]byte, v.EncodedSize())
	v.EncodeValue(buf)
	return buf
}
