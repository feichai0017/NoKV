package mvcc

import (
	"encoding/binary"
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeLockRoundTrip(t *testing.T) {
	lock := Lock{
		Primary:     []byte("primary"),
		Ts:          10,
		TTL:         20,
		Kind:        kvrpcpb.Mutation_Put,
		MinCommitTs: 30,
	}
	encoded := EncodeLock(lock)
	got, err := DecodeLock(encoded)
	require.NoError(t, err)
	require.Equal(t, lock.Primary, got.Primary)
	require.Equal(t, lock.Ts, got.Ts)
	require.Equal(t, lock.TTL, got.TTL)
	require.Equal(t, lock.Kind, got.Kind)
	require.Equal(t, lock.MinCommitTs, got.MinCommitTs)
}

func TestDecodeLockErrors(t *testing.T) {
	_, err := DecodeLock(nil)
	require.Error(t, err)

	_, err = DecodeLock([]byte{0x99})
	require.Error(t, err)

	_, err = DecodeLock([]byte{lockCodecVersion})
	require.Error(t, err)

	_, err = DecodeLock([]byte{lockCodecVersion, 0x05, 'a'})
	require.Error(t, err)
}

func TestEncodeDecodeWriteRoundTrip(t *testing.T) {
	write := Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    42,
		ShortValue: []byte("short"),
		ExpiresAt:  12345,
	}
	encoded := EncodeWrite(write)
	got, err := DecodeWrite(encoded)
	require.NoError(t, err)
	require.Equal(t, write.Kind, got.Kind)
	require.Equal(t, write.StartTs, got.StartTs)
	require.Equal(t, write.ShortValue, got.ShortValue)
	require.Equal(t, write.ExpiresAt, got.ExpiresAt)
}

func TestDecodeWriteBackwardCompatibleWithoutExpiresAt(t *testing.T) {
	// Old format: version, kind, startTs, hasShort, shortLen, shortValue.
	raw := make([]byte, 0, 32)
	raw = append(raw, writeCodecVersion, byte(kvrpcpb.Mutation_Put))
	raw = binary.AppendUvarint(raw, 7)
	raw = append(raw, 1)
	raw = binary.AppendUvarint(raw, 5)
	raw = append(raw, []byte("short")...)

	got, err := DecodeWrite(raw)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Mutation_Put, got.Kind)
	require.Equal(t, uint64(7), got.StartTs)
	require.Equal(t, []byte("short"), got.ShortValue)
	require.Equal(t, uint64(0), got.ExpiresAt)
}

func TestDecodeWriteErrors(t *testing.T) {
	_, err := DecodeWrite([]byte{writeCodecVersion})
	require.Error(t, err)

	_, err = DecodeWrite([]byte{0x99, 0x01, 0x01})
	require.Error(t, err)

	_, err = DecodeWrite([]byte{writeCodecVersion, byte(kvrpcpb.Mutation_Put), 0x01, 0x01})
	require.Error(t, err)

	_, err = DecodeWrite([]byte{writeCodecVersion, byte(kvrpcpb.Mutation_Put), 0x01, 0x01, 0x05})
	require.Error(t, err)

	// hasShort=0 with trailing truncated expires_at varint.
	_, err = DecodeWrite([]byte{writeCodecVersion, byte(kvrpcpb.Mutation_Put), 0x01, 0x00, 0x80})
	require.Error(t, err)
}
