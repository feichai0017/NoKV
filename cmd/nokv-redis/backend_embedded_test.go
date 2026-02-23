package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func newTestEmbeddedBackend(t *testing.T) *embeddedBackend {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.ValueThreshold = 0
	if opt.MaxBatchCount <= 0 {
		opt.MaxBatchCount = 1024
	}
	if opt.MaxBatchSize <= 0 {
		opt.MaxBatchSize = 16 << 20
	}
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = db.Close() })
	return newEmbeddedBackend(db)
}

func TestEmbeddedBackendSetNXandXX(t *testing.T) {
	backend := newTestEmbeddedBackend(t)

	ok, err := backend.Set(setArgs{
		Key: []byte("k1"), Value: []byte("v1"), NX: true,
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = backend.Set(setArgs{
		Key: []byte("k1"), Value: []byte("v2"), NX: true,
	})
	require.ErrorIs(t, err, errConditionNotMet)
	require.False(t, ok)

	ok, err = backend.Set(setArgs{
		Key: []byte("k1"), Value: []byte("v2"), XX: true,
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = backend.Set(setArgs{
		Key: []byte("k2"), Value: []byte("z"), XX: true,
	})
	require.ErrorIs(t, err, errConditionNotMet)
	require.False(t, ok)

	val, err := backend.Get([]byte("k1"))
	require.NoError(t, err)
	require.True(t, val.Found)
	require.Equal(t, []byte("v2"), val.Value)
}

func TestEmbeddedBackendDelAndExists(t *testing.T) {
	backend := newTestEmbeddedBackend(t)

	_, err := backend.Set(setArgs{Key: []byte("a"), Value: []byte("1")})
	require.NoError(t, err)
	_, err = backend.Set(setArgs{Key: []byte("b"), Value: []byte("2")})
	require.NoError(t, err)

	count, err := backend.Exists([][]byte{[]byte("a"), []byte("missing"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	removed, err := backend.Del([][]byte{[]byte("a"), []byte("missing"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, int64(2), removed)

	count, err = backend.Exists([][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestEmbeddedBackendTTLExpire(t *testing.T) {
	backend := newTestEmbeddedBackend(t)

	expireAt := uint64(time.Now().Add(500 * time.Millisecond).Unix())
	if expireAt <= uint64(time.Now().Unix()) {
		expireAt = uint64(time.Now().Unix() + 1)
	}
	ok, err := backend.Set(setArgs{
		Key: []byte("ttl"), Value: []byte("v"), ExpireAt: expireAt,
	})
	require.NoError(t, err)
	require.True(t, ok)

	val, err := backend.Get([]byte("ttl"))
	require.NoError(t, err)
	require.True(t, val.Found)

	time.Sleep(1200 * time.Millisecond)
	val, err = backend.Get([]byte("ttl"))
	require.NoError(t, err)
	require.False(t, val.Found)
}

func TestEmbeddedBackendMSetAndMGet(t *testing.T) {
	backend := newTestEmbeddedBackend(t)

	pairs := [][2][]byte{
		{[]byte("k1"), []byte("v1")},
		{[]byte("k2"), []byte("v2")},
	}
	require.NoError(t, backend.MSet(pairs))

	values, err := backend.MGet([][]byte{[]byte("k1"), []byte("missing"), []byte("k2")})
	require.NoError(t, err)
	require.Len(t, values, 3)
	require.True(t, values[0].Found)
	require.Equal(t, []byte("v1"), values[0].Value)
	require.False(t, values[1].Found)
	require.True(t, values[2].Found)
	require.Equal(t, []byte("v2"), values[2].Value)
}

func TestEmbeddedBackendSetErrorOnEmptyKey(t *testing.T) {
	backend := newTestEmbeddedBackend(t)

	ok, err := backend.Set(setArgs{Key: nil, Value: []byte("v")})
	require.Error(t, err)
	require.False(t, ok)
	require.ErrorIs(t, err, utils.ErrEmptyKey)
}

func TestEmbeddedBackendGetMissingKey(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	val, err := backend.Get([]byte("missing"))
	require.NoError(t, err)
	require.False(t, val.Found)
}

func TestEmbeddedBackendSetPlainAndExpire(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	expireAt := uint64(time.Now().Add(10 * time.Second).Unix())
	ok, err := backend.Set(setArgs{
		Key: []byte("plain"), Value: []byte("v1"), ExpireAt: expireAt,
	})
	require.NoError(t, err)
	require.True(t, ok)

	val, err := backend.Get([]byte("plain"))
	require.NoError(t, err)
	require.True(t, val.Found)
	require.Equal(t, expireAt, val.ExpiresAt)
}

func TestEmbeddedBackendSetNXAfterExpire(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	expireAt := uint64(time.Now().Unix() + 1)
	ok, err := backend.Set(setArgs{
		Key: []byte("exp"), Value: []byte("v"), ExpireAt: expireAt,
	})
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(1200 * time.Millisecond)
	ok, err = backend.Set(setArgs{
		Key: []byte("exp"), Value: []byte("v2"), NX: true,
	})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestEmbeddedBackendDelExpiredKey(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	expireAt := uint64(time.Now().Unix() + 1)
	ok, err := backend.Set(setArgs{
		Key: []byte("gone"), Value: []byte("v"), ExpireAt: expireAt,
	})
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(1200 * time.Millisecond)
	removed, err := backend.Del([][]byte{[]byte("gone")})
	require.NoError(t, err)
	require.Equal(t, int64(0), removed)
}

func TestEmbeddedBackendMGetMissingAndExpired(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	expireAt := uint64(time.Now().Unix() + 1)
	ok, err := backend.Set(setArgs{
		Key: []byte("exp"), Value: []byte("v"), ExpireAt: expireAt,
	})
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(1200 * time.Millisecond)
	vals, err := backend.MGet([][]byte{[]byte("exp"), []byte("missing")})
	require.NoError(t, err)
	require.Len(t, vals, 2)
	require.False(t, vals[0].Found)
	require.False(t, vals[1].Found)
}

func TestEmbeddedBackendMSetEmptyKey(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	err := backend.MSet([][2][]byte{{nil, []byte("v")}})
	require.ErrorIs(t, err, utils.ErrEmptyKey)
}

func TestEmbeddedBackendIncrByErrors(t *testing.T) {
	backend := newTestEmbeddedBackend(t)
	_, err := backend.Set(setArgs{Key: []byte("bad"), Value: []byte("x")})
	require.NoError(t, err)
	_, err = backend.IncrBy([]byte("bad"), 1)
	require.ErrorIs(t, err, errNotInteger)

	_, err = backend.Set(setArgs{Key: []byte("max"), Value: []byte("9223372036854775807")})
	require.NoError(t, err)
	_, err = backend.IncrBy([]byte("max"), 1)
	require.ErrorIs(t, err, errOverflow)
}

func TestStrconvParseIntSafe(t *testing.T) {
	val, err := strconvParseIntSafe([]byte(" "))
	require.NoError(t, err)
	require.Equal(t, int64(0), val)

	_, err = strconvParseIntSafe([]byte("nope"))
	require.Error(t, err)
}
