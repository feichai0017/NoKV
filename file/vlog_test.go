package file

import (
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/stretchr/testify/require"
)

func TestLogFileBootstrapReadWrite(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		FID:      1,
		FileName: filepath.Join(dir, "vlog"),
		MaxSz:    1 << 20,
	}

	var lf LogFile
	require.NoError(t, lf.Open(opt))
	defer lf.Close()

	require.NoError(t, lf.Bootstrap())

	payload := []byte("hello-world")
	offset := kv.ValueLogHeaderSize
	require.NoError(t, lf.Write(uint32(offset), payload))

	vp := &kv.ValuePtr{Offset: uint32(offset), Len: uint32(len(payload))}
	read, err := lf.Read(vp)
	require.NoError(t, err)
	require.Equal(t, payload, read)

	require.NoError(t, lf.SetReadOnly())
	require.True(t, lf.ro)
	require.NoError(t, lf.SetWritable())
	require.False(t, lf.ro)
}
