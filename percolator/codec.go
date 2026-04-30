package percolator

import "github.com/feichai0017/NoKV/engine/mvcc"

const (
	lockCodecVersion  byte = 1
	writeCodecVersion byte = 1
)

type Lock = mvcc.Lock
type Write = mvcc.Write

func EncodeLock(lock Lock) []byte            { return mvcc.EncodeLock(lock) }
func DecodeLock(data []byte) (Lock, error)   { return mvcc.DecodeLock(data) }
func EncodeWrite(write Write) []byte         { return mvcc.EncodeWrite(write) }
func DecodeWrite(data []byte) (Write, error) { return mvcc.DecodeWrite(data) }
