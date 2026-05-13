package peras

func writeBytesHash(h interface{ Write([]byte) (int, error) }, value []byte) {
	writeUint64(h, uint64(len(value)))
	_, _ = h.Write(value)
}

func digestFromHash(sum []byte) [32]byte {
	var out [32]byte
	copy(out[:], sum)
	return out
}
