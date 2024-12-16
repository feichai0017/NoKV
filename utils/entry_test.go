package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueStruct(t *testing.T) {
	v := ValueStruct{
		Value:     []byte("feichai's kv"),
		Meta:      2,
		ExpiresAt: 213123123123,
	}
	data := make([]byte, v.EncodedSize())
	v.EncodeValue(data)
	var vv ValueStruct
	vv.DecodeValue(data)
	assert.Equal(t, vv, v)
}