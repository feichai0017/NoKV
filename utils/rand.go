package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/kv"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

func Int63n(n int64) int64 {
	mu.Lock()
	res := r.Int63n(n)
	mu.Unlock()
	return res
}

func RandN(n int) int {
	mu.Lock()
	res := r.Intn(n)
	mu.Unlock()
	return res
}

func Float64() float64 {
	mu.Lock()
	res := r.Float64()
	mu.Unlock()
	return res
}

// 生成随机字符串作为key和value
func randStr(length int) string {
	if length <= 0 {
		return ""
	}
	const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡NoKV"
	result := make([]byte, length)
	for i := range length {
		result[i] = charset[RandN(len(charset))]
	}
	return string(result)
}

// 构建entry对象
func BuildEntry() *kv.Entry {
	key := fmt.Appendf(nil, "%s%s", randStr(16), "12345678")
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	e := kv.NewEntry(key, value)
	e.ExpiresAt = expiresAt
	return e
}
