package main

import (
	"errors"
)

var (
	errConditionNotMet = errors.New("redis condition not met")
	errUnsupported     = errors.New("redis feature unsupported for current backend")
)

type redisValue struct {
	Value     []byte
	ExpiresAt uint64
	Found     bool
}

func (v *redisValue) GetExpiresAt() uint64 {
	if v == nil {
		return 0
	}
	return v.ExpiresAt
}

type setArgs struct {
	Key      []byte
	Value    []byte
	NX       bool
	XX       bool
	ExpireAt uint64
}

type redisBackend interface {
	Get(key []byte) (*redisValue, error)
	Set(args setArgs) (bool, error)
	Del(keys [][]byte) (int64, error)
	MGet(keys [][]byte) ([]*redisValue, error)
	MSet(pairs [][2][]byte) error
	Exists(keys [][]byte) (int64, error)
	IncrBy(key []byte, delta int64) (int64, error)
	Close() error
}
