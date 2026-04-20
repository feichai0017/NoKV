package main

import (
	"errors"
	"time"
)

type temporaryBackendError struct {
	msg string
	err error
}

func (e *temporaryBackendError) Error() string {
	if e == nil {
		return "TRYAGAIN backend unavailable"
	}
	if e.err != nil {
		return e.msg + ": " + e.err.Error()
	}
	return e.msg
}

func (e *temporaryBackendError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func isTemporaryBackendError(err error) bool {
	var target *temporaryBackendError
	return errors.As(err, &target)
}

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
	TTL      time.Duration
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
