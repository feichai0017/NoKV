package kv

import "errors"

var (
	errStoreNotInitialized = errors.New("raftstore: store not initialized")
)

func IsStoreNotInitialized(err error) bool {
	return errors.Is(err, errStoreNotInitialized)
}
