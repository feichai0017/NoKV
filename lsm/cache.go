package lsm

import (
	coreCache "github.com/feichai0017/NoKV/utils/cache"
)

type cache struct {
	indexs *coreCache.Cache // key fid， value table
	blocks *coreCache.Cache // key fid_blockOffset  value block []byte
}

type blockBuffer struct {
	b []byte
}

const defaultCacheSize = 1024

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{indexs: coreCache.NewCache(defaultCacheSize), blocks: coreCache.NewCache(defaultCacheSize)}
}

// TODO fid 使用字符串是不是会有性能损耗
func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}