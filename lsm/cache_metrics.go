package lsm

import "expvar"

var (
	cacheBypassCount  = expvar.NewInt("NoKV.Cache.Bypass")
	cacheZeroCopyUses = expvar.NewInt("NoKV.Cache.ZeroCopy")
	cacheAutoBypass   = expvar.NewInt("NoKV.Cache.AutoBypass")
)
