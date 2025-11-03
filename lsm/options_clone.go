package lsm

// Clone returns a shallow copy of the LSM options. It is used when background
// workers (e.g. compaction) need an immutable view of the configuration while
// the user may continue tweaking the top-level DB options.
func (opt *Options) Clone() *Options {
	if opt == nil {
		return nil
	}
	clone := *opt
	return &clone
}
