package utils

// AccessPattern hints the OS about expected page access behaviour for a mapped
// file. It is deliberately minimal to avoid leaking platform-specific flags.
type AccessPattern int

const (
	AccessPatternAuto AccessPattern = iota
	AccessPatternNormal
	AccessPatternSequential
	AccessPatternRandom
	AccessPatternWillNeed
	AccessPatternDontNeed
)
