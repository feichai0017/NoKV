package dirpage

import "errors"

var (
	errPageTruncated   = errors.New("dirpage: record truncated")
	errPageBadMagic    = errors.New("dirpage: bad magic")
	errPageBadVersion  = errors.New("dirpage: unsupported version")
	errPageBadChecksum = errors.New("dirpage: checksum mismatch")
)
