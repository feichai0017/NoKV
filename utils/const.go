// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "os"

const (
	// MaxLevelNum _
	MaxLevelNum = 7
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
	// MaxHeaderSize is the worst-case size for uvarint encoding.
	MaxHeaderSize           = 21
	Mi                int64 = 1 << 20
	KVWriteChCapacity       = 1000
)

// codec
var (
	MagicText    = [4]byte{'N', 'O', 'K', 'V'}
	MagicVersion = uint32(1)
)
