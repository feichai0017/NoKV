// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import "errors"

var (
	errCheckpointMissingState = errors.New("root checkpoint missing state")
)
