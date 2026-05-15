// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package failpoints

import "errors"

// ErrAfterSealGrantBeforeReload is the sentinel surfaced when the
// AfterSealGrantBeforeReload failpoint fires. Tests injecting the failpoint
// expect this exact sentinel via errors.Is.
var ErrAfterSealGrantBeforeReload = errors.New("coordinator failpoint: after apply coordinator grant retirement before reload")
