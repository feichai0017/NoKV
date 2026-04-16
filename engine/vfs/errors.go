package vfs

import "errors"

// ErrRenameNoReplaceUnsupported indicates the platform/filesystem cannot provide
// atomic no-replace rename semantics.
var ErrRenameNoReplaceUnsupported = errors.New("vfs: rename no-replace unsupported")
