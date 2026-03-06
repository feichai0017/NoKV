package compact

import "errors"

// ErrFillTables indicates no eligible table set could be selected for a compaction attempt.
var ErrFillTables = errors.New("lsm/compact: fill tables")
