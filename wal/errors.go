package wal

import "errors"

var (
	// ErrPartialRecord indicates a WAL record ended prematurely (typically due to EOF/corruption).
	ErrPartialRecord = errors.New("wal: partial record")
	// ErrEmptyRecord indicates a WAL record header advertised zero payload length.
	ErrEmptyRecord = errors.New("wal: empty record")
)
