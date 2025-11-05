package kv

import (
	"bufio"
	"io"
)

// EntryStream provides a simple iterator-style interface over a value-log or
// WAL stream, yielding fully decoded entries together with their on-disk length.
// Callers must release resources via Close to return pooled entries.
type EntryStream struct {
	reader   *bufio.Reader
	current  *Entry
	lastSize uint32
	err      error
}

// NewEntryStream constructs an EntryStream backed by the supplied reader. The
// stream buffers reads internally for efficiency.
func NewEntryStream(r io.Reader) *EntryStream {
	if r == nil {
		return &EntryStream{err: io.EOF}
	}
	if br, ok := r.(*bufio.Reader); ok {
		return &EntryStream{reader: br}
	}
	return &EntryStream{reader: bufio.NewReader(r)}
}

// Next decodes the next entry from the stream, returning true when an entry was
// read successfully. Callers should retrieve the entry via Entry() and release
// it when done. When Next returns false, Err reports the termination reason.
func (es *EntryStream) Next() bool {
	if es == nil || es.err != nil {
		return false
	}
	es.releaseCurrent()
	entry, size, err := DecodeEntryFrom(es.reader)
	if err != nil {
		es.err = err
		return false
	}
	es.current = entry
	es.lastSize = size
	return true
}

// Entry returns the most recently decoded entry. Ownership of the entry remains
// with the stream until the next call to Next or Close.
func (es *EntryStream) Entry() *Entry {
	if es == nil {
		return nil
	}
	return es.current
}

// RecordLen reports the encoded length of the most recently decoded entry.
func (es *EntryStream) RecordLen() uint32 {
	if es == nil {
		return 0
	}
	return es.lastSize
}

// Err returns the terminal error that ended the iteration. A nil value or
// io.EOF indicates successful exhaustion.
func (es *EntryStream) Err() error {
	if es == nil {
		return nil
	}
	return es.err
}

// Close releases any pooled entry held by the stream.
func (es *EntryStream) Close() error {
	if es == nil {
		return nil
	}
	es.releaseCurrent()
	return nil
}

func (es *EntryStream) releaseCurrent() {
	if es == nil || es.current == nil {
		return
	}
	es.current.DecrRef()
	es.current = nil
}
