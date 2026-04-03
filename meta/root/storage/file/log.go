package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

const recordHeaderSize = 24

type fileEventLog struct {
	fs      vfs.FS
	workdir string
}

func (l fileEventLog) ReadCommitted(offset int64) (rootstorage.CommittedStream, error) {
	path := filepath.Join(l.workdir, LogFileName)
	f, err := l.fs.OpenHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootstorage.CommittedStream{Offset: offset, EndOffset: offset}, nil
		}
		return rootstorage.CommittedStream{}, err
	}
	defer func() { _ = f.Close() }()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return rootstorage.CommittedStream{}, err
		}
	}
	var out []rootstorage.CommittedEvent
	for {
		rec, ok, err := readRecord(f)
		if err != nil {
			return rootstorage.CommittedStream{}, err
		}
		if !ok {
			end, err := fileSize(f)
			if err != nil {
				return rootstorage.CommittedStream{}, err
			}
			return rootstorage.CommittedStream{
				Offset:    offset,
				EndOffset: end,
				Records:   out,
			}, nil
		}
		out = append(out, rec)
	}
}

func (l fileEventLog) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	path := filepath.Join(l.workdir, LogFileName)
	f, err := l.fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	for _, rec := range records {
		if err := writeRecord(f, rec); err != nil {
			_ = f.Close()
			return 0, err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	logEnd, err := fileSize(f)
	if err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return logEnd, nil
}

func (l fileEventLog) CompactCommitted(stream rootstorage.CommittedStream) error {
	path := filepath.Join(l.workdir, LogFileName)
	tmp := path + ".tmp"
	f, err := l.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, rec := range stream.Records {
		if err := writeRecord(f, rec); err != nil {
			_ = f.Close()
			_ = l.fs.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = l.fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = l.fs.Remove(tmp)
		return err
	}
	if err := l.fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(l.fs, l.workdir)
}

func (l fileEventLog) Size() (int64, error) {
	info, err := l.fs.Stat(filepath.Join(l.workdir, LogFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func writeRecord(w io.Writer, rec rootstorage.CommittedEvent) error {
	payload, err := proto.Marshal(metacodec.RootEventToProto(rec.Event))
	if err != nil {
		return err
	}
	hdr := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint64(hdr[0:8], rec.Cursor.Term)
	binary.LittleEndian.PutUint64(hdr[8:16], rec.Cursor.Index)
	binary.LittleEndian.PutUint32(hdr[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[20:24], crc32.ChecksumIEEE(payload))
	if err := writeAll(w, hdr); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func readRecord(r io.Reader) (rootstorage.CommittedEvent, bool, error) {
	hdr := make([]byte, recordHeaderSize)
	n, err := io.ReadFull(r, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return rootstorage.CommittedEvent{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return rootstorage.CommittedEvent{}, false, nil
		}
		return rootstorage.CommittedEvent{}, false, err
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[16:20])
	expectedCRC := binary.LittleEndian.Uint32(hdr[20:24])
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return rootstorage.CommittedEvent{}, false, nil
		}
		return rootstorage.CommittedEvent{}, false, err
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return rootstorage.CommittedEvent{}, false, fmt.Errorf("meta/root/storage/file: root log checksum mismatch")
	}
	var pbEvent metapb.RootEvent
	if err := proto.Unmarshal(payload, &pbEvent); err != nil {
		return rootstorage.CommittedEvent{}, false, err
	}
	return rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{
			Term:  binary.LittleEndian.Uint64(hdr[0:8]),
			Index: binary.LittleEndian.Uint64(hdr[8:16]),
		},
		Event: metacodec.RootEventFromProto(&pbEvent),
	}, true, nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
