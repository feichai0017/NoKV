package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

const recordHeaderSize = 24

type record struct {
	cursor rootpkg.Cursor
	event  rootpkg.Event
}

func loadLog(fs vfs.FS, workdir string, offset int64) ([]record, error) {
	path := filepath.Join(workdir, LogFileName)
	f, err := fs.OpenHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	var out []record
	for {
		rec, ok, err := readRecord(f)
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, rec)
	}
}

func rewriteLog(fs vfs.FS, workdir string, records []record) error {
	path := filepath.Join(workdir, LogFileName)
	tmp := path + ".tmp"
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := writeRecord(f, rec.cursor, rec.event); err != nil {
			_ = f.Close()
			_ = fs.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(fs, workdir)
}

func writeRecord(w io.Writer, cursor rootpkg.Cursor, event rootpkg.Event) error {
	payload, err := proto.Marshal(metacodec.RootEventToProto(event))
	if err != nil {
		return err
	}
	hdr := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint64(hdr[0:8], cursor.Term)
	binary.LittleEndian.PutUint64(hdr[8:16], cursor.Index)
	binary.LittleEndian.PutUint32(hdr[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[20:24], crc32.ChecksumIEEE(payload))
	if err := writeAll(w, hdr); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func readRecord(r io.Reader) (record, bool, error) {
	hdr := make([]byte, recordHeaderSize)
	n, err := io.ReadFull(r, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return record{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return record{}, false, nil
		}
		return record{}, false, err
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[16:20])
	expectedCRC := binary.LittleEndian.Uint32(hdr[20:24])
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return record{}, false, nil
		}
		return record{}, false, err
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return record{}, false, fmt.Errorf("meta/root/local: root log checksum mismatch")
	}
	var pbEvent metapb.RootEvent
	if err := proto.Unmarshal(payload, &pbEvent); err != nil {
		return record{}, false, err
	}
	return record{
		cursor: rootpkg.Cursor{
			Term:  binary.LittleEndian.Uint64(hdr[0:8]),
			Index: binary.LittleEndian.Uint64(hdr[8:16]),
		},
		event: metacodec.RootEventFromProto(&pbEvent),
	}, true, nil
}

func cloneRecords(in []record) []record {
	if len(in) == 0 {
		return nil
	}
	out := make([]record, 0, len(in))
	for _, rec := range in {
		out = append(out, record{
			cursor: rec.cursor,
			event:  cloneEvent(rec.event),
		})
	}
	return out
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
