package rootraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	hardStateFileName = "root-raft-hardstate.pb"
	snapshotFileName  = "root-raft-snapshot.pb"
	walFileName       = "root-raft-wal"
	walHeaderSize     = 8
)

type persistedStorage struct {
	fs      vfs.FS
	workdir string
}

func openPersistedStorage(workdir string, fs vfs.FS) (*persistedStorage, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("meta/root/raft: storage workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	return &persistedStorage{fs: fs, workdir: workdir}, nil
}

func (p *persistedStorage) load() (myraft.HardState, myraft.Snapshot, []myraft.Entry, error) {
	hard, err := p.loadHardState()
	if err != nil {
		return myraft.HardState{}, myraft.Snapshot{}, nil, err
	}
	snap, err := p.loadSnapshot()
	if err != nil {
		return myraft.HardState{}, myraft.Snapshot{}, nil, err
	}
	entries, err := p.loadEntries()
	if err != nil {
		return myraft.HardState{}, myraft.Snapshot{}, nil, err
	}
	return hard, snap, entries, nil
}

func (p *persistedStorage) save(hard myraft.HardState, snap myraft.Snapshot, entries []myraft.Entry) error {
	if err := p.saveHardState(hard); err != nil {
		return err
	}
	if err := p.saveSnapshot(snap); err != nil {
		return err
	}
	if err := p.rewriteEntries(entries); err != nil {
		return err
	}
	return nil
}

func (p *persistedStorage) loadHardState() (myraft.HardState, error) {
	path := filepath.Join(p.workdir, hardStateFileName)
	data, err := p.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return myraft.HardState{}, nil
		}
		return myraft.HardState{}, err
	}
	if len(data) == 0 {
		return myraft.HardState{}, nil
	}
	var hs pb.HardState
	if err := hs.Unmarshal(data); err != nil {
		return myraft.HardState{}, err
	}
	return hs, nil
}

func (p *persistedStorage) saveHardState(hard myraft.HardState) error {
	if myraft.IsEmptyHardState(hard) {
		return nil
	}
	data, err := hard.Marshal()
	if err != nil {
		return err
	}
	return p.writeAtomic(hardStateFileName, data)
}

func (p *persistedStorage) loadSnapshot() (myraft.Snapshot, error) {
	path := filepath.Join(p.workdir, snapshotFileName)
	data, err := p.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return myraft.Snapshot{}, nil
		}
		return myraft.Snapshot{}, err
	}
	if len(data) == 0 {
		return myraft.Snapshot{}, nil
	}
	var snap pb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return myraft.Snapshot{}, err
	}
	return snap, nil
}

func (p *persistedStorage) saveSnapshot(snap myraft.Snapshot) error {
	if myraft.IsEmptySnap(snap) {
		return nil
	}
	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	return p.writeAtomic(snapshotFileName, data)
}

func (p *persistedStorage) loadEntries() ([]myraft.Entry, error) {
	f, err := p.fs.OpenHandle(filepath.Join(p.workdir, walFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []myraft.Entry
	for {
		entry, ok, err := readEntryRecord(f)
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, entry)
	}
}

func (p *persistedStorage) rewriteEntries(entries []myraft.Entry) error {
	path := filepath.Join(p.workdir, walFileName)
	tmp := path + ".tmp"
	f, err := p.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeEntryRecord(f, entry); err != nil {
			_ = f.Close()
			_ = p.fs.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = p.fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = p.fs.Remove(tmp)
		return err
	}
	if err := p.fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(p.fs, p.workdir)
}

func (p *persistedStorage) writeAtomic(name string, data []byte) error {
	path := filepath.Join(p.workdir, name)
	tmp := path + ".tmp"
	f, err := p.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = p.fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = p.fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = p.fs.Remove(tmp)
		return err
	}
	if err := p.fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(p.fs, p.workdir)
}

func writeEntryRecord(w io.Writer, entry myraft.Entry) error {
	payload, err := entry.Marshal()
	if err != nil {
		return err
	}
	hdr := make([]byte, walHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readEntryRecord(r io.Reader) (myraft.Entry, bool, error) {
	hdr := make([]byte, walHeaderSize)
	n, err := io.ReadFull(r, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return myraft.Entry{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return myraft.Entry{}, false, nil
		}
		return myraft.Entry{}, false, err
	}
	payload := make([]byte, binary.LittleEndian.Uint32(hdr[0:4]))
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return myraft.Entry{}, false, nil
		}
		return myraft.Entry{}, false, err
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(hdr[4:8]) {
		return myraft.Entry{}, false, fmt.Errorf("meta/root/raft: wal checksum mismatch")
	}
	var entry pb.Entry
	if err := entry.Unmarshal(payload); err != nil {
		return myraft.Entry{}, false, err
	}
	return myraft.Entry(entry), true, nil
}
