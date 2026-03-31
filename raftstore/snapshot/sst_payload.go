package snapshot

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/vfs"
)

// ExportPayload materializes one SST snapshot and bundles it into
// a transport-safe payload.
func ExportPayload(src exportSource, workDir string, region raftmeta.RegionMeta, fs vfs.FS) ([]byte, Meta, error) {
	dir, cleanup, err := prepareSnapshotTempDir(workDir, "sst-export-*", fs)
	if err != nil {
		return nil, Meta{}, err
	}
	defer cleanup()
	snapshotDir := filepath.Join(dir, "snapshot")
	result, err := ExportFiles(src, snapshotDir, region, fs)
	if err != nil {
		return nil, Meta{}, err
	}
	payload, err := bundleSSTPayload(snapshotDir, result.Meta, fs)
	if err != nil {
		return nil, Meta{}, err
	}
	return payload, result.Meta, nil
}

// ImportPayload unpacks one SST snapshot payload into a temporary workdir and
// installs it through the external SST ingest path.
func ImportPayload(dst installSink, workDir string, payload []byte, fs vfs.FS) (*ImportResult, error) {
	if dst == nil {
		return nil, fmt.Errorf("snapshot: import sst payload requires sink")
	}
	meta, err := ReadPayloadMeta(payload)
	if err != nil {
		return nil, err
	}
	dir, cleanup, err := prepareSnapshotTempDir(workDir, "sst-import-*", fs)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	snapshotDir := filepath.Join(dir, "snapshot")
	if err := unpackSSTPayload(payload, snapshotDir, meta, fs); err != nil {
		return nil, err
	}
	return ImportFiles(dst, snapshotDir, fs)
}

// ReadPayloadMeta decodes only the metadata embedded in one snapshot payload.
func ReadPayloadMeta(payload []byte) (Meta, error) {
	if len(payload) == 0 {
		return Meta{}, fmt.Errorf("snapshot: empty sst payload")
	}
	tr := tar.NewReader(bytes.NewReader(payload))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Meta{}, fmt.Errorf("snapshot: read sst payload tar: %w", err)
		}
		if filepath.Clean(hdr.Name) != sstSnapshotName {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return Meta{}, fmt.Errorf("snapshot: read sst payload meta: %w", err)
		}
		var meta Meta
		if err := json.Unmarshal(data, &meta); err != nil {
			return Meta{}, fmt.Errorf("snapshot: decode sst payload meta: %w", err)
		}
		if meta.Version != sstVersion {
			return Meta{}, fmt.Errorf("snapshot: unsupported sst version %d", meta.Version)
		}
		return meta, nil
	}
	return Meta{}, fmt.Errorf("snapshot: sst payload missing %s", sstSnapshotName)
}

func bundleSSTPayload(dir string, meta Meta, fs vfs.FS) ([]byte, error) {
	fs = vfs.Ensure(fs)
	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	writeFile := func(rel string) error {
		path := filepath.Join(dir, rel)
		info, err := fs.Stat(path)
		if err != nil {
			return fmt.Errorf("snapshot: stat sst snapshot file %s: %w", path, err)
		}
		data, err := fs.ReadFile(path)
		if err != nil {
			return fmt.Errorf("snapshot: read sst snapshot file %s: %w", path, err)
		}
		hdr := &tar.Header{
			Name:    filepath.ToSlash(rel),
			Mode:    0o600,
			Size:    int64(len(data)),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("snapshot: write sst snapshot header %s: %w", rel, err)
		}
		if _, err := tw.Write(data); err != nil {
			return fmt.Errorf("snapshot: write sst snapshot body %s: %w", rel, err)
		}
		return nil
	}
	if err := writeFile(sstSnapshotName); err != nil {
		return nil, err
	}
	for _, table := range meta.Tables {
		if err := writeFile(table.RelativePath); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("snapshot: finalize sst snapshot payload: %w", err)
	}
	return payload.Bytes(), nil
}

func unpackSSTPayload(payload []byte, dir string, meta Meta, fs vfs.FS) error {
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("snapshot: create sst snapshot dir %s: %w", dir, err)
	}
	tr := tar.NewReader(bytes.NewReader(payload))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("snapshot: read sst snapshot payload: %w", err)
		}
		name := filepath.Clean(hdr.Name)
		if name == "." || strings.HasPrefix(name, "..") || filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
			return fmt.Errorf("snapshot: invalid sst snapshot path %q", hdr.Name)
		}
		targetPath := filepath.Join(dir, name)
		parent := filepath.Dir(targetPath)
		if err := fs.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("snapshot: create sst snapshot parent %s: %w", parent, err)
		}
		f, err := fs.OpenFileHandle(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("snapshot: create sst snapshot file %s: %w", targetPath, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			_ = f.Close()
			return fmt.Errorf("snapshot: write sst snapshot file %s: %w", targetPath, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("snapshot: sync sst snapshot file %s: %w", targetPath, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("snapshot: close sst snapshot file %s: %w", targetPath, err)
		}
	}
	if _, err := ReadMeta(dir, fs); err != nil {
		return err
	}
	for _, table := range meta.Tables {
		if _, err := fs.Stat(filepath.Join(dir, table.RelativePath)); err != nil {
			return fmt.Errorf("snapshot: unpacked sst snapshot missing table %s: %w", table.RelativePath, err)
		}
	}
	return nil
}
