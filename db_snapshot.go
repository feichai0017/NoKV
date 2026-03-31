package NoKV

import (
	"fmt"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/utils"
)

type snapshotSource struct {
	db  *DB
	lsm *lsm.LSM
}

func (src snapshotSource) NewInternalIterator(opt *utils.Options) utils.Iterator {
	return src.db.NewInternalIterator(opt)
}

func (src snapshotSource) MaterializeInternalEntry(entry *kv.Entry) (*kv.Entry, error) {
	return src.db.MaterializeInternalEntry(entry)
}

func (src snapshotSource) ExternalSSTOptions() *lsm.Options {
	return src.lsm.ExternalSSTOptions()
}

type snapshotTarget struct {
	lsm *lsm.LSM
}

func (dst snapshotTarget) ImportExternalSST(paths []string) (*lsm.ExternalSSTImportResult, error) {
	return dst.lsm.ImportExternalSST(paths)
}

func (dst snapshotTarget) RollbackExternalSST(fileIDs []uint64) error {
	return dst.lsm.RollbackExternalSST(fileIDs)
}

func (db *DB) openLSM() (*lsm.LSM, error) {
	if db == nil || db.IsClosed() || db.lsm == nil {
		return nil, fmt.Errorf("db: snapshot bridge requires open db")
	}
	return db.lsm, nil
}

func (db *DB) snapshotSource() (snapshotSource, error) {
	lsmCore, err := db.openLSM()
	if err != nil {
		return snapshotSource{}, err
	}
	return snapshotSource{db: db, lsm: lsmCore}, nil
}

func (db *DB) snapshotTarget() (snapshotTarget, error) {
	lsmCore, err := db.openLSM()
	if err != nil {
		return snapshotTarget{}, err
	}
	return snapshotTarget{lsm: lsmCore}, nil
}

// ExportFiles persists one region-scoped snapshot directory in SST form.
func (db *DB) ExportFiles(dir string, region raftmeta.RegionMeta) (*snapshotpkg.ExportResult, error) {
	src, err := db.snapshotSource()
	if err != nil {
		return nil, err
	}
	return snapshotpkg.ExportFiles(src, dir, region, nil)
}

// ImportFiles imports one region-scoped snapshot directory into the current DB.
func (db *DB) ImportFiles(dir string) (*snapshotpkg.ImportResult, error) {
	dst, err := db.snapshotTarget()
	if err != nil {
		return nil, err
	}
	return snapshotpkg.ImportFiles(dst, dir, nil)
}

// ExportSnapshot materializes one region-scoped snapshot payload using the
// current DB's storage format and workdir.
func (db *DB) ExportSnapshot(region raftmeta.RegionMeta) ([]byte, error) {
	src, err := db.snapshotSource()
	if err != nil {
		return nil, err
	}
	payload, _, err := snapshotpkg.ExportPayload(src, db.WorkDir(), region, nil)
	return payload, err
}

// ImportSnapshot imports one region-scoped snapshot payload into the current
// DB and returns the full staged-import result.
//
// Callers that only need the region metadata can read result.Meta.Region.
// Callers that publish peer metadata later can use result.Rollback() if the
// higher-level install lifecycle fails after SST import completes.
func (db *DB) ImportSnapshot(payload []byte) (*snapshotpkg.ImportResult, error) {
	dst, err := db.snapshotTarget()
	if err != nil {
		return nil, err
	}
	return snapshotpkg.ImportPayload(dst, db.WorkDir(), payload, nil)
}
