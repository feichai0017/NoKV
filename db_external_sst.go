package NoKV

import (
	"fmt"

	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
)

func (db *DB) buildLSMOptions() *lsm.Options {
	baseTableSize, baseLevelSize := db.levelSizes()
	return &lsm.Options{
		FS:                            db.fs,
		WorkDir:                       db.opt.WorkDir,
		MemTableSize:                  db.opt.MemTableSize,
		MemTableEngine:                string(db.opt.MemTableEngine),
		SSTableMaxSz:                  db.opt.SSTableMaxSz,
		BlockSize:                     8 * 1024,
		BloomFalsePositive:            0.01,
		BaseLevelSize:                 baseLevelSize,
		LevelSizeMultiplier:           8,
		BaseTableSize:                 baseTableSize,
		TableSizeMultiplier:           2,
		NumLevelZeroTables:            db.opt.NumLevelZeroTables,
		L0SlowdownWritesTrigger:       db.opt.L0SlowdownWritesTrigger,
		L0StopWritesTrigger:           db.opt.L0StopWritesTrigger,
		L0ResumeWritesTrigger:         db.opt.L0ResumeWritesTrigger,
		CompactionSlowdownTrigger:     db.opt.CompactionSlowdownTrigger,
		CompactionStopTrigger:         db.opt.CompactionStopTrigger,
		CompactionResumeTrigger:       db.opt.CompactionResumeTrigger,
		MaxLevelNum:                   utils.MaxLevelNum,
		NumCompactors:                 db.opt.NumCompactors,
		CompactionPolicy:              string(db.opt.CompactionPolicy),
		IngestCompactBatchSize:        db.opt.IngestCompactBatchSize,
		IngestBacklogMergeScore:       db.opt.IngestBacklogMergeScore,
		IngestShardParallelism:        db.opt.IngestShardParallelism,
		WriteThrottleMinRate:          db.opt.WriteThrottleMinRate,
		WriteThrottleMaxRate:          db.opt.WriteThrottleMaxRate,
		CompactionValueWeight:         db.opt.CompactionValueWeight,
		CompactionValueAlertThreshold: db.opt.CompactionValueAlertThreshold,
		BlockCacheBytes:               db.opt.BlockCacheBytes,
		IndexCacheBytes:               db.opt.IndexCacheBytes,
		DiscardStatsCh:                &db.discardStatsCh,
		ManifestSync:                  db.opt.ManifestSync,
		ManifestRewriteThreshold:      db.opt.ManifestRewriteThreshold,
		WALGCPolicy:                   newDBWALGCPolicy(db),
		ThrottleCallback:              db.applyThrottle,
	}
}

// SSTOptions returns the normalized LSM configuration used to build
// snapshot-specific SST artifacts compatible with this DB instance.
func (db *DB) SSTOptions() *lsm.Options {
	if db == nil {
		return nil
	}
	opt := db.buildLSMOptions()
	opt.NormalizeInPlace()
	return opt
}

// IngestExternalSST exposes tracked external SST ingestion so
// callers can roll back an install before publishing higher-level metadata.
func (db *DB) IngestExternalSST(paths []string) (*lsm.ExternalSSTImportResult, error) {
	if db == nil || db.lsm == nil {
		return nil, fmt.Errorf("db: external sst import requires open lsm")
	}
	return db.lsm.IngestExternalSST(paths)
}

// RollbackExternalSST removes previously imported SST tables from the live LSM.
func (db *DB) RollbackExternalSST(fileIDs []uint64) error {
	if db == nil || db.lsm == nil {
		return fmt.Errorf("db: external sst removal requires open lsm")
	}
	return db.lsm.RollbackExternalSST(fileIDs)
}
