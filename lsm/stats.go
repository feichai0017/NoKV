package lsm

import "github.com/feichai0017/NoKV/utils"

// EntryCount returns the total number of physical entries tracked by the LSM tree,
// including keys present in all memtables and SSTables. The count does not attempt
// to de-duplicate older versions or tombstones; it is intended for operational
// introspection.
func (lsm *LSM) EntryCount() int64 {
	if lsm == nil {
		return 0
	}
	var total int64
	if tables, release := lsm.GetMemTables(); tables != nil {
		if release != nil {
			defer release()
		}
		for _, mt := range tables {
			if mt == nil || mt.sl == nil {
				continue
			}
			total += countSkiplistEntries(mt.sl)
		}
	}
	if lsm.levels != nil {
		total += lsm.levels.entryCount()
	}
	return total
}

func countSkiplistEntries(sl *utils.Skiplist) int64 {
	if sl == nil {
		return 0
	}
	itr := sl.NewSkipListIterator()
	defer itr.Close()
	itr.Rewind()
	var count int64
	for ; itr.Valid(); itr.Next() {
		count++
	}
	return count
}

func (lm *levelManager) entryCount() int64 {
	if lm == nil {
		return 0
	}
	var total int64
	for _, level := range lm.levels {
		if level == nil {
			continue
		}
		for _, tbl := range level.tablesSnapshot() {
			if tbl == nil || tbl.ss == nil {
				continue
			}
			total += int64(tbl.ss.Indexs().GetKeyCount())
		}
	}
	return total
}

func (lh *levelHandler) tablesSnapshot() []*table {
	if lh == nil {
		return nil
	}
	lh.RLock()
	defer lh.RUnlock()
	out := make([]*table, len(lh.tables))
	copy(out, lh.tables)
	return out
}