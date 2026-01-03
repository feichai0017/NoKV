package compact

// Plan captures a compaction plan without tying it to in-memory tables.
type Plan struct {
	ThisLevel    int
	NextLevel    int
	TopIDs       []uint64
	BotIDs       []uint64
	ThisRange    KeyRange
	NextRange    KeyRange
	ThisFileSize int64
	NextFileSize int64
	IngestOnly   bool
	IngestMerge  bool
	DropPrefixes [][]byte
	StatsTag     string
}

// StateEntry creates a compaction state entry for this plan.
func (p Plan) StateEntry(thisSize int64) StateEntry {
	entry := StateEntry{
		ThisLevel: p.ThisLevel,
		NextLevel: p.NextLevel,
		ThisRange: p.ThisRange,
		NextRange: p.NextRange,
		ThisSize:  thisSize,
	}
	if len(p.TopIDs) == 0 && len(p.BotIDs) == 0 {
		return entry
	}
	entry.TableIDs = make([]uint64, 0, len(p.TopIDs)+len(p.BotIDs))
	entry.TableIDs = append(entry.TableIDs, p.TopIDs...)
	entry.TableIDs = append(entry.TableIDs, p.BotIDs...)
	return entry
}
