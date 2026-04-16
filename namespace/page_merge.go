package namespace

import (
	"bytes"
	"sort"
)

func mergePageEntriesWithDeltaPairs(parent []byte, baseEntries []Entry, deltaPairs []KVPair) ([]Entry, error) {
	return mergePageEntriesWithDeltaPairsMode(parent, baseEntries, deltaPairs, true)
}

func mergePageEntriesWithDeltaPairsNoMeta(parent []byte, baseEntries []Entry, deltaPairs []KVPair) ([]Entry, error) {
	return mergePageEntriesWithDeltaPairsMode(parent, baseEntries, deltaPairs, false)
}

func mergePageEntriesWithDeltaPairsMode(parent []byte, baseEntries []Entry, deltaPairs []KVPair, includeMeta bool) ([]Entry, error) {
	if len(deltaPairs) == 0 {
		out := make([]Entry, 0, len(baseEntries))
		for _, entry := range baseEntries {
			if includeMeta {
				out = append(out, cloneEntry(entry))
			} else {
				out = append(out, cloneEntryNoMeta(entry))
			}
		}
		return out, nil
	}
	type latestDelta struct {
		delta ListingDelta
		seq   uint64
	}
	latestByName := make(map[string]latestDelta, len(deltaPairs))
	for _, pair := range deltaPairs {
		delta, err := decodeAnyDeltaFromKV(parent, pair.Key, pair.Value)
		if err != nil {
			return nil, err
		}
		var seq uint64
		if bytes.HasPrefix(pair.Key, encodePageDeltaParentPrefix(parent)) {
			if parsed, err := decodePageDeltaSeqFromKey(parent, pair.Key); err == nil {
				seq = parsed
			}
		}
		nameKey := string(delta.Name)
		current, ok := latestByName[nameKey]
		if !ok || seq >= current.seq {
			latestByName[nameKey] = latestDelta{delta: delta, seq: seq}
		}
	}
	deltas := make([]ListingDelta, 0, len(latestByName))
	for _, item := range latestByName {
		deltas = append(deltas, item.delta)
	}
	sort.Slice(deltas, func(i, j int) bool {
		return bytes.Compare(deltas[i].Name, deltas[j].Name) < 0
	})
	out := make([]Entry, 0, len(baseEntries)+len(deltas))
	i, j := 0, 0
	for i < len(baseEntries) && j < len(deltas) {
		switch cmp := bytes.Compare(baseEntries[i].Name, deltas[j].Name); {
		case cmp < 0:
			if includeMeta {
				out = append(out, cloneEntry(baseEntries[i]))
			} else {
				entry := baseEntries[i]
				entry.MetaKey = nil
				out = append(out, entry)
			}
			i++
		case cmp > 0:
			if deltas[j].Op == DeltaOpAdd {
				entry := Entry{
					Name: cloneBytes(deltas[j].Name),
					Kind: deltas[j].Kind,
				}
				if includeMeta {
					entry.MetaKey = encodeTruthKey(joinPath(parent, deltas[j].Name))
				}
				out = append(out, entry)
			}
			j++
		default:
			if deltas[j].Op == DeltaOpAdd {
				entry := Entry{
					Name: cloneBytes(deltas[j].Name),
					Kind: deltas[j].Kind,
				}
				if includeMeta {
					entry.MetaKey = encodeTruthKey(joinPath(parent, deltas[j].Name))
				}
				out = append(out, entry)
			}
			i++
			j++
		}
	}
	for ; i < len(baseEntries); i++ {
		if includeMeta {
			out = append(out, cloneEntry(baseEntries[i]))
		} else {
			entry := baseEntries[i]
			entry.MetaKey = nil
			out = append(out, entry)
		}
	}
	for ; j < len(deltas); j++ {
		if deltas[j].Op != DeltaOpAdd {
			continue
		}
		entry := Entry{
			Name: cloneBytes(deltas[j].Name),
			Kind: deltas[j].Kind,
		}
		if includeMeta {
			entry.MetaKey = encodeTruthKey(joinPath(parent, deltas[j].Name))
		}
		out = append(out, entry)
	}
	return out, nil
}
