package namespace

import "sort"

func hasDeltaKeyPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := range prefix {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

type pageDeltaSegment struct {
	PageID  []byte
	Pairs   []KVPair
	Records int
	Bytes   int
}

type deltaSnapshot struct {
	parent          []byte
	bootstrap       []KVPair
	pageLocal       []KVPair
	bootstrapByPage map[string][]KVPair
	pageLocalByPage map[string][]KVPair
	pageCounts      map[string]int
	pageBytes       map[string]int
}

func (s *Store) loadDeltaSnapshot(parent []byte) (deltaSnapshot, error) {
	bootstrapPairs, err := s.kv.ScanPrefix(encodeListingDeltaParentPrefix(parent), nil, 0)
	if err != nil {
		return deltaSnapshot{}, err
	}
	pageLocalPairs, err := s.kv.ScanPrefix(encodePageDeltaParentPrefix(parent), nil, 0)
	if err != nil {
		return deltaSnapshot{}, err
	}
	snapshot := deltaSnapshot{
		parent:          cloneBytes(parent),
		bootstrap:       bootstrapPairs,
		pageLocal:       pageLocalPairs,
		bootstrapByPage: make(map[string][]KVPair),
		pageLocalByPage: make(map[string][]KVPair),
		pageCounts:      make(map[string]int),
		pageBytes:       make(map[string]int),
	}
	for _, pair := range bootstrapPairs {
		pageID, err := decodeListingDeltaPageIDFromKey(parent, pair.Key)
		if err != nil {
			return deltaSnapshot{}, err
		}
		pageKey := string(pageID)
		snapshot.bootstrapByPage[pageKey] = append(snapshot.bootstrapByPage[pageKey], pair)
		snapshot.pageCounts[pageKey]++
		snapshot.pageBytes[pageKey] += len(pair.Key) + len(pair.Value)
	}
	for _, pair := range pageLocalPairs {
		pageID, err := decodePageDeltaPageIDFromKey(parent, pair.Key)
		if err != nil {
			return deltaSnapshot{}, err
		}
		pageKey := string(pageID)
		snapshot.pageLocalByPage[pageKey] = append(snapshot.pageLocalByPage[pageKey], pair)
		snapshot.pageCounts[pageKey]++
		snapshot.pageBytes[pageKey] += len(pair.Key) + len(pair.Value)
	}
	return snapshot, nil
}

func (d deltaSnapshot) totalCount() int {
	return len(d.bootstrap) + len(d.pageLocal)
}

func (d deltaSnapshot) hasBootstrap() bool {
	return len(d.bootstrap) > 0
}

func (d deltaSnapshot) allPairs() []KVPair {
	if len(d.bootstrap) == 0 {
		return d.pageLocal
	}
	if len(d.pageLocal) == 0 {
		return d.bootstrap
	}
	out := make([]KVPair, 0, len(d.bootstrap)+len(d.pageLocal))
	out = append(out, d.bootstrap...)
	out = append(out, d.pageLocal...)
	return out
}

func (d deltaSnapshot) maxSeq() uint64 {
	var maxSeq uint64
	for _, pair := range d.bootstrap {
		delta, err := decodeListingDelta(pair.Value)
		if err != nil {
			continue
		}
		if delta.Seq > maxSeq {
			maxSeq = delta.Seq
		}
	}
	for _, pair := range d.pageLocal {
		delta, err := decodePageDeltaRecord(pair.Value)
		if err != nil {
			continue
		}
		if delta.Seq > maxSeq {
			maxSeq = delta.Seq
		}
	}
	return maxSeq
}

func maxDeltaSeq(pairs []KVPair) uint64 {
	var maxSeq uint64
	for _, pair := range pairs {
		var (
			delta ListingDelta
			err   error
		)
		switch {
		case len(pair.Key) > 0 && hasDeltaKeyPrefix(pair.Key, deltaKeyPrefix):
			delta, err = decodeListingDelta(pair.Value)
		case len(pair.Key) > 0 && hasDeltaKeyPrefix(pair.Key, pageDeltaPrefix):
			delta, err = decodePageDeltaRecord(pair.Value)
		default:
			continue
		}
		if err != nil {
			continue
		}
		if delta.Seq > maxSeq {
			maxSeq = delta.Seq
		}
	}
	return maxSeq
}

func (d deltaSnapshot) listingStats(pages []ReadPage) ListingStats {
	stats := ListingStats{
		DeltaRecords:       d.totalCount(),
		DistinctDeltaPages: len(d.pageCounts),
	}
	if len(pages) > 0 {
		stats.MaterializedPages = len(pages)
		for _, page := range pages {
			stats.MaterializedEntries += len(page.Entries)
		}
	}
	return stats
}

func (d deltaSnapshot) hottestPageSegments(root ReadRoot, limit int) []pageDeltaSegment {
	if limit <= 0 {
		return nil
	}
	type rankedSegment struct {
		segment pageDeltaSegment
		order   int
	}
	ranked := make([]rankedSegment, 0, len(d.pageLocalByPage))
	for i, ref := range root.Pages {
		pageKey := string(ref.PageID)
		pairs, ok := d.pageLocalByPage[pageKey]
		if !ok || len(pairs) == 0 {
			continue
		}
		ranked = append(ranked, rankedSegment{
			segment: pageDeltaSegment{
				PageID:  cloneBytes(ref.PageID),
				Pairs:   pairs,
				Records: len(pairs),
				Bytes:   d.pageBytes[pageKey],
			},
			order: i,
		})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].segment.Records != ranked[j].segment.Records {
			return ranked[i].segment.Records > ranked[j].segment.Records
		}
		if ranked[i].segment.Bytes != ranked[j].segment.Bytes {
			return ranked[i].segment.Bytes > ranked[j].segment.Bytes
		}
		return ranked[i].order < ranked[j].order
	})
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	out := make([]pageDeltaSegment, 0, len(ranked))
	for _, item := range ranked {
		out = append(out, item.segment)
	}
	return out
}

func (d deltaSnapshot) selectPairs(root ReadRoot, hasReadPlane bool, maxDeltaPages int, shards int) ([]KVPair, map[string]struct{}, error) {
	selected := make([]KVPair, 0, d.totalCount())
	selectedPages := make(map[string]struct{})

	if !hasReadPlane && maxDeltaPages > 0 {
		for shard := 0; shard < shards && len(selectedPages) < maxDeltaPages; shard++ {
			pageID := encodePageID(uint32(shard))
			pageKey := string(pageID)
			pagePairs, ok := d.bootstrapByPage[pageKey]
			if !ok {
				continue
			}
			selected = append(selected, pagePairs...)
			selectedPages[pageKey] = struct{}{}
		}
		return selected, selectedPages, nil
	}

	selected = append(selected, d.bootstrap...)
	for pageKey := range d.bootstrapByPage {
		selectedPages[pageKey] = struct{}{}
	}
	if !hasReadPlane {
		return selected, selectedPages, nil
	}

	if maxDeltaPages > 0 {
		for _, segment := range d.hottestPageSegments(root, maxDeltaPages) {
			selected = append(selected, segment.Pairs...)
			selectedPages[string(segment.PageID)] = struct{}{}
		}
		return selected, selectedPages, nil
	}

	selected = append(selected, d.pageLocal...)
	for pageKey := range d.pageLocalByPage {
		selectedPages[pageKey] = struct{}{}
	}
	return selected, selectedPages, nil
}
