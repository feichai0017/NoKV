package namespace

import (
	"bytes"
	"encoding/json"
)

var (
	truthKeyPrefix   = []byte("M|")
	listingKeyPrefix = []byte("L|")
	deltaKeyPrefix   = []byte("LD|")
)

func encodeTruthKey(path []byte) []byte {
	out := make([]byte, 0, len(truthKeyPrefix)+len(path))
	out = append(out, truthKeyPrefix...)
	out = append(out, path...)
	return out
}

func encodeListingPrefix(parent []byte) []byte {
	out := make([]byte, 0, len(listingKeyPrefix)+len(parent)+1)
	out = append(out, listingKeyPrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	return out
}

func encodeListingPageKey(parent, pageID []byte) []byte {
	out := encodeListingPrefix(parent)
	out = append(out, pageID...)
	return out
}

func encodeListingDeltaPrefix(parent, pageID []byte) []byte {
	out := make([]byte, 0, len(deltaKeyPrefix)+len(parent)+1+len(pageID)+1)
	out = append(out, deltaKeyPrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	out = append(out, pageID...)
	out = append(out, '|')
	return out
}

func encodeListingDeltaKey(parent, pageID, name []byte) []byte {
	out := encodeListingDeltaPrefix(parent, pageID)
	out = append(out, name...)
	return out
}

func encodeListingPage(page ListingPage) ([]byte, error) {
	return json.Marshal(page)
}

func decodeListingPage(raw []byte) (ListingPage, error) {
	var page ListingPage
	err := json.Unmarshal(raw, &page)
	return page, err
}

func splitPath(path []byte) (parent, name []byte, err error) {
	if len(path) == 0 || path[0] != '/' || bytes.Equal(path, []byte("/")) {
		return nil, nil, ErrInvalidPath
	}
	idx := bytes.LastIndexByte(path, '/')
	if idx <= 0 || idx == len(path)-1 {
		return nil, nil, ErrInvalidPath
	}
	return cloneBytes(path[:idx]), cloneBytes(path[idx+1:]), nil
}
