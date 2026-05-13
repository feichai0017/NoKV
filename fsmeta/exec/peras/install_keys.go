package peras

import (
	"bytes"

	"github.com/feichai0017/NoKV/fsmeta"
)

// PerasSegmentInstallKeys returns the exact metadata keys written by a segment
// install command. The apply scheduler uses this set as the dependency surface
// for CMD_PERAS_INSTALL_SEGMENT, so malformed or mismatched routing state fails
// closed instead of producing a partial key set.
func PerasSegmentInstallKeys(segment PerasSegment, routingKey []byte, materialize bool) ([][]byte, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	if materialize {
		return perasSegmentMaterializeInstallKeys(segment)
	}
	return perasSegmentCatalogInstallKeys(segment, routingKey)
}

func perasSegmentMaterializeInstallKeys(segment PerasSegment) ([][]byte, error) {
	keys := make([][]byte, 0, len(segment.entries)+2)
	err := segment.ForEachEntry(func(entry SegmentKV) error {
		if len(entry.Key) == 0 {
			return ErrInvalidPerasSegment
		}
		keys = appendUniquePerasInstallKey(keys, entry.Key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	objectKey, err := PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	keys = appendUniquePerasInstallKey(keys, objectKey)
	indexKeys, err := PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, err
	}
	for _, key := range indexKeys {
		keys = appendUniquePerasInstallKey(keys, key)
	}
	return keys, nil
}

func perasSegmentCatalogInstallKeys(segment PerasSegment, routingKey []byte) ([][]byte, error) {
	if len(routingKey) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	if !perasSegmentCatalogObjectKeyMatches(segment, routingKey) {
		return nil, ErrInvalidPerasSegment
	}
	objectKey, err := PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	parts, ok := fsmeta.InspectKey(routingKey)
	if !ok || parts.Kind != fsmeta.KeyKindPeras || parts.PerasRecord != fsmeta.PerasSegmentRecordObject || parts.PerasRoot != segment.Root {
		return nil, ErrInvalidPerasSegment
	}
	indexKey, err := fsmeta.EncodePerasSegmentCatalogIndexKey(parts.MountKeyID, parts.Bucket, segment.Root)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, 2)
	keys = appendUniquePerasInstallKey(keys, objectKey)
	keys = appendUniquePerasInstallKey(keys, indexKey)
	return keys, nil
}

func perasSegmentCatalogObjectKeyMatches(segment PerasSegment, key []byte) bool {
	objectKeys, err := PerasSegmentCatalogObjectKeys(segment)
	if err != nil {
		return false
	}
	for _, objectKey := range objectKeys {
		if bytes.Equal(objectKey, key) {
			return true
		}
	}
	return false
}

func appendUniquePerasInstallKey(keys [][]byte, key []byte) [][]byte {
	if len(key) == 0 {
		return keys
	}
	for _, existing := range keys {
		if bytes.Equal(existing, key) {
			return keys
		}
	}
	return append(keys, append([]byte(nil), key...))
}
