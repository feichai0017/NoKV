package manifest

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const editMagic = "NoKV"

// Internal encoding helpers
func writeEdit(w io.Writer, edit Edit) error {
	// Overall Manifest Entry Binary Format:
	// +----------------------+--------------------------------+
	// | Length (4B, LittleE) | Payload (Edit Record)          |
	// +----------------------+--------------------------------+
	//
	// Payload (Edit Record) Binary Format:
	// +----------------+------------+--------------------------+
	// | Magic (4B)     | EditType (1B) | Type-Specific Data       |
	// +----------------+------------+--------------------------+

	buf := make([]byte, 0, 64)
	// Magic + Type
	buf = append(buf, []byte(editMagic)...)
	buf = append(buf, byte(edit.Type))

	// Type-Specific Data
	switch edit.Type {
	case EditAddFile, EditDeleteFile:
		// EditAddFile / EditDeleteFile Data Format:
		// +----------------+----------------+----------------+----------------+----------------+
		// | Level (v)      | FileID (v)     | Size (v)       | Smallest (lv)  | Largest (lv)   |
		// +----------------+----------------+----------------+----------------+----------------+
		// | CreatedAt (v)  | ValueSize (v)  | Ingest (1B)    |
		// +----------------+----------------+
		// (v) denotes Uvarint, (lv) denotes Length-prefixed Bytes (Uvarint length + bytes)
		meta := edit.File
		buf = binary.AppendUvarint(buf, uint64(meta.Level))
		buf = binary.AppendUvarint(buf, meta.FileID)
		buf = binary.AppendUvarint(buf, meta.Size)
		buf = appendBytes(buf, meta.Smallest)
		buf = appendBytes(buf, meta.Largest)
		buf = binary.AppendUvarint(buf, meta.CreatedAt)
		buf = binary.AppendUvarint(buf, meta.ValueSize)
		if meta.Ingest {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case EditLogPointer:
		// EditLogPointer Data Format:
		// +----------------+----------------+
		// | LogSegment (v) | LogOffset (v)  |
		// +----------------+----------------+
		// (v) denotes Uvarint
		buf = binary.AppendUvarint(buf, uint64(edit.LogSeg))
		buf = binary.AppendUvarint(buf, edit.LogOffset)
	case EditValueLogHead:
		// EditValueLogHead Data Format:
		// +----------------+----------------+----------------+
		// | Bucket (v)     | FileID (v)     | Offset (v)     |
		// +----------------+----------------+----------------+
		// (v) denotes Uvarint
		if edit.ValueLog != nil {
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.Bucket))
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.FileID))
			buf = binary.AppendUvarint(buf, edit.ValueLog.Offset)
		}
	case EditDeleteValueLog:
		// EditDeleteValueLog Data Format:
		// +----------------+----------------+
		// | Bucket (v)     | FileID (v)     |
		// +----------------+----------------+
		// (v) denotes Uvarint
		if edit.ValueLog != nil {
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.Bucket))
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.FileID))
		}
	case EditUpdateValueLog:
		// EditUpdateValueLog Data Format:
		// +----------------+----------------+----------------+----------+
		// | Bucket (v)     | FileID (v)     | Offset (v)     | Valid (1B)|
		// +----------------+----------------+----------------+----------+
		// (v) denotes Uvarint
		if edit.ValueLog != nil {
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.Bucket))
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.FileID))
			buf = binary.AppendUvarint(buf, edit.ValueLog.Offset)
			if edit.ValueLog.Valid {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	case EditRaftPointer:
		// EditRaftPointer Data Format:
		// +-----------------+-----------------+-----------------+-----------------+
		// | GroupID (v)     | Segment (v)     | Offset (v)      | AppliedIndex (v)|
		// +-----------------+-----------------+-----------------+-----------------+
		// | AppliedTerm (v) | Committed (v)   | SnapshotIndex (v)| SnapshotTerm (v)|
		// +-----------------+-----------------+-----------------+-----------------+
		// | TruncatedIndex (v)| TruncatedTerm (v)| SegmentIndex (v)| TruncatedOffset (v)|
		// +-----------------+-----------------+-----------------+-----------------+
		// (v) denotes Uvarint
		if edit.Raft != nil {
			buf = binary.AppendUvarint(buf, edit.Raft.GroupID)
			buf = binary.AppendUvarint(buf, uint64(edit.Raft.Segment))
			buf = binary.AppendUvarint(buf, edit.Raft.Offset)
			buf = binary.AppendUvarint(buf, edit.Raft.AppliedIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.AppliedTerm)
			buf = binary.AppendUvarint(buf, edit.Raft.Committed)
			buf = binary.AppendUvarint(buf, edit.Raft.SnapshotIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.SnapshotTerm)
			buf = binary.AppendUvarint(buf, edit.Raft.TruncatedIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.TruncatedTerm)
			buf = binary.AppendUvarint(buf, edit.Raft.SegmentIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.TruncatedOffset)
		}
	case EditRegion:
		// EditRegion Data Format:
		// +----------------+------------+----------------------------------------------------+
		// | RegionID (v)   | Delete (1B)| If Delete=0: StartKey (lv) | EndKey (lv) | Epoch.Version (v) |
		// +----------------+------------+----------------------------------------------------+
		// | Epoch.ConfVersion (v) | State (1B) | PeersCount (v) | Peer1.StoreID (v) | Peer1.PeerID (v) | ... |
		// +-----------------------+------------+----------------+-------------------+------------------+
		// (v) denotes Uvarint, (lv) denotes Length-prefixed Bytes (Uvarint length + bytes)
		if edit.Region != nil {
			buf = binary.AppendUvarint(buf, edit.Region.Meta.ID)
			if edit.Region.Delete {
				buf = append(buf, 1)
				break
			}
			buf = append(buf, 0)
			buf = appendBytes(buf, edit.Region.Meta.StartKey)
			buf = appendBytes(buf, edit.Region.Meta.EndKey)
			buf = binary.AppendUvarint(buf, edit.Region.Meta.Epoch.Version)
			buf = binary.AppendUvarint(buf, edit.Region.Meta.Epoch.ConfVersion)
			buf = append(buf, byte(edit.Region.Meta.State))
			buf = binary.AppendUvarint(buf, uint64(len(edit.Region.Meta.Peers)))
			for _, peer := range edit.Region.Meta.Peers {
				buf = binary.AppendUvarint(buf, peer.StoreID)
				buf = binary.AppendUvarint(buf, peer.PeerID)
			}
		}
	}
	// length prefix
	length := uint32(len(buf))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := w.Write(buf)
	return err
}

func readEdit(r *bufio.Reader) (Edit, error) {
	// Overall Manifest Entry Binary Format:
	// +----------------------+--------------------------------+
	// | Length (4B, LittleE) | Payload (Edit Record)          |
	// +----------------------+--------------------------------+
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return Edit{}, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Edit{}, err
	}
	return decodeEdit(data)
}

func decodeEdit(data []byte) (Edit, error) {
	// Payload (Edit Record) Binary Format:
	// +----------------+------------+--------------------------+
	// | Magic (4B)     | EditType (1B) | Type-Specific Data       |
	// +----------------+------------+--------------------------+
	//
	// Type-Specific Data formats are described in writeEdit function.

	if len(data) < len(editMagic)+1 {
		return Edit{}, fmt.Errorf("manifest entry too short")
	}
	if string(data[:len(editMagic)]) != editMagic {
		return Edit{}, fmt.Errorf("bad manifest magic")
	}
	edit := Edit{Type: EditType(data[len(editMagic)])}
	pos := len(editMagic) + 1
	switch edit.Type {
	case EditAddFile, EditDeleteFile:
		// EditAddFile / EditDeleteFile Data Format:
		// +----------------+----------------+----------------+----------------+----------------+
		// | Level (v)      | FileID (v)     | Size (v)       | Smallest (lv)  | Largest (lv)   |
		// +----------------+----------------+----------------+----------------+----------------+
		// | CreatedAt (v)  | ValueSize (v)  | Ingest (1B)    |
		// +----------------+----------------+
		// (v) denotes Uvarint, (lv) denotes Length-prefixed Bytes (Uvarint length + bytes)
		level, n := binary.Uvarint(data[pos:])
		pos += n
		fileID, n := binary.Uvarint(data[pos:])
		pos += n
		size, n := binary.Uvarint(data[pos:])
		pos += n
		smallest, n := readBytes(data[pos:])
		pos += n
		largest, n := readBytes(data[pos:])
		pos += n
		created, n := binary.Uvarint(data[pos:])
		pos += n
		var valueSize uint64
		if pos <= len(data) {
			if pos == len(data) {
				valueSize = 0
			} else {
				vs, consumed := binary.Uvarint(data[pos:])
				pos += consumed
				valueSize = vs
			}
		}
		var ingest bool
		if pos < len(data) {
			ingest = data[pos] == 1
			pos++
		}
		if pos > len(data) {
			return Edit{}, fmt.Errorf("manifest add/delete truncated")
		}
		edit.File = &FileMeta{
			Level:     int(level),
			FileID:    fileID,
			Size:      size,
			Smallest:  smallest,
			Largest:   largest,
			CreatedAt: created,
			ValueSize: valueSize,
			Ingest:    ingest,
		}
	case EditLogPointer:
		// EditLogPointer Data Format:
		// +----------------+----------------+
		// | LogSegment (v) | LogOffset (v)  |
		// +----------------+----------------+
		// (v) denotes Uvarint
		seg, n := binary.Uvarint(data[pos:])
		pos += n
		off, n := binary.Uvarint(data[pos:])
		pos += n
		if pos > len(data) {
			return Edit{}, fmt.Errorf("manifest log pointer truncated")
		}
		edit.LogSeg = uint32(seg)
		edit.LogOffset = off
	case EditValueLogHead:
		// EditValueLogHead Data Format:
		// +----------------+----------------+----------------+
		// | Bucket (v)     | FileID (v)     | Offset (v)     |
		// +----------------+----------------+----------------+
		// (v) denotes Uvarint
		if pos < len(data) {
			bucket64, n := binary.Uvarint(data[pos:])
			pos += n
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			offset, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest value log head truncated")
			}
			edit.ValueLog = &ValueLogMeta{
				Bucket: uint32(bucket64),
				FileID: uint32(fid64),
				Offset: offset,
				Valid:  true,
			}
		}
	case EditDeleteValueLog:
		// EditDeleteValueLog Data Format:
		// +----------------+----------------+
		// | Bucket (v)     | FileID (v)     |
		// +----------------+----------------+
		// (v) denotes Uvarint
		if pos < len(data) {
			bucket64, n := binary.Uvarint(data[pos:])
			pos += n
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest value log delete truncated")
			}
			edit.ValueLog = &ValueLogMeta{
				Bucket: uint32(bucket64),
				FileID: uint32(fid64),
			}
		}
	case EditUpdateValueLog:
		// EditUpdateValueLog Data Format:
		// +----------------+----------------+----------------+----------+
		// | Bucket (v)     | FileID (v)     | Offset (v)     | Valid (1B)|
		// +----------------+----------------+----------------+----------+
		// (v) denotes Uvarint
		if pos < len(data) {
			bucket64, n := binary.Uvarint(data[pos:])
			pos += n
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			offset, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest value log update truncated")
			}
			valid := false
			if pos < len(data) {
				valid = data[pos] == 1
			}
			edit.ValueLog = &ValueLogMeta{
				Bucket: uint32(bucket64),
				FileID: uint32(fid64),
				Offset: offset,
				Valid:  valid,
			}
		}
	case EditRaftPointer:
		// EditRaftPointer Data Format:
		// +-----------------+-----------------+-----------------+-----------------+
		// | GroupID (v)     | Segment (v)     | Offset (v)      | AppliedIndex (v)|
		// +-----------------+-----------------+-----------------+-----------------+
		// | AppliedTerm (v) | Committed (v)   | SnapshotIndex (v)| SnapshotTerm (v)|
		// +-----------------+-----------------+-----------------+-----------------+
		// | TruncatedIndex (v)| TruncatedTerm (v)| SegmentIndex (v)| TruncatedOffset (v)|
		// +-----------------+-----------------+-----------------+-----------------+
		// (v) denotes Uvarint
		if pos <= len(data) {
			groupID, n := binary.Uvarint(data[pos:])
			pos += n
			seg, n := binary.Uvarint(data[pos:])
			pos += n
			off, n := binary.Uvarint(data[pos:])
			pos += n
			appliedIdx, n := binary.Uvarint(data[pos:])
			pos += n
			appliedTerm, n := binary.Uvarint(data[pos:])
			pos += n
			committed, n := binary.Uvarint(data[pos:])
			pos += n
			snapIdx, n := binary.Uvarint(data[pos:])
			pos += n
			snapTerm, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest raft pointer truncated")
			}
			var truncatedIdx uint64
			var truncatedTerm uint64
			var segmentIndex uint64
			var truncatedOffset uint64
			if pos < len(data) {
				truncatedIdx, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer truncated index overflow")
				}
			}
			if pos < len(data) {
				truncatedTerm, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer truncated term overflow")
				}
			}
			if pos < len(data) {
				segmentIndex, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer segment index overflow")
				}
			}
			if pos < len(data) {
				truncatedOffset, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer truncated offset overflow")
				}
			}
			edit.Raft = &RaftLogPointer{
				GroupID:         groupID,
				Segment:         uint32(seg),
				Offset:          off,
				AppliedIndex:    appliedIdx,
				AppliedTerm:     appliedTerm,
				Committed:       committed,
				SnapshotIndex:   snapIdx,
				SnapshotTerm:    snapTerm,
				TruncatedIndex:  truncatedIdx,
				TruncatedTerm:   truncatedTerm,
				SegmentIndex:    segmentIndex,
				TruncatedOffset: truncatedOffset,
			}
		}
	case EditRegion:
		// EditRegion Data Format:
		// +----------------+------------+----------------------------------------------------+
		// | RegionID (v)   | Delete (1B)| If Delete=0: StartKey (lv) | EndKey (lv) | Epoch.Version (v) |
		// +----------------+------------+----------------------------------------------------+
		// | Epoch.ConfVersion (v) | State (1B) | PeersCount (v) | Peer1.StoreID (v) | Peer1.PeerID (v) | ... |
		// +-----------------------+------------+----------------+-------------------+------------------+
		// (v) denotes Uvarint, (lv) denotes Length-prefixed Bytes (Uvarint length + bytes)
		if pos <= len(data) {
			regionID, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest region edit truncated after id")
			}
			var delete bool
			if pos < len(data) {
				delete = data[pos] == 1
				pos++
			}
			if delete {
				edit.Region = &RegionEdit{
					Meta:   RegionMeta{ID: regionID},
					Delete: true,
				}
				break
			}
			start, n := readBytes(data[pos:])
			pos += n
			end, n := readBytes(data[pos:])
			pos += n
			version, n := binary.Uvarint(data[pos:])
			pos += n
			confVer, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest region edit truncated epoch")
			}
			var state RegionState
			if pos < len(data) {
				state = RegionState(data[pos])
				pos++
			}
			peersCount := uint64(0)
			if pos < len(data) {
				peersCount, n = binary.Uvarint(data[pos:])
				pos += n
			}
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest region edit truncated peer count")
			}
			peers := make([]PeerMeta, 0, peersCount)
			for i := uint64(0); i < peersCount; i++ {
				storeID, n := binary.Uvarint(data[pos:])
				pos += n
				peerID, n := binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest region edit truncated peer meta")
				}
				peers = append(peers, PeerMeta{StoreID: storeID, PeerID: peerID})
			}
			edit.Region = &RegionEdit{
				Meta: RegionMeta{
					ID:       regionID,
					StartKey: append([]byte(nil), start...),
					EndKey:   append([]byte(nil), end...),
					Epoch: RegionEpoch{
						Version:     version,
						ConfVersion: confVer,
					},
					Peers: append([]PeerMeta(nil), peers...),
					State: state,
				},
			}
		}
	}
	return edit, nil
}

func appendBytes(dst []byte, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func readBytes(data []byte) ([]byte, int) {
	length, n := binary.Uvarint(data)
	pos := n
	end := pos + int(length)
	if n <= 0 || end > len(data) {
		return nil, len(data)
	}
	return data[pos:end], n + int(length)
}
