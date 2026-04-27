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
	default:
		return fmt.Errorf("unsupported manifest edit type: %d", edit.Type)
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
	default:
		return Edit{}, fmt.Errorf("unsupported manifest edit type: %d", edit.Type)
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
