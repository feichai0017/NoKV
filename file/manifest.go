package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *Options
	f                         *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int
	manifest                  *Manifest
}

// Manifest corekv 元数据状态维护
type Manifest struct {
	Levels    []levelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}
type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's
}

//TableMeta sst 的一些元信息
type TableMeta struct {
	ID       uint64
	Checksum []byte
}

// OpenManifestFile 打开manifest文件
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{lock: sync.Mutex{}, opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	// 如果打开失败 则尝试创建一个新的 manifest file
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		m := createManifest()
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		utils.CondPanic(netCreations == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))
		if err != nil {
			return mf, err
		}
		mf.f = fp
		f = fp
		mf.manifest = m
		return mf, nil
	}

	// 如果打开 则对manifest文件重放
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		_ = f.Close()
		return mf, err
	}
	// Truncate file so we don't have a half-written entry at the end.
	if err := f.Truncate(truncOffset); err != nil {
		_ = f.Close()
		return mf, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return mf, err
	}
	mf.f = f
	mf.manifest = manifest
	return mf, nil
}

// ReplayManifestFile 对已经存在的manifest文件重新应用所有状态变更
func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	r := &bufReader{reader: bufio.NewReader(fp)}
	var magicBuf [8]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != uint32(utils.MagicVersion) {
		return &Manifest{}, 0,
			fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, utils.MagicVersion)
	}

	build := createManifest()
	var offset int64
	for {
		offset = r.count
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, err
		}

		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}

	return build, offset, err
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func applyManifestChange(build *Manifest, tc *pb.ManifestChange) error {
	switch tc.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.Tables[tc.Id] = TableManifest{
			Level:    uint8(tc.Level),
			Checksum: append([]byte{}, tc.Checksum...),
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, levelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case pb.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

type bufReader struct {
	reader *bufio.Reader
	count  int64
}

func (r *bufReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.count += int64(n)
	return
}

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// Must be called while appendLock is held.
func (mf *ManifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, nextCreations, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = nextCreations
	mf.manifest.Deletions = 0
	mf.f = fp
	return nil
}

func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	// We explicitly sync.
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	netCreations := len(m.Tables)
	changes := m.asChanges()
	set := pb.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil
}

// Close 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// AddChanges 对外暴露的写比那更丰富
func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}
func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{Changes: changesParam}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// TODO 锁粒度可以优化
	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

// AddTableMeta 存储level表到manifest的level中
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

// RevertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			filename := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(filename); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// GetManifest manifest
func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}