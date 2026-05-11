package peras

import (
	"bytes"
	"strconv"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// PredicateIndex is the holder-local admission cache used before an operation
// enters the visible Peras log. Unknown means "take the ordinary transaction
// path"; it is never treated as permission.
type PredicateIndex interface {
	KeyState(key []byte) (present bool, known bool)
	DirectoryEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool
	RememberKey(key []byte, present bool)
	RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID)
}

func DirectoryFactKey(mount fsmeta.MountIdentity, inode fsmeta.InodeID) string {
	buf := make([]byte, 0, len(mount.MountID)+48)
	buf = append(buf, mount.MountID...)
	buf = append(buf, '#')
	buf = strconv.AppendUint(buf, uint64(mount.MountKeyID), 10)
	buf = append(buf, '#')
	buf = strconv.AppendUint(buf, uint64(inode), 10)
	return string(buf)
}

func RememberDeltaFacts(known map[string]bool, emptyDirs map[string]struct{}, delta compile.SemanticDelta) error {
	for _, effect := range delta.WriteEffects {
		if len(effect.Key) == 0 {
			return ErrInvalidPerasSegment
		}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ErrInvalidPerasSegment
			}
			known[string(effect.Key)] = true
		case compile.EffectDelete:
			known[string(effect.Key)] = false
		default:
			return ErrInvalidPerasSegment
		}
	}
	if delta.Kind != fsmeta.OperationCreate || len(delta.Plan.MutateKeys) < 2 {
		return nil
	}
	inodeKey := delta.Plan.MutateKeys[1]
	for _, effect := range delta.WriteEffects {
		if effect.Kind != compile.EffectPut || !bytes.Equal(effect.Key, inodeKey) {
			continue
		}
		inode, err := fsmeta.DecodeInodeValue(effect.Value)
		if err != nil {
			return err
		}
		if inode.Type == fsmeta.InodeTypeDirectory {
			RememberEmptyDirectoryFact(emptyDirs, fsmeta.MountIdentity{
				MountID:    delta.Authority.Mount,
				MountKeyID: delta.Authority.MountKeyID,
			}, inode.Inode)
		}
		return nil
	}
	return nil
}

func RememberEmptyDirectoryFact(emptyDirs map[string]struct{}, mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if mount.MountID == "" || mount.MountKeyID == 0 || inode == 0 {
		return
	}
	emptyDirs[DirectoryFactKey(mount, inode)] = struct{}{}
}
