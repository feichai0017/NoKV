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
	SessionNamespaceEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool
	RememberKey(key []byte, present bool)
	RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID)
	RememberEmptySessionNamespace(mount fsmeta.MountIdentity, inode fsmeta.InodeID)
}

func DirectoryFactKey(mount fsmeta.MountIdentity, inode fsmeta.InodeID) string {
	return scopedInodeFactKey(mount, inode, "dir")
}

// Session namespace facts intentionally use MountKeyID rather than MountID.
// Raw session-key inspection exposes the stable mount key id, so this cache
// must use the same identity or KeyState could miss a recorded empty session
// namespace.
func SessionNamespaceFactKey(mount fsmeta.MountIdentity, inode fsmeta.InodeID) string {
	buf := make([]byte, 0, 40)
	buf = append(buf, "session"...)
	buf = append(buf, '#')
	buf = strconv.AppendUint(buf, uint64(mount.MountKeyID), 10)
	buf = append(buf, '#')
	buf = strconv.AppendUint(buf, uint64(inode), 10)
	return string(buf)
}

func scopedInodeFactKey(mount fsmeta.MountIdentity, inode fsmeta.InodeID, class string) string {
	buf := make([]byte, 0, len(mount.MountID)+48)
	buf = append(buf, class...)
	buf = append(buf, '#')
	buf = append(buf, mount.MountID...)
	buf = append(buf, '#')
	buf = strconv.AppendUint(buf, uint64(mount.MountKeyID), 10)
	buf = append(buf, '#')
	buf = strconv.AppendUint(buf, uint64(inode), 10)
	return string(buf)
}

func RememberOperationFacts(known map[string]bool, emptyDirs map[string]struct{}, emptySessions map[string]struct{}, op compile.MaterializedOp) error {
	delta := op.Delta
	for _, effect := range op.Effects {
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
		rememberSessionFactMutation(emptySessions, delta.Authority, effect)
	}
	if delta.Kind != fsmeta.OperationCreate || len(delta.Plan.MutateKeys) < 2 {
		return nil
	}
	inodeKey := delta.Plan.MutateKeys[1]
	for _, effect := range op.Effects {
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
		} else {
			RememberEmptySessionNamespaceFact(emptySessions, fsmeta.MountIdentity{
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

func RememberEmptySessionNamespaceFact(emptySessions map[string]struct{}, mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if emptySessions == nil || mount.MountID == "" || mount.MountKeyID == 0 || inode == 0 {
		return
	}
	emptySessions[SessionNamespaceFactKey(mount, inode)] = struct{}{}
}

func ForgetEmptySessionNamespaceFact(emptySessions map[string]struct{}, mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if emptySessions == nil || mount.MountID == "" || mount.MountKeyID == 0 || inode == 0 {
		return
	}
	delete(emptySessions, SessionNamespaceFactKey(mount, inode))
}

func SessionNamespaceEmptyForKey(emptySessions map[string]struct{}, key []byte) bool {
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.Kind != fsmeta.KeyKindSession {
		return false
	}
	_, ok = emptySessions[SessionNamespaceFactKey(fsmeta.MountIdentity{
		MountKeyID: parts.MountKeyID,
	}, parts.Inode)]
	return ok
}

func ForgetEmptySessionNamespaceForKey(emptySessions map[string]struct{}, key []byte) {
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.Kind != fsmeta.KeyKindSession {
		return
	}
	delete(emptySessions, SessionNamespaceFactKey(fsmeta.MountIdentity{MountKeyID: parts.MountKeyID}, parts.Inode))
}

func rememberSessionFactMutation(emptySessions map[string]struct{}, scope compile.AuthorityScope, effect compile.EffectPlan) {
	if effect.Kind != compile.EffectPut || emptySessions == nil {
		return
	}
	parts, ok := fsmeta.InspectKey(effect.Key)
	if !ok || parts.Kind != fsmeta.KeyKindSession || parts.MountKeyID != scope.MountKeyID {
		return
	}
	ForgetEmptySessionNamespaceFact(emptySessions, fsmeta.MountIdentity{
		MountID:    scope.Mount,
		MountKeyID: scope.MountKeyID,
	}, parts.Inode)
}
