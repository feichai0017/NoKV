package contract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestFSMetaExecutorModelContract(t *testing.T) {
	seeds := envInt("NOKV_CONTRACT_SEEDS", 16)
	steps := envInt("NOKV_CONTRACT_STEPS", 80)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			model := NewModel("vol")
			runner := newVersionedRunner()
			executor, err := fsmetaexec.New(runner, fsmetaexec.WithClock(func() time.Time {
				return time.Unix(0, model.NowUnixNs)
			}))
			require.NoError(t, err)

			ops := generateScript(seed, steps)
			err = Run(context.Background(), executor, model, ops)
			require.NoError(t, err, "seed=%d steps=%d", seed, steps)
		})
	}
}

func generateScript(seed int64, steps int) []Operation {
	rng := rand.New(rand.NewSource(seed))
	model := NewModel("vol")
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	sessions := []fsmeta.SessionID{"writer-a", "writer-b", "writer-c"}
	nextInode := fsmeta.InodeID(10)
	nextSnapshotRef := 0
	syntheticSnapshotVersion := uint64(10_000)
	ops := make([]Operation, 0, steps)
	for range steps {
		op := chooseOperation(rng, model, names, sessions, &nextInode, &nextSnapshotRef)
		ops = append(ops, op)
		if op.Kind == OpSnapshotSubtree {
			_ = model.ApplySnapshot(op, fsmeta.SnapshotSubtreeToken{
				Mount:       op.Mount,
				RootInode:   op.Parent,
				ReadVersion: syntheticSnapshotVersion,
			})
			syntheticSnapshotVersion++
			continue
		}
		_ = model.Apply(op)
	}
	return ops
}

func chooseOperation(rng *rand.Rand, model *Model, names []string, sessions []fsmeta.SessionID, nextInode *fsmeta.InodeID, nextSnapshotRef *int) Operation {
	existing := model.ExistingDentries()
	files := model.ExistingFileDentries()
	liveSessions := model.ExistingSessions()
	if len(existing) < 2 && rng.Intn(100) < 60 {
		return createOperation(rng, model, names, nextInode)
	}
	switch rng.Intn(100) {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
		return createOperation(rng, model, names, nextInode)
	case 10, 11, 12, 13, 14, 15, 16, 17:
		return lookupOperation(rng, model, names)
	case 18, 19, 20, 21, 22, 23, 24, 25:
		return readDirPlusOperation(rng, model, names)
	case 26, 27, 28, 29, 30, 31, 32, 33:
		return renameOperation(rng, model, names)
	case 34, 35, 36, 37, 38, 39, 40:
		return linkOperation(rng, model, names)
	case 41, 42, 43, 44, 45, 46, 47:
		return unlinkOperation(rng, model, names)
	case 48, 49, 50, 51, 52, 53:
		return updateOperation(rng, model, names)
	case 54, 55, 56, 57, 58:
		if len(files) == 0 {
			return createOperation(rng, model, names, nextInode)
		}
		record := files[rng.Intn(len(files))]
		return Operation{
			Kind:      OpOpenWriteSession,
			Mount:     model.Mount,
			Inode:     record.Inode,
			Session:   sessions[rng.Intn(len(sessions))],
			ExpiresNs: model.NowUnixNs + int64(1+rng.Intn(10))*int64(time.Second),
		}
	case 59, 60, 61, 62:
		if len(liveSessions) == 0 {
			return readDirPlusOperation(rng, model, names)
		}
		session := liveSessions[rng.Intn(len(liveSessions))]
		return Operation{
			Kind:      OpHeartbeatSession,
			Mount:     model.Mount,
			Inode:     session.Inode,
			Session:   session.Session,
			ExpiresNs: model.NowUnixNs + int64(10+rng.Intn(10))*int64(time.Second),
		}
	case 63, 64, 65:
		if len(liveSessions) == 0 {
			return lookupOperation(rng, model, names)
		}
		session := liveSessions[rng.Intn(len(liveSessions))]
		return Operation{Kind: OpCloseSession, Mount: model.Mount, Session: session.Session}
	case 66, 67:
		return Operation{
			Kind:      OpAdvanceTime,
			Mount:     model.Mount,
			AdvanceNs: int64(1+rng.Intn(6)) * int64(time.Second),
		}
	case 68, 69:
		return Operation{Kind: OpExpireSessions, Mount: model.Mount, Limit: 16}
	case 70, 71, 72:
		ref := *nextSnapshotRef
		*nextSnapshotRef++
		return Operation{Kind: OpSnapshotSubtree, Mount: model.Mount, Parent: model.Root, SnapshotRef: ref}
	default:
		return readDirPlusOperation(rng, model, names)
	}
}

func createOperation(rng *rand.Rand, model *Model, names []string, nextInode *fsmeta.InodeID) Operation {
	name := names[rng.Intn(len(names))]
	if rng.Intn(100) < 70 {
		if free := freeNames(model, names); len(free) > 0 {
			name = free[rng.Intn(len(free))]
		}
	}
	inode := *nextInode
	*nextInode++
	if rng.Intn(100) < 15 && len(model.inodes) > 1 {
		for existing := range model.inodes {
			if existing != model.Root {
				inode = existing
				break
			}
		}
	}
	typ := fsmeta.InodeTypeFile
	if rng.Intn(100) < 20 {
		typ = fsmeta.InodeTypeDirectory
	}
	return Operation{
		Kind:   OpCreate,
		Mount:  model.Mount,
		Parent: model.Root,
		Name:   name,
		Inode:  inode,
		Type:   typ,
		Size:   uint64(1 + rng.Intn(4096)),
		Mode:   0o600 + uint32(rng.Intn(0o177)),
	}
}

func lookupOperation(rng *rand.Rand, model *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 70 {
		name = existing[rng.Intn(len(existing))].Name
	}
	return Operation{Kind: OpLookup, Mount: model.Mount, Parent: model.Root, Name: name}
}

func readDirPlusOperation(rng *rand.Rand, model *Model, names []string) Operation {
	startAfter := ""
	if rng.Intn(100) < 25 {
		startAfter = names[rng.Intn(len(names))]
	}
	snapshotRef := -1
	refs := model.KnownSnapshotRefs()
	if len(refs) > 0 && rng.Intn(100) < 35 {
		snapshotRef = refs[rng.Intn(len(refs))]
	}
	return Operation{
		Kind:        OpReadDirPlus,
		Mount:       model.Mount,
		Parent:      model.Root,
		StartAfter:  startAfter,
		Limit:       uint32(1 + rng.Intn(6)),
		SnapshotRef: snapshotRef,
	}
}

func renameOperation(rng *rand.Rand, model *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 80 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if rng.Intn(100) < 70 {
		if free := freeNames(model, names); len(free) > 0 {
			toName = free[rng.Intn(len(free))]
		}
	}
	return Operation{
		Kind:       OpRenameSubtree,
		Mount:      model.Mount,
		FromParent: model.Root,
		FromName:   fromName,
		ToParent:   model.Root,
		ToName:     toName,
	}
}

func linkOperation(rng *rand.Rand, model *Model, names []string) Operation {
	fromName := names[rng.Intn(len(names))]
	if files := model.ExistingFileDentries(); len(files) > 0 && rng.Intn(100) < 85 {
		fromName = files[rng.Intn(len(files))].Name
	} else if existing := model.ExistingDentries(); len(existing) > 0 {
		fromName = existing[rng.Intn(len(existing))].Name
	}
	toName := names[rng.Intn(len(names))]
	if rng.Intn(100) < 75 {
		if free := freeNames(model, names); len(free) > 0 {
			toName = free[rng.Intn(len(free))]
		}
	}
	return Operation{
		Kind:       OpLink,
		Mount:      model.Mount,
		FromParent: model.Root,
		FromName:   fromName,
		ToParent:   model.Root,
		ToName:     toName,
	}
}

func unlinkOperation(rng *rand.Rand, model *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 80 {
		name = existing[rng.Intn(len(existing))].Name
	}
	return Operation{Kind: OpUnlink, Mount: model.Mount, Parent: model.Root, Name: name}
}

func updateOperation(rng *rand.Rand, model *Model, names []string) Operation {
	name := names[rng.Intn(len(names))]
	inode := fsmeta.InodeID(9999)
	if existing := model.ExistingDentries(); len(existing) > 0 && rng.Intn(100) < 85 {
		record := existing[rng.Intn(len(existing))]
		name = record.Name
		inode = record.Inode
	}
	return Operation{
		Kind:   OpUpdateInode,
		Mount:  model.Mount,
		Parent: model.Root,
		Name:   name,
		Inode:  inode,
		Size:   uint64(1 + rng.Intn(8192)),
		Mode:   0o600 + uint32(rng.Intn(0o177)),
	}
}

func freeNames(model *Model, names []string) []string {
	out := make([]string, 0, len(names))
	used := make(map[string]struct{})
	for _, record := range model.ExistingDentries() {
		used[record.Name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := used[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

type versionedRunner struct {
	nextTS uint64
	data   map[string][]versionedValue
}

type versionedValue struct {
	version uint64
	value   []byte
	deleted bool
}

func newVersionedRunner() *versionedRunner {
	return &versionedRunner{
		nextTS: 1,
		data:   make(map[string][]versionedValue),
	}
}

func (r *versionedRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *versionedRunner) Get(_ context.Context, key []byte, version uint64) ([]byte, bool, error) {
	value, ok := r.visible(key, version)
	return value, ok, nil
}

func (r *versionedRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.visible(key, version); ok {
			out[string(key)] = value
		}
	}
	return out, nil
}

func (r *versionedRunner) Scan(_ context.Context, startKey []byte, limit uint32, version uint64) ([]fsmetaexec.KV, error) {
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		raw := []byte(key)
		if bytes.Compare(raw, startKey) < 0 {
			continue
		}
		if _, ok := r.visible(raw, version); ok {
			keys = append(keys, append([]byte(nil), raw...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	out := make([]fsmetaexec.KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		value, _ := r.visible(key, version)
		out = append(out, fsmetaexec.KV{
			Key:   append([]byte(nil), key...),
			Value: value,
		})
	}
	return out, nil
}

func (r *versionedRunner) Mutate(_ context.Context, _ []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, _ uint64) error {
	for _, mut := range mutations {
		if mut.GetAssertionNotExist() {
			if _, ok := r.visible(mut.GetKey(), startVersion); ok {
				return fsmeta.ErrExists
			}
		}
	}
	for _, mut := range mutations {
		key := string(mut.GetKey())
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			r.data[key] = append(r.data[key], versionedValue{
				version: commitVersion,
				value:   append([]byte(nil), mut.GetValue()...),
			})
		case kvrpcpb.Mutation_Delete:
			r.data[key] = append(r.data[key], versionedValue{
				version: commitVersion,
				deleted: true,
			})
		default:
			return fsmeta.ErrInvalidRequest
		}
	}
	return nil
}

func (r *versionedRunner) visible(key []byte, version uint64) ([]byte, bool) {
	versions := r.data[string(key)]
	for i := len(versions) - 1; i >= 0; i-- {
		candidate := versions[i]
		if candidate.version > version {
			continue
		}
		if candidate.deleted {
			return nil, false
		}
		return append([]byte(nil), candidate.value...), true
	}
	return nil, false
}
