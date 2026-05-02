package percolator_test

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

type txnModelRead struct {
	key    string
	readTs uint64
	value  string
	found  bool
}

type txnModelTxn struct {
	id        int
	startTs   uint64
	commitTs  uint64
	reads     []txnModelRead
	writes    map[string]txnModelWrite
	committed bool
}

type txnModelWrite struct {
	value    string
	deleted  bool
	lockOnly bool
}

type txnModelVersion struct {
	commitTs uint64
	value    string
	deleted  bool
}

type txnModel struct {
	versions map[string][]txnModelVersion
}

func TestTxnModelGeneratedScheduleIsSerializable(t *testing.T) {
	seeds := percolatorModelEnvInt("NOKV_PERCOLATOR_MODEL_SEEDS", 8)
	steps := percolatorModelEnvInt("NOKV_PERCOLATOR_MODEL_STEPS", 64)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			db := openPercolatorModelDB(t)
			latches := latch.NewManager(64)
			model := newTxnModel()
			history := runGeneratedTxnSchedule(t, db, latches, model, seed, steps)

			require.NoError(t, checkSerializableByTimestamp(history))
			reader := percolator.NewReader(db)
			for _, key := range txnModelKeys() {
				assertReaderMatchesTxnModel(t, reader, model, key, uint64(steps*20+10_000))
			}
		})
	}
}

func openPercolatorModelDB(t *testing.T) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "db")
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func runGeneratedTxnSchedule(t *testing.T, db *NoKV.DB, latches *latch.Manager, model *txnModel, seed int64, steps int) []txnModelTxn {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	reader := percolator.NewReader(db)
	history := make([]txnModelTxn, 0, steps)
	nextStartTs := uint64(10)
	for step := range steps {
		startTs := nextStartTs
		commitTs := startTs + uint64(1+rng.Intn(3))
		nextStartTs = commitTs + uint64(1+rng.Intn(3))

		mutations := generatedTxnMutations(rng, step)
		txn := txnModelTxn{
			id:       step,
			startTs:  startTs,
			commitTs: commitTs,
			reads:    readTxnSnapshot(t, reader, mutations, startTs),
			writes:   writesFromMutations(mutations),
		}
		for _, read := range txn.reads {
			assertReadMatchesTxnModel(t, model, read)
		}

		errs := percolator.Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  mutations[0].GetKey(),
			StartVersion: startTs,
			LockTtl:      3000,
		})
		require.Empty(t, errs, "seed=%d step=%d startTs=%d commitTs=%d", seed, step, startTs, commitTs)

		rollback := rng.Intn(100) < 25
		if rollback {
			require.Nil(t, percolator.BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
				Keys:         mutationKeys(mutations),
				StartVersion: startTs,
			}))
			history = append(history, txn)
			assertAllKeysMatchTxnModel(t, reader, model, commitTs+1)
			continue
		}

		require.Nil(t, percolator.Commit(db, latches, &kvrpcpb.CommitRequest{
			Keys:          mutationKeys(mutations),
			StartVersion:  startTs,
			CommitVersion: commitTs,
		}))
		require.Nil(t, percolator.Commit(db, latches, &kvrpcpb.CommitRequest{
			Keys:          mutationKeys(mutations),
			StartVersion:  startTs,
			CommitVersion: commitTs,
		}))
		txn.committed = true
		model.apply(txn)
		history = append(history, txn)
		assertAllKeysMatchTxnModel(t, reader, model, commitTs+1)
	}
	return history
}

func generatedTxnMutations(rng *rand.Rand, step int) []*kvrpcpb.Mutation {
	keys := txnModelKeys()
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	count := 1 + rng.Intn(3)
	mutations := make([]*kvrpcpb.Mutation, 0, count)
	for i := range count {
		key := []byte(keys[i])
		switch roll := rng.Intn(100); {
		case roll < 75:
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:    kvrpcpb.Mutation_Put,
				Key:   key,
				Value: fmt.Appendf(nil, "value-%03d-%s", step, keys[i]),
			})
		default:
			mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: key})
		}
	}
	return mutations
}

func readTxnSnapshot(t *testing.T, reader *percolator.Reader, mutations []*kvrpcpb.Mutation, readTs uint64) []txnModelRead {
	t.Helper()
	reads := make([]txnModelRead, 0, len(mutations))
	seen := make(map[string]struct{})
	for _, mutation := range mutations {
		key := string(mutation.GetKey())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		value, _, err := reader.GetValue(mutation.GetKey(), readTs)
		read := txnModelRead{key: key, readTs: readTs}
		if err == nil {
			read.value = string(value)
			read.found = true
		} else {
			require.True(t, errors.Is(err, utils.ErrKeyNotFound), "unexpected read error for key=%s ts=%d: %v", key, readTs, err)
		}
		reads = append(reads, read)
	}
	return reads
}

func writesFromMutations(mutations []*kvrpcpb.Mutation) map[string]txnModelWrite {
	writes := make(map[string]txnModelWrite, len(mutations))
	for _, mutation := range mutations {
		key := string(mutation.GetKey())
		switch mutation.GetOp() {
		case kvrpcpb.Mutation_Put:
			writes[key] = txnModelWrite{value: string(mutation.GetValue())}
		case kvrpcpb.Mutation_Delete:
			writes[key] = txnModelWrite{deleted: true}
		case kvrpcpb.Mutation_Lock:
			writes[key] = txnModelWrite{lockOnly: true}
		}
	}
	return writes
}

func checkSerializableByTimestamp(history []txnModelTxn) error {
	committed := append([]txnModelTxn(nil), history...)
	sort.Slice(committed, func(i, j int) bool {
		if committed[i].commitTs != committed[j].commitTs {
			return committed[i].commitTs < committed[j].commitTs
		}
		return committed[i].id < committed[j].id
	})
	model := newTxnModel()
	for _, txn := range committed {
		if txn.committed {
			model.apply(txn)
		}
	}
	for _, txn := range history {
		for _, read := range txn.reads {
			value, found := model.visible(read.key, read.readTs)
			if found != read.found || value != read.value {
				return fmt.Errorf("txn %d read key=%s ts=%d got=(%v,%q) serial=(%v,%q)", txn.id, read.key, read.readTs, read.found, read.value, found, value)
			}
		}
	}
	return nil
}

func newTxnModel() *txnModel {
	return &txnModel{versions: make(map[string][]txnModelVersion)}
}

func (m *txnModel) apply(txn txnModelTxn) {
	for key, write := range txn.writes {
		if write.lockOnly {
			continue
		}
		m.versions[key] = append(m.versions[key], txnModelVersion{
			commitTs: txn.commitTs,
			value:    write.value,
			deleted:  write.deleted,
		})
	}
	for key := range txn.writes {
		sort.Slice(m.versions[key], func(i, j int) bool {
			return m.versions[key][i].commitTs < m.versions[key][j].commitTs
		})
	}
}

func (m *txnModel) visible(key string, readTs uint64) (string, bool) {
	versions := m.versions[key]
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		if version.commitTs > readTs {
			continue
		}
		if version.deleted {
			return "", false
		}
		return version.value, true
	}
	return "", false
}

func assertReaderMatchesTxnModel(t *testing.T, reader *percolator.Reader, model *txnModel, key string, readTs uint64) {
	t.Helper()
	got, _, err := reader.GetValue([]byte(key), readTs)
	want, found := model.visible(key, readTs)
	if !found {
		require.ErrorIs(t, err, utils.ErrKeyNotFound, "key=%s ts=%d", key, readTs)
		return
	}
	require.NoError(t, err, "key=%s ts=%d", key, readTs)
	require.Equal(t, want, string(got), "key=%s ts=%d", key, readTs)
}

func assertAllKeysMatchTxnModel(t *testing.T, reader *percolator.Reader, model *txnModel, readTs uint64) {
	t.Helper()
	for _, key := range txnModelKeys() {
		assertReaderMatchesTxnModel(t, reader, model, key, readTs)
	}
}

func assertReadMatchesTxnModel(t *testing.T, model *txnModel, read txnModelRead) {
	t.Helper()
	want, found := model.visible(read.key, read.readTs)
	require.Equal(t, found, read.found, "key=%s ts=%d", read.key, read.readTs)
	require.Equal(t, want, read.value, "key=%s ts=%d", read.key, read.readTs)
}

func mutationKeys(mutations []*kvrpcpb.Mutation) [][]byte {
	keys := make([][]byte, 0, len(mutations))
	for _, mutation := range mutations {
		keys = append(keys, append([]byte(nil), mutation.GetKey()...))
	}
	return keys
}

func txnModelKeys() []string {
	return []string{"key-a", "key-b", "key-c", "key-d", "key-e"}
}

func percolatorModelEnvInt(name string, fallback int) int {
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
