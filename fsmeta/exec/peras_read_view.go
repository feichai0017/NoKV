package exec

import (
	"bytes"
	"context"
	"maps"
	"sort"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (e *Executor) perasOverlay() PerasOverlayReader {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	overlay, ok := e.perasCommitter.(PerasOverlayReader)
	if !ok {
		return nil
	}
	return overlay
}

func (e *Executor) flushPeras(ctx context.Context) error {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	flusher, ok := e.perasCommitter.(PerasFlusher)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return flusher.FlushDurable(context.WithoutCancel(ctx))
}

func (e *Executor) flushPerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	if len(scopes) == 0 {
		return e.flushPeras(ctx)
	}
	if scoped, ok := e.perasCommitter.(PerasAuthorityFlusher); ok {
		for _, scope := range scopes {
			if authorityScopeEmpty(scope) {
				return e.flushPeras(ctx)
			}
			if err := scoped.FlushAuthority(context.WithoutCancel(ctx), scope); err != nil {
				return err
			}
		}
		return nil
	}
	return e.flushPeras(ctx)
}

func (e *Executor) retirePerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil || e.perasAuthority == nil {
		return nil
	}
	retirer, ok := e.perasAuthority.(PerasAuthorityRetirer)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return retirer.RetirePerasAuthority(context.WithoutCancel(ctx), scopes...)
}

func (e *Executor) drainPerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil {
		return nil
	}
	if e.perasCommitter != nil && e.perasAuthority != nil {
		drainer, drainOK := e.perasCommitter.(PerasAuthorityDrainer)
		retirer, retireOK := e.perasAuthority.(PerasAuthorityRetirer)
		if drainOK && retireOK {
			if ctx == nil {
				ctx = context.Background()
			}
			return drainer.DrainAuthority(context.WithoutCancel(ctx), retirer, scopes...)
		}
	}
	if err := e.flushPerasAuthority(ctx, scopes...); err != nil {
		return err
	}
	return e.retirePerasAuthority(ctx, scopes...)
}

func authorityScopeEmpty(scope compile.AuthorityScope) bool {
	return scope.Mount == "" || scope.MountKeyID == 0
}

func (e *Executor) perasOverlayGet(key []byte) ([]byte, bool, bool) {
	overlay := e.perasOverlay()
	if overlay == nil {
		return nil, false, false
	}
	return overlay.GetPerasOverlay(key)
}

func (e *Executor) getMergedValue(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	if value, deleted, ok := e.perasOverlayGet(key); ok {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	return e.runner.Get(ctx, key, version)
}

type perasReadView struct {
	executor    *Executor
	ctx         context.Context
	version     uint64
	haveVersion bool
	observed    map[string]perasObservedValue
}

func (e *Executor) newPerasReadView(ctx context.Context) *perasReadView {
	if ctx == nil {
		ctx = context.Background()
	}
	return &perasReadView{
		executor: e,
		ctx:      ctx,
		observed: make(map[string]perasObservedValue),
	}
}

func (v *perasReadView) get(key []byte) ([]byte, bool, error) {
	if v == nil || v.executor == nil {
		return nil, false, fsmeta.ErrInvalidRequest
	}
	if value, deleted, ok := v.executor.perasOverlayGet(key); ok {
		if deleted {
			v.remember(key, nil, false, compile.ReadSourceOverlay, 0)
			return nil, false, nil
		}
		v.remember(key, value, true, compile.ReadSourceOverlay, 0)
		return value, true, nil
	}
	if index := v.executor.perasPredicateIndex(); index != nil {
		present, known := index.KeyState(key)
		if known && !present {
			v.remember(key, nil, false, compile.ReadSourceUnknown, 0)
			return nil, false, nil
		}
	}
	if !v.haveVersion {
		version, err := v.executor.reserveReadVersion(v.ctx)
		if err != nil {
			return nil, false, err
		}
		v.version = version
		v.haveVersion = true
	}
	value, ok, err := v.executor.runner.Get(v.ctx, key, v.version)
	if err != nil {
		return nil, false, err
	}
	v.remember(key, value, ok, compile.ReadSourceBase, v.version)
	return value, ok, nil
}

type perasObservedValue struct {
	value   []byte
	present bool
	source  compile.ReadSource
	version uint64
}

func (v *perasReadView) remember(key, value []byte, present bool, source compile.ReadSource, version uint64) {
	if v == nil {
		return
	}
	v.observed[string(key)] = perasObservedValue{
		value:   cloneBytes(value),
		present: present,
		source:  source,
		version: version,
	}
}

func (v *perasReadView) materializePerasOp(delta compile.SemanticDelta, effects []compile.WriteEffect) compile.MaterializedOp {
	if v == nil || v.executor == nil {
		return compile.MaterializeDelta(concretePerasDelta(delta, effects), nil)
	}
	index := v.executor.perasPredicateIndex()
	allowAbsentDowngrade := perasDeltaAllowsAbsentObservedValue(delta)
	seen := make(map[string]struct{}, len(delta.ReadPredicates)+len(v.observed))
	for i := range delta.ReadPredicates {
		predicate := &delta.ReadPredicates[i]
		if predicate.Kind == compile.PredicatePrefixScan {
			continue
		}
		seen[string(predicate.Key)] = struct{}{}
		if observed, ok := v.observed[string(predicate.Key)]; ok {
			applyPerasObservedPredicate(predicate, observed)
			continue
		}
		if allowAbsentDowngrade &&
			predicate.Kind == compile.PredicateObservedValue &&
			v.executor.perasNotExistsKnown(delta.Authority, predicate.Key, index) {
			predicate.Kind = compile.PredicateNotExists
			predicate.ExpectedValue = nil
			predicate.HasExpectedValue = false
			predicate.RuntimeChecked = true
		}
	}
	if len(v.observed) == 0 {
		return compile.MaterializeDelta(concretePerasDelta(delta, effects), nil)
	}
	keys := make([]string, 0, len(v.observed))
	for key := range v.observed {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		observed := v.observed[key]
		predicate := compile.Predicate{Key: []byte(key)}
		applyPerasObservedPredicate(&predicate, observed)
		delta.ReadPredicates = append(delta.ReadPredicates, predicate)
	}
	return compile.MaterializeDelta(concretePerasDelta(delta, effects), v.predicateProofs())
}

func (v *perasReadView) predicateProofs() []compile.PredicateProof {
	if v == nil || len(v.observed) == 0 {
		return nil
	}
	keys := make([]string, 0, len(v.observed))
	for key := range v.observed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	proofs := make([]compile.PredicateProof, 0, len(keys))
	for _, key := range keys {
		observed := v.observed[key]
		proof := compile.PredicateProof{
			Key:     []byte(key),
			Present: observed.present,
			Value:   cloneBytes(observed.value),
			Version: observed.version,
			Source:  observed.source,
		}
		proof.Digest = compile.PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source)
		proofs = append(proofs, proof)
	}
	return proofs
}

func applyPerasObservedPredicate(predicate *compile.Predicate, observed perasObservedValue) {
	if predicate == nil {
		return
	}
	if !observed.present {
		predicate.Kind = compile.PredicateNotExists
		predicate.ExpectedValue = nil
		predicate.HasExpectedValue = false
		predicate.RuntimeChecked = true
		return
	}
	predicate.Kind = compile.PredicateObservedValue
	predicate.ExpectedValue = cloneBytes(observed.value)
	predicate.HasExpectedValue = true
	predicate.RuntimeChecked = true
}

func (v *perasReadView) readDentry(key []byte) (fsmeta.DentryRecord, error) {
	value, ok, err := v.get(key)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

func (v *perasReadView) readInode(mount fsmeta.MountIdentity, inodeID fsmeta.InodeID) (fsmeta.InodeRecord, bool, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := v.get(key)
	if err != nil || !ok {
		return fsmeta.InodeRecord{}, ok, err
	}
	inode, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (v *perasReadView) readSession(key []byte) (fsmeta.SessionRecord, bool, error) {
	value, ok, err := v.get(key)
	if err != nil || !ok {
		return fsmeta.SessionRecord{}, ok, err
	}
	session, err := fsmeta.DecodeSessionValue(value)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	return session, true, nil
}

func (e *Executor) batchGetMergedValues(ctx context.Context, keys [][]byte, version uint64, includeOverlay bool) (map[string][]byte, error) {
	if !includeOverlay || e.perasOverlay() == nil {
		return e.runner.BatchGet(ctx, keys, version)
	}
	values := make(map[string][]byte, len(keys))
	missing := make([][]byte, 0, len(keys))
	for _, key := range keys {
		value, deleted, ok := e.perasOverlayGet(key)
		switch {
		case ok && !deleted:
			values[string(key)] = value
		case ok && deleted:
		default:
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return values, nil
	}
	base, err := e.runner.BatchGet(ctx, missing, version)
	if err != nil {
		return nil, err
	}
	maps.Copy(values, base)
	return values, nil
}

func (e *Executor) mergePerasOverlayScan(kvs []KV, start []byte, limit uint32) []KV {
	overlay := e.perasOverlay()
	if overlay == nil || limit == 0 {
		return kvs
	}
	overlayKVs := overlay.ScanPerasOverlay(start, limit)
	if len(overlayKVs) == 0 {
		return kvs
	}
	out := make([]KV, 0, int(limit))
	base, peras := 0, 0
	for len(out) < int(limit) && (base < len(kvs) || peras < len(overlayKVs)) {
		switch {
		case base >= len(kvs):
			out = appendOverlayScanKV(out, overlayKVs[peras])
			peras++
		case peras >= len(overlayKVs):
			out = append(out, kvs[base])
			base++
		default:
			cmp := bytes.Compare(kvs[base].Key, overlayKVs[peras].Key)
			switch {
			case cmp < 0:
				out = append(out, kvs[base])
				base++
			case cmp > 0:
				out = appendOverlayScanKV(out, overlayKVs[peras])
				peras++
			default:
				out = appendOverlayScanKV(out, overlayKVs[peras])
				base++
				peras++
			}
		}
	}
	return out
}

func appendOverlayScanKV(out []KV, kv fsperas.OverlayKV) []KV {
	if kv.Delete {
		return out
	}
	return append(out, KV{Key: kv.Key, Value: kv.Value})
}
