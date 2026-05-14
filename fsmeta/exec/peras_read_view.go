// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"sort"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/proof"
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
	retirer, ok := e.perasAuthority.(fsperas.AuthorityRetirer)
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
		retirer, retireOK := e.perasAuthority.(fsperas.AuthorityRetirer)
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

func (e *Executor) readPerasProgram(program compile.ReadProgram) ([]byte, bool, bool) {
	if len(program.Key) == 0 {
		return nil, false, false
	}
	return e.perasOverlayGet(program.Key)
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

func (e *Executor) getMergedProgramValue(ctx context.Context, program compile.ReadProgram, version uint64) ([]byte, bool, error) {
	if value, deleted, ok := e.readPerasProgram(program); ok {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	return e.runner.Get(ctx, program.Key, version)
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
			v.remember(key, nil, false, proof.ReadSourceOverlay, 0)
			return nil, false, nil
		}
		v.remember(key, value, true, proof.ReadSourceOverlay, 0)
		return value, true, nil
	}
	if index := v.executor.perasPredicateIndex(); index != nil {
		present, known := index.KeyState(key)
		if known && !present {
			v.remember(key, nil, false, proof.ReadSourceUnknown, 0)
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
	v.remember(key, value, ok, proof.ReadSourceBase, v.version)
	return value, ok, nil
}

type perasObservedValue struct {
	value   []byte
	present bool
	source  proof.ReadSource
	version uint64
}

func (v *perasReadView) remember(key, value []byte, present bool, source proof.ReadSource, version uint64) {
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

func (v *perasReadView) materializePerasCompiledOp(compiled compile.CompiledOp, effects []compile.WriteEffect) (compile.MaterializedOp, error) {
	if v == nil || v.executor == nil {
		return compile.MaterializeCompiledOpWithEvidence(compiled, effects, compile.PredicateEvidence{}, nil)
	}
	return compile.MaterializeCompiledOpWithEvidence(compiled, effects, v.predicateEvidenceForDelta(compiled.Delta), nil)
}

func (v *perasReadView) predicateProofs() []proof.PredicateProof {
	if v == nil || len(v.observed) == 0 {
		return nil
	}
	keys := make([]string, 0, len(v.observed))
	for key := range v.observed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	proofs := make([]proof.PredicateProof, 0, len(keys))
	for _, key := range keys {
		observed := v.observed[key]
		proofs = append(proofs, proof.NewPredicateProof([]byte(key), observed.value, observed.present, observed.version, observed.source, proof.ProofFrontier{}))
	}
	return proofs
}

func (v *perasReadView) predicateEvidenceForDelta(delta compile.SemanticDelta) compile.PredicateEvidence {
	if v == nil || v.executor == nil {
		return compile.PredicateEvidence{}
	}
	index := v.executor.perasPredicateIndex()
	allowAbsentDowngrade := perasDeltaAllowsAbsentObservedValue(delta)
	proofs := v.predicateProofs()
	for _, predicate := range delta.ReadPredicates {
		if predicate.Kind == compile.PredicatePrefixScan {
			continue
		}
		if _, ok := v.observed[string(predicate.Key)]; ok {
			continue
		}
		if allowAbsentDowngrade &&
			predicate.Kind == compile.PredicateObservedValue &&
			v.executor.perasNotExistsKnown(delta.Authority, predicate.Key, index) {
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, proof.ProofFrontier{}))
		}
	}
	return compile.PredicateEvidence{
		Proofs: proofs,
	}
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
	program, err := compile.CompileGetAttrReadProgram(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := v.get(program.Key)
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
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.Kind != fsmeta.KeyKindSession {
		return fsmeta.SessionRecord{}, false, fsmeta.ErrInvalidKey
	}
	program, err := compile.CompileReadSessionKeyProgram(fsmeta.MountIdentity{MountKeyID: parts.MountKeyID}, key)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	value, ok, err := v.get(program.Key)
	if err != nil || !ok {
		return fsmeta.SessionRecord{}, ok, err
	}
	session, err := fsmeta.DecodeSessionValue(value)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	return session, true, nil
}

func (e *Executor) batchGetMergedValuesOrdered(ctx context.Context, keys [][]byte, version uint64, includeOverlay bool) ([][]byte, []bool, error) {
	values := make([][]byte, len(keys))
	present := make([]bool, len(keys))

	overlay := e.perasOverlay()
	if !includeOverlay || overlay == nil {
		base, err := e.runner.BatchGet(ctx, keys, version)
		if err != nil {
			return nil, nil, err
		}
		for i, key := range keys {
			value, ok := base[string(key)]
			if ok {
				values[i] = value
				present[i] = true
			}
		}
		return values, present, nil
	}

	missing := make([][]byte, 0, len(keys))
	missingIndexes := make([]int, 0, len(keys))
	for i, key := range keys {
		value, deleted, ok := overlay.GetPerasOverlayView(key)
		switch {
		case ok && !deleted:
			values[i] = value
			present[i] = true
		case ok && deleted:
		default:
			missing = append(missing, key)
			missingIndexes = append(missingIndexes, i)
		}
	}
	if len(missing) == 0 {
		return values, present, nil
	}
	base, err := e.runner.BatchGet(ctx, missing, version)
	if err != nil {
		return nil, nil, err
	}
	for i, key := range missing {
		value, ok := base[string(key)]
		if ok {
			index := missingIndexes[i]
			values[index] = value
			present[index] = true
		}
	}
	return values, present, nil
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
	return mergeOverlayScanRows(kvs, overlayKVs, limit)
}

func (e *Executor) mergePerasDirectoryOverlayScan(kvs []KV, prefix, start []byte, limit uint32) ([]KV, uint32, bool) {
	overlayKVs, usedIndex := e.scanPerasDirectoryOverlayRows(prefix, start, limit)
	if len(overlayKVs) == 0 {
		return kvs, 0, usedIndex
	}
	out := mergeOverlayScanRows(kvs, overlayKVs, limit)
	return out, uint32(len(overlayKVs)), usedIndex
}

func (e *Executor) scanPerasDirectoryOverlayRows(prefix, start []byte, limit uint32) ([]fsperas.OverlayKV, bool) {
	var (
		out       []fsperas.OverlayKV
		startKey  = cloneBytes(start)
		usedIndex bool
	)
	for {
		batch, batchUsedIndex := e.scanPerasDirectoryOverlayBatch(prefix, startKey, limit)
		usedIndex = usedIndex || batchUsedIndex
		if len(batch) == 0 {
			return out, usedIndex
		}
		out = append(out, batch...)
		visible := mergeOverlayScanRows(nil, out, limit)
		if directoryMergeComplete(visible, prefix, limit) || !directoryOverlayScanMayContinue(batch, prefix, limit) {
			return out, usedIndex
		}
		startKey = keyAfter(batch[len(batch)-1].Key)
	}
}

func (e *Executor) scanPerasDirectoryOverlayBatch(prefix, start []byte, limit uint32) ([]fsperas.OverlayKV, bool) {
	overlay := e.perasOverlay()
	if overlay == nil || limit == 0 {
		return nil, false
	}
	var (
		overlayKVs []fsperas.OverlayKV
		usedIndex  bool
	)
	if directoryReader, ok := overlay.(PerasDirectoryOverlayReader); ok {
		overlayKVs = directoryReader.ScanPerasDirectory(prefix, start, limit)
		usedIndex = true
	} else {
		overlayKVs = overlay.ScanPerasOverlay(start, limit)
	}
	if len(overlayKVs) == 0 {
		return nil, usedIndex
	}
	out := overlayKVs[:0]
	for _, row := range overlayKVs {
		if !bytes.HasPrefix(row.Key, prefix) {
			break
		}
		out = append(out, row)
	}
	return out, usedIndex
}

func (e *Executor) scanMergedDirectoryRows(ctx context.Context, plan compile.DirectoryReadPlan, version uint64) ([]KV, uint32, uint32, bool, error) {
	overlayKVs, usedIndex := e.scanPerasDirectoryOverlayRows(plan.Prefix, plan.StartKey, plan.Limit)
	start := cloneBytes(plan.StartKey)
	baseRows := make([]KV, 0, plan.Limit)
	var baseTotal uint32
	for {
		batch, err := e.runner.Scan(ctx, start, plan.Limit, version)
		if err != nil {
			return nil, baseTotal, uint32(len(overlayKVs)), usedIndex, err
		}
		baseTotal += uint32(len(batch))
		baseRows = append(baseRows, batch...)
		merged := mergeOverlayScanRows(baseRows, overlayKVs, plan.Limit)
		if directoryMergeComplete(merged, plan.Prefix, plan.Limit) || !directoryBaseScanMayContinue(batch, plan.Prefix, plan.Limit) {
			return merged, baseTotal, uint32(len(overlayKVs)), usedIndex, nil
		}
		start = keyAfter(batch[len(batch)-1].Key)
	}
}

func mergeOverlayScanRows(kvs []KV, overlayKVs []fsperas.OverlayKV, limit uint32) []KV {
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

func directoryMergeComplete(kvs []KV, prefix []byte, limit uint32) bool {
	if limit == 0 {
		return true
	}
	var n uint32
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, prefix) {
			return true
		}
		n++
		if n >= limit {
			return true
		}
	}
	return false
}

func directoryBaseScanMayContinue(batch []KV, prefix []byte, limit uint32) bool {
	if limit == 0 || uint32(len(batch)) < limit {
		return false
	}
	return bytes.HasPrefix(batch[len(batch)-1].Key, prefix)
}

func directoryOverlayScanMayContinue(batch []fsperas.OverlayKV, prefix []byte, limit uint32) bool {
	if limit == 0 || uint32(len(batch)) < limit {
		return false
	}
	return bytes.HasPrefix(batch[len(batch)-1].Key, prefix)
}

func keyAfter(key []byte) []byte {
	out := make([]byte, len(key)+1)
	copy(out, key)
	return out
}
