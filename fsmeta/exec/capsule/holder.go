package capsule

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

var (
	ErrHolderConfigInvalid      = errors.New("fsmeta capsule: invalid holder config")
	ErrIneligibleOperation      = errors.New("fsmeta capsule: ineligible operation")
	ErrWitnessQuorumUnavailable = errors.New("fsmeta capsule: witness quorum unavailable")
	ErrWitnessCommitAmbiguous   = errors.New("fsmeta capsule: witness commit ambiguous")
)

type WitnessReplica interface {
	ID() string
	AppendPrepare(context.Context, PrepareRecord) error
	AppendCommitCertificate(context.Context, CommitCertificateRecord) error
}

type LocalWitnessReplica struct {
	id  string
	log *WALWitnessLog
}

func NewLocalWitnessReplica(id string, log *WALWitnessLog) (*LocalWitnessReplica, error) {
	if id == "" || log == nil {
		return nil, ErrHolderConfigInvalid
	}
	return &LocalWitnessReplica{id: id, log: log}, nil
}

func (r *LocalWitnessReplica) ID() string {
	if r == nil {
		return ""
	}
	return r.id
}

func (r *LocalWitnessReplica) AppendPrepare(ctx context.Context, record PrepareRecord) error {
	if r == nil || r.log == nil {
		return ErrWitnessLogRequired
	}
	_, err := r.log.AppendPrepare(ctx, record)
	return err
}

func (r *LocalWitnessReplica) AppendCommitCertificate(ctx context.Context, record CommitCertificateRecord) error {
	if r == nil || r.log == nil {
		return ErrWitnessLogRequired
	}
	_, err := r.log.AppendCommitCertificate(ctx, record)
	return err
}

type HolderConfig struct {
	EpochID   uint64
	HolderID  string
	Witnesses []WitnessReplica
	Quorum    int
	Now       func() time.Time
}

type Holder struct {
	epochID   uint64
	holderID  string
	witnesses []WitnessReplica
	quorum    int
	detector  *ConflictDetector
	now       func() time.Time
}

func NewHolder(cfg HolderConfig) (*Holder, error) {
	if cfg.EpochID == 0 || cfg.HolderID == "" || len(cfg.Witnesses) == 0 {
		return nil, ErrHolderConfigInvalid
	}
	witnesses := make([]WitnessReplica, 0, len(cfg.Witnesses))
	seen := make(map[string]struct{}, len(cfg.Witnesses))
	for _, witness := range cfg.Witnesses {
		if witness == nil || witness.ID() == "" {
			return nil, ErrHolderConfigInvalid
		}
		if _, ok := seen[witness.ID()]; ok {
			return nil, ErrHolderConfigInvalid
		}
		seen[witness.ID()] = struct{}{}
		witnesses = append(witnesses, witness)
	}
	quorum := cfg.Quorum
	if quorum == 0 {
		quorum = len(witnesses)/2 + 1
	}
	if quorum <= 0 || quorum > len(witnesses) {
		return nil, ErrHolderConfigInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Holder{
		epochID:   cfg.EpochID,
		holderID:  cfg.HolderID,
		witnesses: witnesses,
		quorum:    quorum,
		detector:  NewConflictDetector(),
		now:       now,
	}, nil
}

func (h *Holder) Submit(ctx context.Context, id OperationID, delta compile.SemanticDelta) (CommitCertificateRecord, error) {
	if h == nil || h.detector == nil {
		return CommitCertificateRecord{}, ErrHolderConfigInvalid
	}
	if delta.Eligibility != compile.EligibilityFastPath {
		return CommitCertificateRecord{}, ErrIneligibleOperation
	}
	predecessors, err := h.detector.Admit(id, delta)
	if err != nil {
		return CommitCertificateRecord{}, err
	}

	deltaDigest, predicateDigest, authorityDigest := digestSemanticDelta(delta)
	prepare := PrepareRecord{
		EpochID:              h.epochID,
		OpID:                 id,
		DeltaDigest:          deltaDigest,
		PredicateDigest:      predicateDigest,
		AuthorityProofDigest: authorityDigest,
		ConflictDAGFrontier:  predecessors,
		TimestampUnixNano:    h.now().UnixNano(),
		HolderID:             h.holderID,
	}
	prepareAcks := h.broadcastPrepare(ctx, prepare)
	if len(prepareAcks) < h.quorum {
		h.detector.Remove(id)
		return CommitCertificateRecord{}, ErrWitnessQuorumUnavailable
	}
	prepareDigest, err := PrepareDigest(prepare)
	if err != nil {
		h.detector.Remove(id)
		return CommitCertificateRecord{}, err
	}
	commit := CommitCertificateRecord{
		EpochID:           h.epochID,
		OpID:              id,
		PrepareDigest:     prepareDigest,
		QuorumAckSet:      prepareAcks,
		TimestampUnixNano: h.now().UnixNano(),
		HolderID:          h.holderID,
	}
	commitAcks := h.broadcastCommit(ctx, commit)
	if len(commitAcks) < h.quorum {
		if len(commitAcks) == 0 {
			h.detector.Remove(id)
			return CommitCertificateRecord{}, ErrWitnessQuorumUnavailable
		}
		return commit, ErrWitnessCommitAmbiguous
	}
	return commit, nil
}

func (h *Holder) MarkSealed(ids ...OperationID) {
	if h == nil || h.detector == nil {
		return
	}
	for _, id := range ids {
		h.detector.Remove(id)
	}
}

func (h *Holder) BuildSeal(snapshot WitnessSnapshot) (CapsuleSeal, error) {
	if h == nil {
		return CapsuleSeal{}, ErrHolderConfigInvalid
	}
	return BuildCapsuleSeal(h.epochID, snapshot)
}

func (h *Holder) MarkSealApplied(seal CapsuleSeal) error {
	if h == nil || h.detector == nil {
		return ErrHolderConfigInvalid
	}
	if seal.EpochID != h.epochID || len(seal.Certificates) == 0 {
		return ErrInvalidCapsuleSeal
	}
	ids := make([]OperationID, 0, len(seal.Certificates))
	for _, cert := range seal.Certificates {
		ids = append(ids, cert.Prepare.OpID)
	}
	h.MarkSealed(ids...)
	return nil
}

func (h *Holder) Pending() int {
	if h == nil || h.detector == nil {
		return 0
	}
	return h.detector.Len()
}

func (h *Holder) broadcastPrepare(ctx context.Context, record PrepareRecord) []string {
	acks := make([]string, 0, len(h.witnesses))
	for _, witness := range h.witnesses {
		if err := ctxErr(ctx); err != nil {
			break
		}
		if err := witness.AppendPrepare(ctx, record); err != nil {
			continue
		}
		acks = append(acks, witness.ID())
	}
	return acks
}

func (h *Holder) broadcastCommit(ctx context.Context, record CommitCertificateRecord) []string {
	acks := make([]string, 0, len(h.witnesses))
	for _, witness := range h.witnesses {
		if err := ctxErr(ctx); err != nil {
			break
		}
		if err := witness.AppendCommitCertificate(ctx, record); err != nil {
			continue
		}
		acks = append(acks, witness.ID())
	}
	return acks
}

func digestSemanticDelta(delta compile.SemanticDelta) ([32]byte, [32]byte, [32]byte) {
	return hashDelta(delta), hashPredicates(delta.ReadPredicates), hashAuthority(delta.Authority)
}

func hashDelta(delta compile.SemanticDelta) [32]byte {
	h := sha256.New()
	writeString(h, string(delta.Kind))
	writeKeySetHash(h, delta.Plan.ReadKeys)
	writeKeySetHash(h, delta.Plan.ReadPrefixes)
	writeKeySetHash(h, delta.Plan.MutateKeys)
	for _, predicate := range delta.ReadPredicates {
		writeUint64(h, uint64(predicate.Kind))
		writeBytesHash(h, predicate.Key)
	}
	for _, effect := range delta.WriteEffects {
		writeUint64(h, uint64(effect.Kind))
		writeBytesHash(h, effect.Key)
		writeBytesHash(h, effect.Value)
	}
	for _, guard := range delta.RuntimeGuards {
		writeString(h, string(guard))
	}
	return digestFromHash(h.Sum(nil))
}

func hashPredicates(predicates []compile.Predicate) [32]byte {
	h := sha256.New()
	for _, predicate := range predicates {
		writeUint64(h, uint64(predicate.Kind))
		writeBytesHash(h, predicate.Key)
	}
	return digestFromHash(h.Sum(nil))
}

func hashAuthority(scope compile.AuthorityScope) [32]byte {
	h := sha256.New()
	writeString(h, string(scope.Mount))
	writeUint64(h, uint64(scope.MountKeyID))
	for _, bucket := range scope.Buckets {
		writeUint64(h, uint64(bucket))
	}
	for _, parent := range scope.Parents {
		writeUint64(h, uint64(parent))
	}
	for _, inode := range scope.Inodes {
		writeUint64(h, uint64(inode))
	}
	return digestFromHash(h.Sum(nil))
}

func writeKeySetHash(h interface{ Write([]byte) (int, error) }, keys [][]byte) {
	for _, key := range keys {
		writeBytesHash(h, key)
	}
}

func writeBytesHash(h interface{ Write([]byte) (int, error) }, value []byte) {
	writeUint64(h, uint64(len(value)))
	_, _ = h.Write(value)
}

func digestFromHash(sum []byte) [32]byte {
	var out [32]byte
	copy(out[:], sum)
	return out
}
