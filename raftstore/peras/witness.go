package peras

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
)

var (
	ErrWitnessNodeConfigInvalid = errors.New("raftstore peras: invalid witness node config")
	ErrWitnessAuthorityMissing  = errors.New("raftstore peras: missing active authority")
	ErrWitnessAuthorityMismatch = errors.New("raftstore peras: authority mismatch")
)

type WitnessNodeConfig struct {
	NodeID      string
	Log         *fsperas.WALWitnessLog
	Authorities *perasauth.ActiveAuthorities
	Now         func() time.Time
}

type WitnessNode struct {
	nodeID      string
	log         *fsperas.WALWitnessLog
	authorities *perasauth.ActiveAuthorities
	now         func() time.Time

	mu       sync.Mutex
	segments map[witnessSegmentKey]struct{}
}

type witnessSegmentKey struct {
	epochID uint64
	root    [32]byte
	digest  [32]byte
}

func NewWitnessNode(cfg WitnessNodeConfig) (*WitnessNode, error) {
	if cfg.NodeID == "" || cfg.Log == nil || cfg.Authorities == nil {
		return nil, ErrWitnessNodeConfigInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &WitnessNode{
		nodeID:      cfg.NodeID,
		log:         cfg.Log,
		authorities: cfg.Authorities,
		now:         now,
		segments:    make(map[witnessSegmentKey]struct{}),
	}, nil
}

func (n *WitnessNode) ID() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}

func (n *WitnessNode) AppendSegment(ctx context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	if n == nil || n.log == nil || n.authorities == nil {
		return ErrWitnessNodeConfigInvalid
	}
	if err := n.validateAuthority(scope, record.EpochID, record.HolderID); err != nil {
		return err
	}
	key := witnessSegmentKey{epochID: record.EpochID, root: record.SegmentRoot, digest: record.SegmentPayloadDigest}

	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.segments[key]; ok {
		return nil
	}
	if err := n.loadEpochLocked(ctx, record.EpochID); err != nil {
		return err
	}
	if _, ok := n.segments[key]; ok {
		return nil
	}
	if _, err := n.log.AppendSegment(ctx, record); err != nil {
		return err
	}
	n.segments[key] = struct{}{}
	return nil
}

func (n *WitnessNode) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if n == nil || n.log == nil {
		return fsperas.WitnessSnapshot{}, ErrWitnessNodeConfigInvalid
	}
	return n.log.Probe(ctx, epochID)
}

func (n *WitnessNode) validateAuthority(scope compile.AuthorityScope, epochID uint64, holderID string) error {
	grant, ok, err := n.authorities.Find(scope, n.now())
	if err != nil {
		return err
	}
	if !ok {
		return ErrWitnessAuthorityMissing
	}
	if grant.EpochID != epochID || grant.HolderID != holderID {
		return ErrWitnessAuthorityMismatch
	}
	return nil
}

func (n *WitnessNode) loadEpochLocked(ctx context.Context, epochID uint64) error {
	snapshot, err := n.log.Probe(ctx, epochID)
	if err != nil {
		return err
	}
	for _, segment := range snapshot.Segments {
		n.segments[witnessSegmentKey{epochID: segment.EpochID, root: segment.SegmentRoot, digest: segment.SegmentPayloadDigest}] = struct{}{}
	}
	return nil
}
