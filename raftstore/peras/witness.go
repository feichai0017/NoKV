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
	ErrWitnessDuplicateRecord   = errors.New("raftstore peras: duplicate witness record")
	ErrWitnessPrepareMissing    = errors.New("raftstore peras: missing prepare record")
	ErrWitnessPrepareMismatch   = errors.New("raftstore peras: prepare digest mismatch")
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
	prepares map[witnessOpKey][32]byte
	commits  map[witnessOpKey][32]byte
}

type witnessOpKey struct {
	epochID uint64
	opID    fsperas.OperationID
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
		prepares:    make(map[witnessOpKey][32]byte),
		commits:     make(map[witnessOpKey][32]byte),
	}, nil
}

func (n *WitnessNode) ID() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}

func (n *WitnessNode) AppendPrepare(ctx context.Context, scope compile.AuthorityScope, record fsperas.PrepareRecord) error {
	if n == nil || n.log == nil || n.authorities == nil {
		return ErrWitnessNodeConfigInvalid
	}
	if err := n.validateAuthority(scope, record.EpochID, record.HolderID); err != nil {
		return err
	}
	digest, err := fsperas.PrepareDigest(record)
	if err != nil {
		return err
	}
	key := witnessKey(record.EpochID, record.OpID)

	n.mu.Lock()
	defer n.mu.Unlock()
	if existing, ok := n.prepares[key]; ok {
		if existing != digest {
			return ErrWitnessDuplicateRecord
		}
		return nil
	}
	if existing, ok := n.commits[key]; ok && existing != digest {
		return ErrWitnessDuplicateRecord
	}
	if _, err := n.log.AppendPrepare(ctx, record); err != nil {
		return err
	}
	n.prepares[key] = digest
	return nil
}

func (n *WitnessNode) AppendCommitCertificate(ctx context.Context, scope compile.AuthorityScope, record fsperas.CommitCertificateRecord) error {
	if n == nil || n.log == nil || n.authorities == nil {
		return ErrWitnessNodeConfigInvalid
	}
	if err := n.validateAuthority(scope, record.EpochID, record.HolderID); err != nil {
		return err
	}
	key := witnessKey(record.EpochID, record.OpID)

	n.mu.Lock()
	defer n.mu.Unlock()
	if existing, ok := n.commits[key]; ok {
		if existing != record.PrepareDigest {
			return ErrWitnessDuplicateRecord
		}
		return nil
	}
	prepareDigest, ok := n.prepares[key]
	if !ok {
		if err := n.loadEpochLocked(ctx, record.EpochID); err != nil {
			return err
		}
		prepareDigest, ok = n.prepares[key]
	}
	if !ok {
		return ErrWitnessPrepareMissing
	}
	if prepareDigest != record.PrepareDigest {
		return ErrWitnessPrepareMismatch
	}
	if _, err := n.log.AppendCommitCertificate(ctx, record); err != nil {
		return err
	}
	n.commits[key] = record.PrepareDigest
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
	for _, prepare := range snapshot.Prepares {
		digest, err := fsperas.PrepareDigest(prepare)
		if err != nil {
			return err
		}
		key := witnessKey(prepare.EpochID, prepare.OpID)
		if existing, ok := n.prepares[key]; ok && existing != digest {
			return ErrWitnessDuplicateRecord
		}
		n.prepares[key] = digest
	}
	for _, commit := range snapshot.Commits {
		key := witnessKey(commit.EpochID, commit.OpID)
		if existing, ok := n.commits[key]; ok && existing != commit.PrepareDigest {
			return ErrWitnessDuplicateRecord
		}
		n.commits[key] = commit.PrepareDigest
	}
	return nil
}

func witnessKey(epochID uint64, opID fsperas.OperationID) witnessOpKey {
	return witnessOpKey{epochID: epochID, opID: opID}
}
