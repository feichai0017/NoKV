package capsule

import (
	"crypto/sha256"
	"errors"
	"slices"
)

var ErrInvalidCapsuleSeal = errors.New("fsmeta capsule: invalid capsule seal")

type SealedCertificate struct {
	Prepare PrepareRecord
	Commit  CommitCertificateRecord
}

type CapsuleSeal struct {
	EpochID           uint64
	Certificates      []SealedCertificate
	DAGFrontierMerkle [32]byte
}

func BuildCapsuleSeal(epochID uint64, snapshot WitnessSnapshot) (CapsuleSeal, error) {
	if epochID == 0 {
		return CapsuleSeal{}, ErrInvalidCapsuleSeal
	}
	prepares := make(map[OperationID]PrepareRecord, len(snapshot.Prepares))
	for _, prepare := range snapshot.Prepares {
		if prepare.EpochID != epochID {
			continue
		}
		if err := validatePrepareRecord(prepare); err != nil {
			return CapsuleSeal{}, err
		}
		if _, ok := prepares[prepare.OpID]; ok {
			return CapsuleSeal{}, ErrInvalidCapsuleSeal
		}
		prepares[prepare.OpID] = clonePrepareRecord(prepare)
	}
	commits := make(map[OperationID]CommitCertificateRecord, len(snapshot.Commits))
	for _, commit := range snapshot.Commits {
		if commit.EpochID != epochID {
			continue
		}
		if err := validateCommitCertificateRecord(commit); err != nil {
			return CapsuleSeal{}, err
		}
		if _, ok := commits[commit.OpID]; ok {
			return CapsuleSeal{}, ErrInvalidCapsuleSeal
		}
		prepare, ok := prepares[commit.OpID]
		if !ok {
			return CapsuleSeal{}, ErrInvalidCapsuleSeal
		}
		digest, err := PrepareDigest(prepare)
		if err != nil {
			return CapsuleSeal{}, err
		}
		if digest != commit.PrepareDigest {
			return CapsuleSeal{}, ErrInvalidCapsuleSeal
		}
		commits[commit.OpID] = cloneCommitCertificateRecord(commit)
	}
	if len(commits) == 0 {
		return CapsuleSeal{}, ErrInvalidCapsuleSeal
	}
	ordered, err := topologicalSealOrder(prepares, commits)
	if err != nil {
		return CapsuleSeal{}, err
	}
	seal := CapsuleSeal{
		EpochID:      epochID,
		Certificates: ordered,
	}
	seal.DAGFrontierMerkle, err = sealMerkleRoot(ordered)
	if err != nil {
		return CapsuleSeal{}, err
	}
	return seal, nil
}

func topologicalSealOrder(prepares map[OperationID]PrepareRecord, commits map[OperationID]CommitCertificateRecord) ([]SealedCertificate, error) {
	remaining := make(map[OperationID]struct{}, len(commits))
	for id := range commits {
		remaining[id] = struct{}{}
	}
	out := make([]SealedCertificate, 0, len(commits))
	for len(remaining) > 0 {
		ready := make([]OperationID, 0, len(remaining))
		for id := range remaining {
			prepare := prepares[id]
			blocked := false
			for _, predecessor := range prepare.ConflictDAGFrontier {
				if _, committed := commits[predecessor]; !committed {
					return nil, ErrInvalidCapsuleSeal
				}
				if _, pending := remaining[predecessor]; pending {
					blocked = true
					break
				}
			}
			if !blocked {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			return nil, ErrInvalidCapsuleSeal
		}
		slices.SortFunc(ready, compareOperationID)
		for _, id := range ready {
			out = append(out, SealedCertificate{
				Prepare: clonePrepareRecord(prepares[id]),
				Commit:  cloneCommitCertificateRecord(commits[id]),
			})
			delete(remaining, id)
		}
	}
	return out, nil
}

func sealMerkleRoot(certs []SealedCertificate) ([32]byte, error) {
	if len(certs) == 0 {
		return [32]byte{}, ErrInvalidCapsuleSeal
	}
	level := make([][32]byte, 0, len(certs))
	for _, cert := range certs {
		digest, err := sealedCertificateDigest(cert)
		if err != nil {
			return [32]byte{}, err
		}
		level = append(level, digest)
	}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left
			if i+1 < len(level) {
				right = level[i+1]
			}
			h := sha256.New()
			_, _ = h.Write(left[:])
			_, _ = h.Write(right[:])
			next = append(next, digestFromHash(h.Sum(nil)))
		}
		level = next
	}
	return level[0], nil
}

func sealedCertificateDigest(cert SealedCertificate) ([32]byte, error) {
	preparePayload, err := EncodePrepareRecord(cert.Prepare)
	if err != nil {
		return [32]byte{}, err
	}
	commitPayload, err := EncodeCommitCertificateRecord(cert.Commit)
	if err != nil {
		return [32]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write(preparePayload)
	_, _ = h.Write(commitPayload)
	return digestFromHash(h.Sum(nil)), nil
}

func compareOperationID(left, right OperationID) int {
	if left.ClientID < right.ClientID {
		return -1
	}
	if left.ClientID > right.ClientID {
		return 1
	}
	if left.Seq < right.Seq {
		return -1
	}
	if left.Seq > right.Seq {
		return 1
	}
	return 0
}
