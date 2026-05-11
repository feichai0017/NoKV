package peras

import (
	"crypto/sha256"
	"errors"
	"slices"
)

var ErrInvalidPerasSeal = errors.New("fsmeta peras: invalid peras seal")

type SealedCertificate struct {
	Prepare PrepareRecord
	Commit  CommitCertificateRecord
}

type PerasSeal struct {
	EpochID               uint64
	Versions              ReplayVersionRange
	Certificates          []SealedCertificate
	CertificateMerkleRoot [32]byte
}

func BuildPerasSeal(epochID uint64, snapshot WitnessSnapshot) (PerasSeal, error) {
	return buildPerasSeal(epochID, ReplayVersionRange{}, snapshot)
}

func BuildPerasSealWithVersions(epochID, firstVersion uint64, snapshot WitnessSnapshot) (PerasSeal, error) {
	committed := committedCertificateCount(epochID, snapshot)
	if committed == 0 {
		return PerasSeal{}, ErrInvalidPerasSeal
	}
	versions := ReplayVersionRange{First: firstVersion, Count: committed}
	if err := versions.Validate(); err != nil {
		return PerasSeal{}, err
	}
	return buildPerasSeal(epochID, versions, snapshot)
}

func buildPerasSeal(epochID uint64, versions ReplayVersionRange, snapshot WitnessSnapshot) (PerasSeal, error) {
	if epochID == 0 {
		return PerasSeal{}, ErrInvalidPerasSeal
	}
	prepares := make(map[OperationID]PrepareRecord, len(snapshot.Prepares))
	for _, prepare := range snapshot.Prepares {
		if prepare.EpochID != epochID {
			continue
		}
		if err := validatePrepareRecord(prepare); err != nil {
			return PerasSeal{}, err
		}
		if _, ok := prepares[prepare.OpID]; ok {
			return PerasSeal{}, ErrInvalidPerasSeal
		}
		prepares[prepare.OpID] = clonePrepareRecord(prepare)
	}
	commits := make(map[OperationID]CommitCertificateRecord, len(snapshot.Commits))
	for _, commit := range snapshot.Commits {
		if commit.EpochID != epochID {
			continue
		}
		if err := validateCommitCertificateRecord(commit); err != nil {
			return PerasSeal{}, err
		}
		if _, ok := commits[commit.OpID]; ok {
			return PerasSeal{}, ErrInvalidPerasSeal
		}
		prepare, ok := prepares[commit.OpID]
		if !ok {
			return PerasSeal{}, ErrInvalidPerasSeal
		}
		digest, err := PrepareDigest(prepare)
		if err != nil {
			return PerasSeal{}, err
		}
		if digest != commit.PrepareDigest {
			return PerasSeal{}, ErrInvalidPerasSeal
		}
		commits[commit.OpID] = cloneCommitCertificateRecord(commit)
	}
	if len(commits) == 0 {
		return PerasSeal{}, ErrInvalidPerasSeal
	}
	ordered, err := orderedSealCertificates(prepares, commits)
	if err != nil {
		return PerasSeal{}, err
	}
	seal := PerasSeal{
		EpochID:      epochID,
		Versions:     versions,
		Certificates: ordered,
	}
	seal.CertificateMerkleRoot, err = sealMerkleRoot(epochID, versions, ordered)
	if err != nil {
		return PerasSeal{}, err
	}
	return seal, nil
}

func committedCertificateCount(epochID uint64, snapshot WitnessSnapshot) uint64 {
	prepares := make(map[OperationID]struct{}, len(snapshot.Prepares))
	for _, prepare := range snapshot.Prepares {
		if prepare.EpochID == epochID && prepare.OpID.Valid() {
			prepares[prepare.OpID] = struct{}{}
		}
	}
	var count uint64
	seen := make(map[OperationID]struct{}, len(snapshot.Commits))
	for _, commit := range snapshot.Commits {
		if commit.EpochID != epochID || !commit.OpID.Valid() {
			continue
		}
		if _, ok := seen[commit.OpID]; ok {
			continue
		}
		if _, ok := prepares[commit.OpID]; !ok {
			continue
		}
		seen[commit.OpID] = struct{}{}
		count++
	}
	return count
}

func filterWitnessSnapshotByIDs(snapshot WitnessSnapshot, ids []OperationID) WitnessSnapshot {
	if len(ids) == 0 {
		return WitnessSnapshot{}
	}
	want := make(map[OperationID]struct{}, len(ids))
	for _, id := range ids {
		if id.Valid() {
			want[id] = struct{}{}
		}
	}
	out := WitnessSnapshot{
		Prepares: make([]PrepareRecord, 0, len(ids)),
		Commits:  make([]CommitCertificateRecord, 0, len(ids)),
	}
	for _, prepare := range snapshot.Prepares {
		if _, ok := want[prepare.OpID]; ok {
			out.Prepares = append(out.Prepares, clonePrepareRecord(prepare))
		}
	}
	for _, commit := range snapshot.Commits {
		if _, ok := want[commit.OpID]; ok {
			out.Commits = append(out.Commits, cloneCommitCertificateRecord(commit))
		}
	}
	return out
}

func orderedSealCertificates(prepares map[OperationID]PrepareRecord, commits map[OperationID]CommitCertificateRecord) ([]SealedCertificate, error) {
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
			for _, predecessor := range prepare.DependencyFrontier {
				if _, committed := commits[predecessor]; !committed {
					return nil, ErrInvalidPerasSeal
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
			return nil, ErrInvalidPerasSeal
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

func sealMerkleRoot(epochID uint64, versions ReplayVersionRange, certs []SealedCertificate) ([32]byte, error) {
	if len(certs) == 0 {
		return [32]byte{}, ErrInvalidPerasSeal
	}
	level := make([][32]byte, 0, len(certs)+1)
	level = append(level, sealMetadataDigest(epochID, versions))
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

func sealMetadataDigest(epochID uint64, versions ReplayVersionRange) [32]byte {
	h := sha256.New()
	writeUint64(h, epochID)
	writeUint64(h, versions.First)
	writeUint64(h, versions.Count)
	return digestFromHash(h.Sum(nil))
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
