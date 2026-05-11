package peras

import (
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// ScopeToProto keeps the StoreKV wire shape transport-only. The authority
// itself remains rooted in meta/root and compile.AuthorityScope.
func ScopeToProto(scope compile.AuthorityScope) *kvrpcpb.PerasAuthorityScope {
	out := &kvrpcpb.PerasAuthorityScope{
		Mount:      string(scope.Mount),
		MountKeyId: uint64(scope.MountKeyID),
		Buckets:    make([]uint32, 0, len(scope.Buckets)),
		Parents:    make([]uint64, 0, len(scope.Parents)),
		Inodes:     make([]uint64, 0, len(scope.Inodes)),
	}
	for _, bucket := range scope.Buckets {
		out.Buckets = append(out.Buckets, uint32(bucket))
	}
	for _, parent := range scope.Parents {
		out.Parents = append(out.Parents, uint64(parent))
	}
	for _, inode := range scope.Inodes {
		out.Inodes = append(out.Inodes, uint64(inode))
	}
	return out
}

func ScopeFromProto(in *kvrpcpb.PerasAuthorityScope) (compile.AuthorityScope, error) {
	if in == nil {
		return compile.AuthorityScope{}, fmt.Errorf("peras wire: authority scope missing")
	}
	out := compile.AuthorityScope{
		Mount:      fsmeta.MountID(in.GetMount()),
		MountKeyID: fsmeta.MountKeyID(in.GetMountKeyId()),
		Buckets:    make([]fsmeta.AffinityBucket, 0, len(in.GetBuckets())),
		Parents:    make([]fsmeta.InodeID, 0, len(in.GetParents())),
		Inodes:     make([]fsmeta.InodeID, 0, len(in.GetInodes())),
	}
	for _, bucket := range in.GetBuckets() {
		out.Buckets = append(out.Buckets, fsmeta.AffinityBucket(bucket))
	}
	for _, parent := range in.GetParents() {
		out.Parents = append(out.Parents, fsmeta.InodeID(parent))
	}
	for _, inode := range in.GetInodes() {
		out.Inodes = append(out.Inodes, fsmeta.InodeID(inode))
	}
	return out, nil
}

func OperationIDToProto(id fsperas.OperationID) *kvrpcpb.PerasOperationID {
	return &kvrpcpb.PerasOperationID{ClientId: id.ClientID, Seq: id.Seq}
}

func OperationIDFromProto(in *kvrpcpb.PerasOperationID) (fsperas.OperationID, error) {
	if in == nil {
		return fsperas.OperationID{}, fmt.Errorf("peras wire: operation id missing")
	}
	id := fsperas.OperationID{ClientID: in.GetClientId(), Seq: in.GetSeq()}
	if !id.Valid() {
		return fsperas.OperationID{}, fmt.Errorf("peras wire: operation id invalid")
	}
	return id, nil
}

func PrepareRecordToProto(record fsperas.PrepareRecord) *kvrpcpb.PerasPrepareRecord {
	out := &kvrpcpb.PerasPrepareRecord{
		EpochId:              record.EpochID,
		OpId:                 OperationIDToProto(record.OpID),
		DeltaPayload:         append([]byte(nil), record.DeltaPayload...),
		DeltaDigest:          append([]byte(nil), record.DeltaDigest[:]...),
		PredicateDigest:      append([]byte(nil), record.PredicateDigest[:]...),
		AuthorityProofDigest: append([]byte(nil), record.AuthorityProofDigest[:]...),
		DependencyFrontier:   make([]*kvrpcpb.PerasOperationID, 0, len(record.DependencyFrontier)),
		TimestampUnixNano:    record.TimestampUnixNano,
		HolderId:             record.HolderID,
		HolderSignature:      append([]byte(nil), record.HolderSignature[:]...),
	}
	for _, id := range record.DependencyFrontier {
		out.DependencyFrontier = append(out.DependencyFrontier, OperationIDToProto(id))
	}
	return out
}

func PrepareRecordFromProto(in *kvrpcpb.PerasPrepareRecord) (fsperas.PrepareRecord, error) {
	if in == nil {
		return fsperas.PrepareRecord{}, fmt.Errorf("peras wire: prepare record missing")
	}
	opID, err := OperationIDFromProto(in.GetOpId())
	if err != nil {
		return fsperas.PrepareRecord{}, err
	}
	out := fsperas.PrepareRecord{
		EpochID:           in.GetEpochId(),
		OpID:              opID,
		DeltaPayload:      append([]byte(nil), in.GetDeltaPayload()...),
		TimestampUnixNano: in.GetTimestampUnixNano(),
		HolderID:          in.GetHolderId(),
	}
	if err := copyFixed(out.DeltaDigest[:], in.GetDeltaDigest(), "delta_digest"); err != nil {
		return fsperas.PrepareRecord{}, err
	}
	if err := copyFixed(out.PredicateDigest[:], in.GetPredicateDigest(), "predicate_digest"); err != nil {
		return fsperas.PrepareRecord{}, err
	}
	if err := copyFixed(out.AuthorityProofDigest[:], in.GetAuthorityProofDigest(), "authority_proof_digest"); err != nil {
		return fsperas.PrepareRecord{}, err
	}
	if err := copyFixed(out.HolderSignature[:], in.GetHolderSignature(), "holder_signature"); err != nil {
		return fsperas.PrepareRecord{}, err
	}
	if len(in.GetDependencyFrontier()) > 0 {
		out.DependencyFrontier = make([]fsperas.OperationID, 0, len(in.GetDependencyFrontier()))
		for _, predecessor := range in.GetDependencyFrontier() {
			id, err := OperationIDFromProto(predecessor)
			if err != nil {
				return fsperas.PrepareRecord{}, err
			}
			out.DependencyFrontier = append(out.DependencyFrontier, id)
		}
	}
	return out, nil
}

func CommitCertificateRecordToProto(record fsperas.CommitCertificateRecord) *kvrpcpb.PerasCommitCertificateRecord {
	return &kvrpcpb.PerasCommitCertificateRecord{
		EpochId:           record.EpochID,
		OpId:              OperationIDToProto(record.OpID),
		PrepareDigest:     append([]byte(nil), record.PrepareDigest[:]...),
		QuorumAckSet:      append([]string(nil), record.QuorumAckSet...),
		TimestampUnixNano: record.TimestampUnixNano,
		HolderId:          record.HolderID,
		HolderSignature:   append([]byte(nil), record.HolderSignature[:]...),
	}
}

func CommitCertificateRecordFromProto(in *kvrpcpb.PerasCommitCertificateRecord) (fsperas.CommitCertificateRecord, error) {
	if in == nil {
		return fsperas.CommitCertificateRecord{}, fmt.Errorf("peras wire: commit certificate record missing")
	}
	opID, err := OperationIDFromProto(in.GetOpId())
	if err != nil {
		return fsperas.CommitCertificateRecord{}, err
	}
	out := fsperas.CommitCertificateRecord{
		EpochID:           in.GetEpochId(),
		OpID:              opID,
		QuorumAckSet:      append([]string(nil), in.GetQuorumAckSet()...),
		TimestampUnixNano: in.GetTimestampUnixNano(),
		HolderID:          in.GetHolderId(),
	}
	if err := copyFixed(out.PrepareDigest[:], in.GetPrepareDigest(), "prepare_digest"); err != nil {
		return fsperas.CommitCertificateRecord{}, err
	}
	if err := copyFixed(out.HolderSignature[:], in.GetHolderSignature(), "holder_signature"); err != nil {
		return fsperas.CommitCertificateRecord{}, err
	}
	return out, nil
}

func SnapshotToProto(snapshot fsperas.WitnessSnapshot) *kvrpcpb.PerasWitnessProbeResponse {
	out := &kvrpcpb.PerasWitnessProbeResponse{
		Prepares: make([]*kvrpcpb.PerasPrepareRecord, 0, len(snapshot.Prepares)),
		Commits:  make([]*kvrpcpb.PerasCommitCertificateRecord, 0, len(snapshot.Commits)),
	}
	for _, prepare := range snapshot.Prepares {
		out.Prepares = append(out.Prepares, PrepareRecordToProto(prepare))
	}
	for _, commit := range snapshot.Commits {
		out.Commits = append(out.Commits, CommitCertificateRecordToProto(commit))
	}
	return out
}

func SnapshotFromProto(in *kvrpcpb.PerasWitnessProbeResponse) (fsperas.WitnessSnapshot, error) {
	if in == nil {
		return fsperas.WitnessSnapshot{}, fmt.Errorf("peras wire: witness snapshot missing")
	}
	out := fsperas.WitnessSnapshot{
		Prepares: make([]fsperas.PrepareRecord, 0, len(in.GetPrepares())),
		Commits:  make([]fsperas.CommitCertificateRecord, 0, len(in.GetCommits())),
	}
	for _, prepare := range in.GetPrepares() {
		record, err := PrepareRecordFromProto(prepare)
		if err != nil {
			return fsperas.WitnessSnapshot{}, err
		}
		out.Prepares = append(out.Prepares, record)
	}
	for _, commit := range in.GetCommits() {
		record, err := CommitCertificateRecordFromProto(commit)
		if err != nil {
			return fsperas.WitnessSnapshot{}, err
		}
		out.Commits = append(out.Commits, record)
	}
	return out, nil
}

func copyFixed(dst []byte, src []byte, field string) error {
	if len(src) != len(dst) {
		return fmt.Errorf("peras wire: %s length %d != %d", field, len(src), len(dst))
	}
	copy(dst, src)
	return nil
}
