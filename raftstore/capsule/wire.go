package capsule

import (
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// ScopeToProto keeps the StoreKV wire shape transport-only. The authority
// itself remains rooted in meta/root and compile.AuthorityScope.
func ScopeToProto(scope compile.AuthorityScope) *kvrpcpb.CapsuleAuthorityScope {
	out := &kvrpcpb.CapsuleAuthorityScope{
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

func ScopeFromProto(in *kvrpcpb.CapsuleAuthorityScope) (compile.AuthorityScope, error) {
	if in == nil {
		return compile.AuthorityScope{}, fmt.Errorf("capsule wire: authority scope missing")
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

func OperationIDToProto(id fscapsule.OperationID) *kvrpcpb.CapsuleOperationID {
	return &kvrpcpb.CapsuleOperationID{ClientId: id.ClientID, Seq: id.Seq}
}

func OperationIDFromProto(in *kvrpcpb.CapsuleOperationID) (fscapsule.OperationID, error) {
	if in == nil {
		return fscapsule.OperationID{}, fmt.Errorf("capsule wire: operation id missing")
	}
	id := fscapsule.OperationID{ClientID: in.GetClientId(), Seq: in.GetSeq()}
	if !id.Valid() {
		return fscapsule.OperationID{}, fmt.Errorf("capsule wire: operation id invalid")
	}
	return id, nil
}

func PrepareRecordToProto(record fscapsule.PrepareRecord) *kvrpcpb.CapsulePrepareRecord {
	out := &kvrpcpb.CapsulePrepareRecord{
		EpochId:              record.EpochID,
		OpId:                 OperationIDToProto(record.OpID),
		DeltaPayload:         append([]byte(nil), record.DeltaPayload...),
		DeltaDigest:          append([]byte(nil), record.DeltaDigest[:]...),
		PredicateDigest:      append([]byte(nil), record.PredicateDigest[:]...),
		AuthorityProofDigest: append([]byte(nil), record.AuthorityProofDigest[:]...),
		ConflictDagFrontier:  make([]*kvrpcpb.CapsuleOperationID, 0, len(record.DependencyFrontier)),
		TimestampUnixNano:    record.TimestampUnixNano,
		HolderId:             record.HolderID,
		HolderSignature:      append([]byte(nil), record.HolderSignature[:]...),
	}
	for _, id := range record.DependencyFrontier {
		out.ConflictDagFrontier = append(out.ConflictDagFrontier, OperationIDToProto(id))
	}
	return out
}

func PrepareRecordFromProto(in *kvrpcpb.CapsulePrepareRecord) (fscapsule.PrepareRecord, error) {
	if in == nil {
		return fscapsule.PrepareRecord{}, fmt.Errorf("capsule wire: prepare record missing")
	}
	opID, err := OperationIDFromProto(in.GetOpId())
	if err != nil {
		return fscapsule.PrepareRecord{}, err
	}
	out := fscapsule.PrepareRecord{
		EpochID:           in.GetEpochId(),
		OpID:              opID,
		DeltaPayload:      append([]byte(nil), in.GetDeltaPayload()...),
		TimestampUnixNano: in.GetTimestampUnixNano(),
		HolderID:          in.GetHolderId(),
	}
	if err := copyFixed(out.DeltaDigest[:], in.GetDeltaDigest(), "delta_digest"); err != nil {
		return fscapsule.PrepareRecord{}, err
	}
	if err := copyFixed(out.PredicateDigest[:], in.GetPredicateDigest(), "predicate_digest"); err != nil {
		return fscapsule.PrepareRecord{}, err
	}
	if err := copyFixed(out.AuthorityProofDigest[:], in.GetAuthorityProofDigest(), "authority_proof_digest"); err != nil {
		return fscapsule.PrepareRecord{}, err
	}
	if err := copyFixed(out.HolderSignature[:], in.GetHolderSignature(), "holder_signature"); err != nil {
		return fscapsule.PrepareRecord{}, err
	}
	if len(in.GetConflictDagFrontier()) > 0 {
		out.DependencyFrontier = make([]fscapsule.OperationID, 0, len(in.GetConflictDagFrontier()))
		for _, predecessor := range in.GetConflictDagFrontier() {
			id, err := OperationIDFromProto(predecessor)
			if err != nil {
				return fscapsule.PrepareRecord{}, err
			}
			out.DependencyFrontier = append(out.DependencyFrontier, id)
		}
	}
	return out, nil
}

func CommitCertificateRecordToProto(record fscapsule.CommitCertificateRecord) *kvrpcpb.CapsuleCommitCertificateRecord {
	return &kvrpcpb.CapsuleCommitCertificateRecord{
		EpochId:           record.EpochID,
		OpId:              OperationIDToProto(record.OpID),
		PrepareDigest:     append([]byte(nil), record.PrepareDigest[:]...),
		QuorumAckSet:      append([]string(nil), record.QuorumAckSet...),
		TimestampUnixNano: record.TimestampUnixNano,
		HolderId:          record.HolderID,
		HolderSignature:   append([]byte(nil), record.HolderSignature[:]...),
	}
}

func CommitCertificateRecordFromProto(in *kvrpcpb.CapsuleCommitCertificateRecord) (fscapsule.CommitCertificateRecord, error) {
	if in == nil {
		return fscapsule.CommitCertificateRecord{}, fmt.Errorf("capsule wire: commit certificate record missing")
	}
	opID, err := OperationIDFromProto(in.GetOpId())
	if err != nil {
		return fscapsule.CommitCertificateRecord{}, err
	}
	out := fscapsule.CommitCertificateRecord{
		EpochID:           in.GetEpochId(),
		OpID:              opID,
		QuorumAckSet:      append([]string(nil), in.GetQuorumAckSet()...),
		TimestampUnixNano: in.GetTimestampUnixNano(),
		HolderID:          in.GetHolderId(),
	}
	if err := copyFixed(out.PrepareDigest[:], in.GetPrepareDigest(), "prepare_digest"); err != nil {
		return fscapsule.CommitCertificateRecord{}, err
	}
	if err := copyFixed(out.HolderSignature[:], in.GetHolderSignature(), "holder_signature"); err != nil {
		return fscapsule.CommitCertificateRecord{}, err
	}
	return out, nil
}

func SnapshotToProto(snapshot fscapsule.WitnessSnapshot) *kvrpcpb.CapsuleWitnessProbeResponse {
	out := &kvrpcpb.CapsuleWitnessProbeResponse{
		Prepares: make([]*kvrpcpb.CapsulePrepareRecord, 0, len(snapshot.Prepares)),
		Commits:  make([]*kvrpcpb.CapsuleCommitCertificateRecord, 0, len(snapshot.Commits)),
	}
	for _, prepare := range snapshot.Prepares {
		out.Prepares = append(out.Prepares, PrepareRecordToProto(prepare))
	}
	for _, commit := range snapshot.Commits {
		out.Commits = append(out.Commits, CommitCertificateRecordToProto(commit))
	}
	return out
}

func SnapshotFromProto(in *kvrpcpb.CapsuleWitnessProbeResponse) (fscapsule.WitnessSnapshot, error) {
	if in == nil {
		return fscapsule.WitnessSnapshot{}, fmt.Errorf("capsule wire: witness snapshot missing")
	}
	out := fscapsule.WitnessSnapshot{
		Prepares: make([]fscapsule.PrepareRecord, 0, len(in.GetPrepares())),
		Commits:  make([]fscapsule.CommitCertificateRecord, 0, len(in.GetCommits())),
	}
	for _, prepare := range in.GetPrepares() {
		record, err := PrepareRecordFromProto(prepare)
		if err != nil {
			return fscapsule.WitnessSnapshot{}, err
		}
		out.Prepares = append(out.Prepares, record)
	}
	for _, commit := range in.GetCommits() {
		record, err := CommitCertificateRecordFromProto(commit)
		if err != nil {
			return fscapsule.WitnessSnapshot{}, err
		}
		out.Commits = append(out.Commits, record)
	}
	return out, nil
}

func copyFixed(dst []byte, src []byte, field string) error {
	if len(src) != len(dst) {
		return fmt.Errorf("capsule wire: %s length %d != %d", field, len(src), len(dst))
	}
	copy(dst, src)
	return nil
}
