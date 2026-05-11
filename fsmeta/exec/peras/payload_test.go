package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestSemanticDeltaPayloadCodecRoundTrip(t *testing.T) {
	delta := testSemanticDelta()

	payload, err := EncodeSemanticDeltaPayload(delta)
	require.NoError(t, err)
	decoded, err := DecodeSemanticDeltaPayload(payload)
	require.NoError(t, err)

	require.Equal(t, delta, decoded)
	payload[0] ^= 0xff
	require.Equal(t, delta, decoded, "decode must not alias encoded payload")
}

func TestSemanticDeltaPayloadDigestRejectsEmptyPayload(t *testing.T) {
	_, err := SemanticDeltaPayloadDigest(nil)
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
}

func TestSemanticDeltaPayloadDecodeRejectsCorruption(t *testing.T) {
	payload := testSemanticDeltaPayload()
	payload[0] ^= 0xff

	_, err := DecodeSemanticDeltaPayload(payload)
	require.ErrorIs(t, err, ErrInvalidWitnessRecord)
}

func testSemanticDeltaPayload() []byte {
	payload, err := EncodeSemanticDeltaPayload(testSemanticDelta())
	if err != nil {
		panic(err)
	}
	return payload
}

func testSemanticDelta() compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind: fsmeta.OperationCreate,
		Plan: fsmeta.OperationPlan{
			Kind:       fsmeta.OperationCreate,
			Mount:      "vol",
			PrimaryKey: []byte("dentry/a"),
			ReadKeys: [][]byte{
				[]byte("dentry/a"),
			},
			MutateKeys: [][]byte{
				[]byte("dentry/a"),
				[]byte("inode/7"),
			},
		},
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Buckets:    []fsmeta.AffinityBucket{3},
			Parents:    []fsmeta.InodeID{fsmeta.RootInode},
			Inodes:     []fsmeta.InodeID{7},
		},
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: []byte("dentry/a")},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: []byte("dentry/a"), Value: []byte("inode=7")},
			{Kind: compile.EffectPut, Key: []byte("inode/7"), Value: []byte("attrs")},
		},
		RuntimeGuards: []compile.RuntimeGuard{compile.GuardQuotaCredit},
		Eligibility:   compile.EligibilityVisibleCommit,
	}
}
