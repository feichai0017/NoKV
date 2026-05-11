package capsule

import (
	"testing"

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
