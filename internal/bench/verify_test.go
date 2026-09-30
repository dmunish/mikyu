package bench

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadDeterministic(t *testing.T) {
	a := make([]byte, 256)
	b := make([]byte, 256)
	payloadFor(7, 42, a)
	payloadFor(7, 42, b)
	assert.Equal(t, a, b, "same seed and offset must produce the same bytes")

	payloadFor(8, 42, b)
	assert.NotEqual(t, a, b, "a different seed must change the bytes")

	payloadFor(7, 43, b)
	assert.NotEqual(t, a, b, "a different offset must change the bytes")
}

func TestPayloadShort(t *testing.T) {
	short := make([]byte, 3)
	payloadFor(1, 1, short)
	assert.Len(t, short, 3)
}

func TestVerifyCatchesCorruption(t *testing.T) {
	payload := make([]byte, 256)
	payloadFor(3, 99, payload)
	require.NoError(t, verifyPayload(3, 99, payload))

	payload[100] ^= 0xff
	err := verifyPayload(3, 99, payload)
	require.ErrorIs(t, err, ErrVerify)

	var verify *VerifyError
	require.ErrorAs(t, err, &verify)
	assert.Equal(t, uint64(99), verify.Offset)
}

func TestVerifyOverhead(t *testing.T) {
	if raceEnabled {
		t.Skip("timing assertions are meaningless under the race detector")
	}
	payload := make([]byte, 256)
	payloadFor(1, 1, payload)
	start := time.Now()
	for range 1_000_000 {
		if err := verifyPayload(1, 1, payload); err != nil {
			t.Fatalf("matching payload reported an error: %v", err)
		}
	}
	assert.Less(t, time.Since(start), time.Second, "verification must stay well under 1µs per record")
}
