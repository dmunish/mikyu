package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"uuid"
)

// TestEncodeBufferReused pins the buffer pool: encoding a batch of records
// reuses one pooled buffer instead of allocating per record.
func TestEncodeBufferReused(t *testing.T) {
	id := uuid.NewV7()
	ts := time.Now()
	group := []byte("grp")
	payload := make([]byte, 512)
	maxKeep := 4 * int(testConfig().MaxPayload)

	for range 10 {
		bb := getBuf(0)
		encodeRecord(bb.b, id, ts, group, payload)
		bb.release(maxKeep)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		bb := getBuf(0)
		encodeRecord(bb.b, id, ts, group, payload)
		bb.release(maxKeep)
	})
	require.Less(t, allocs, float64(5), "pooled encode path must not allocate per record")
}

// TestPooledRecordsRoundTrip drives the pooled encode path end to end:
// produce-only (gate closed, buffers pooled and recycled), then read every
// record back from disk to prove recycling never corrupts written data.
func TestPooledRecordsRoundTrip(t *testing.T) {
	q := openAsyncQueue(t, "pooled", func(c *Config) { c.GateCache = true })

	const singles = 200
	for i := range singles {
		_, err := q.Append(t.Context(), fmt.Appendf(nil, "pooled-%04d", i))
		require.NoError(t, err)
	}
	const batched = 50
	batch := make([][]byte, batched)
	for i := range batched {
		batch[i] = fmt.Appendf(nil, "batched-%04d", i)
	}
	_, err := q.AppendBatch(t.Context(), batch)
	require.NoError(t, err)

	waitFlushed(t, q, singles+batched)

	for i := range singles {
		rec, err := q.Read(t.Context(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("pooled-%04d", i), string(rec.Payload))
	}
	for i := range batched {
		rec, err := q.Read(t.Context(), uint64(singles+i))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("batched-%04d", i), string(rec.Payload))
	}
}
