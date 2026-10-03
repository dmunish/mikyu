package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReadBatchBulksLogReads requires a batch of disk reads to reach the log
// file with one contiguous read per chunk instead of one read per record.
func TestReadBatchBulksLogReads(t *testing.T) {
	q := openAsyncQueue(t, "bulkread", func(c *Config) { c.GateCache = true })

	const count = 1000
	for i := range count {
		_, err := q.Append(t.Context(), fmt.Appendf(nil, "record-%04d", i))
		require.NoError(t, err)
	}
	waitFlushed(t, q, count)

	var reads int
	batchReadAtHook = func(n int) { reads++ }
	t.Cleanup(func() { batchReadAtHook = nil })

	msgs, err := q.ReadBatch(t.Context(), 0, count)
	require.NoError(t, err)
	require.Len(t, msgs, count)
	require.LessOrEqual(t, reads, 2, "1000 records must be served by at most two bulk log reads")
	for i, msg := range msgs {
		require.Equal(t, fmt.Sprintf("record-%04d", i), string(msg.Payload))
	}
}

// TestReadBatchMatchesRead checks the bulk path byte-for-byte against the
// per-record path, across variable payload sizes that force span-cap chunking.
func TestReadBatchMatchesRead(t *testing.T) {
	q := openAsyncQueue(t, "parity", func(c *Config) { c.GateCache = true })

	payloads := [][]byte{
		[]byte("tiny"),
		make([]byte, 64<<10),
		make([]byte, 1<<20),
		make([]byte, 1<<20),
		make([]byte, 1<<20),
		make([]byte, 1<<20),
		make([]byte, 1<<20),
		[]byte("small again"),
		make([]byte, 512<<10),
	}
	for i, p := range payloads {
		p[0] = byte(i)
		_, err := q.Append(t.Context(), p)
		require.NoError(t, err)
	}
	waitFlushed(t, q, uint64(len(payloads)))

	msgs, err := q.ReadBatch(t.Context(), 0, len(payloads))
	require.NoError(t, err)
	require.Len(t, msgs, len(payloads))
	for i, msg := range msgs {
		want := payloads[i]
		want[0] = byte(i)
		require.Equal(t, want, msg.Payload, "record %d must match the per-record read", i)
	}
}
