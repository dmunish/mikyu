package engine

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

// TestOverlappedSingleSyscall drives one large batch through a live queue and
// requires the WAL to reach the kernel with one log write and one index write.
func TestOverlappedSingleSyscall(t *testing.T) {
	type span struct {
		off, n int64
	}
	dir := t.TempDir()
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	cfg.MaxFlushBytes = 8 << 20
	seg, err := openSegment(dir, 0, 0)
	require.NoError(t, err)
	defer seg.Close()
	w := newWAL(seg, cfg, 0, nil)

	var writes []span
	writeAtHook = func(off int64, n int) {
		writes = append(writes, span{off, int64(n)})
	}
	t.Cleanup(func() { writeAtHook = nil })

	const count = 1000
	const recLen = 8
	entries := make([]asyncEntry, count)
	for i := range count {
		entries[i] = asyncEntry{
			seg:    seg,
			rec:    make([]byte, recLen),
			pos:    int64(i) * recLen,
			recLen: recLen,
			offset: uint64(i),
		}
	}
	require.NoError(t, w.pushAsyncBatch(t.Context(), entries))
	w.start()
	waitWALDrained(t, w, count)
	require.NoError(t, w.stop())

	require.Len(t, writes, 1, "1000 records must reach the kernel in a single write")
	sort.Slice(writes, func(i, j int) bool { return writes[i].off < writes[j].off })
	var want int64
	for _, s := range writes {
		require.Equal(t, want, s.off)
		want += s.n
	}
	require.Equal(t, int64(count*recLen), want)
}
