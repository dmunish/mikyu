package engine

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

// TestOverlappedSingleSyscall drives one large batch through a live queue and
// requires the WAL to reach the kernel at most eight times regardless of the
// underlying handle mode.
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
	w := newWAL(seg.logFile, cfg, 0)

	var writes []span
	writeAtHook = func(off int64, n int) {
		writes = append(writes, span{off, int64(n)})
	}
	t.Cleanup(func() { writeAtHook = nil })

	const count = 1000
	const recLen = 8
	for i := range count {
		e := asyncEntry{
			seg:    seg,
			rec:    make([]byte, recLen),
			pos:    int64(i) * recLen,
			recLen: recLen,
			offset: uint64(i),
		}
		require.NoError(t, w.pushAsync(t.Context(), e))
	}
	w.start()
	waitWALDrained(t, w, count)
	require.NoError(t, w.stop())

	require.LessOrEqual(t, len(writes), 8, "1000 entries must reach the kernel in at most eight writes")
	sort.Slice(writes, func(i, j int) bool { return writes[i].off < writes[j].off })
	var want int64
	for _, s := range writes {
		require.Equal(t, want, s.off)
		want += s.n
	}
	require.Equal(t, int64(count*recLen), want)
}
