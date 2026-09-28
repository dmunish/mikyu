package engine

import (
	"context"
	"errors"
	"fmt"

	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// Helper to construct a temporary test queue with custom parameters.
func newTestQueue(t *testing.T, maxSegBytes int64) (*Queue, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	if err != nil {
		t.Fatalf("failed to open offset store: %v", err)
	}

	cfg := testConfig()
	cfg.MaxSegment = maxSegBytes
	cfg.Durability = spec.DurabilityAsync // Async for fast test execution

	q, err := openQueue(dir, "compact-test-queue", cfg, store)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = q.Close()
		_ = store.Close()
	})
	return q, dir
}

// 1. Basic Truncation & Physical File Deletion
func TestCompact_BasicTruncationAndMetadata(t *testing.T) {
	ctx := t.Context()
	// Force small segment size so every ~3 messages create a new segment
	q, queueDir := newTestQueue(t, 256)

	payload := []byte("hello-compact-world-1234567890") // ~60 bytes encoded
	var offsets []uint64

	// Write 20 messages to cross multiple segment rolls
	for i := range 20 {
		off, err := q.Append(ctx, payload)
		if err != nil {
			t.Fatalf("Append failed at %d: %v", i, err)
		}
		offsets = append(offsets, off)
	}

	if q.MinOffset() != 0 {
		t.Fatalf("expected initial MinOffset to be 0, got %d", q.MinOffset())
	}

	initialSegs := q.Segments()
	if len(initialSegs) <= 1 {
		t.Fatalf("expected multiple segments, got %d", len(initialSegs))
	}

	// Target middle offset for truncation
	targetOffset := offsets[10]

	// Count physical .log files before truncation
	qPath := filepath.Join(queueDir, "compact-test-queue")
	entries, _ := os.ReadDir(qPath)
	logFilesBefore := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logFilesBefore++
		}
	}

	reclaimed, err := q.TruncateBefore(ctx, targetOffset)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}
	if reclaimed <= 0 {
		t.Fatalf("expected positive reclaimed bytes, got %d", reclaimed)
	}

	// Verify MinOffset advanced
	newMin := q.MinOffset()
	if newMin <= 0 || newMin > targetOffset {
		t.Fatalf("unexpected MinOffset after truncation: %d (target: %d)", newMin, targetOffset)
	}

	// Count physical .log files after truncation
	entries, _ = os.ReadDir(qPath)
	logFilesAfter := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logFilesAfter++
		}
	}

	if logFilesAfter >= logFilesBefore {
		t.Fatalf("expected physical file count to decrease: before=%d, after=%d", logFilesBefore, logFilesAfter)
	}
}

// 2. Read & ReadBatch Error Propagation
func TestCompact_ReadAndReadBatchErrOffsetTruncated(t *testing.T) {
	ctx := t.Context()
	q, _ := newTestQueue(t, 256)

	// Write 15 records
	for i := range 15 {
		_, err := q.Append(ctx, fmt.Appendf(nil, "payload-%d", i))
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Truncate up to offset 8
	_, err := q.TruncateBefore(ctx, 8)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	minOff := q.MinOffset()

	// Single Read on truncated offset
	_, err = q.Read(ctx, minOff-1)
	if !errors.Is(err, ErrOffsetTruncated) {
		t.Fatalf("expected ErrOffsetTruncated on Read(%d), got %v", minOff-1, err)
	}

	// ReadBatch starting at truncated offset
	_, err = q.ReadBatch(ctx, minOff-1, 5)
	if !errors.Is(err, ErrOffsetTruncated) {
		t.Fatalf("expected ErrOffsetTruncated on ReadBatch starting at %d, got %v", minOff-1, err)
	}

	// Valid Read beyond minOffset
	msg, err := q.Read(ctx, minOff)
	if err != nil {
		t.Fatalf("expected successful Read at %d, got %v", minOff, err)
	}
	if msg.Offset != minOff {
		t.Fatalf("expected offset %d, got %d", minOff, msg.Offset)
	}

	// Valid ReadBatch starting at minOffset
	msgs, err := q.ReadBatch(ctx, minOff, 3)
	if err != nil {
		t.Fatalf("expected successful ReadBatch at %d, got %v", minOff, err)
	}
	if len(msgs) == 0 || msgs[0].Offset != minOff {
		t.Fatalf("expected first message offset %d, got %v", minOff, msgs)
	}
}

// 3. Active Segment Protection
func TestCompact_ActiveSegmentProtection(t *testing.T) {
	ctx := t.Context()
	q, _ := newTestQueue(t, 1024*1024) // 1MB segment size (single active segment)

	for range 5 {
		_, _ = q.Append(ctx, []byte("active-segment-data"))
	}

	segsBefore := len(q.Segments())
	if segsBefore != 1 {
		t.Fatalf("expected 1 active segment, got %d", segsBefore)
	}

	// Attempt truncation past the highest written offset
	reclaimed, err := q.TruncateBefore(ctx, 100)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	if reclaimed != 0 {
		t.Fatalf("expected 0 reclaimed bytes when active segment cannot be truncated, got %d", reclaimed)
	}

	if len(q.Segments()) != 1 {
		t.Fatalf("active segment was incorrectly deleted")
	}

	if q.MinOffset() != 0 {
		t.Fatalf("MinOffset should remain 0, got %d", q.MinOffset())
	}
}

// 4. Ring Cache Eviction Protection
func TestCompact_RingCacheEviction(t *testing.T) {
	ctx := t.Context()
	q, _ := newTestQueue(t, 256)

	// Fill queue so payloads are in both ring cache and disk segments
	for i := range 10 {
		_, _ = q.Append(ctx, fmt.Appendf(nil, "cache-test-%d", i))
	}

	// Verify message 2 is hit from cache
	msg, ok := q.cache.Get(2)
	if !ok || msg.Offset != 2 {
		t.Fatalf("expected offset 2 in ring cache")
	}

	// Truncate up to offset 5
	_, err := q.TruncateBefore(ctx, 5)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	// Ring cache must reject reading evicted offset 2
	_, err = q.Read(ctx, 2)
	if !errors.Is(err, ErrOffsetTruncated) {
		t.Fatalf("expected ErrOffsetTruncated for evicted cache offset 2, got %v", err)
	}
}

// 5. Crash Recovery After Truncation
func TestCompact_CrashRecoveryAfterTruncation(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	store, err := openOffsetStore(dir)
	if err != nil {
		t.Fatalf("openOffsetStore failed: %v", err)
	}

	cfg := testConfig()
	cfg.MaxSegment = 256
	cfg.Durability = spec.DurabilityAsync

	q, err := openQueue(dir, "crash-queue", cfg, store)
	if err != nil {
		t.Fatalf("openQueue failed: %v", err)
	}

	for range 20 {
		_, _ = q.Append(ctx, []byte("crash-recovery-payload"))
	}

	_, err = q.TruncateBefore(ctx, 10)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}

	expectedMin := q.MinOffset()
	expectedLast := q.LastOffset()

	// Simulate sudden process restart / crash shutdown
	_ = q.Close()

	// Re-open queue from same directory
	qReopened, err := openQueue(dir, "crash-queue", cfg, store)
	if err != nil {
		t.Fatalf("re-opening queue failed: %v", err)
	}
	defer func() {
		_ = qReopened.Close()
		_ = store.Close()
	}()

	if qReopened.MinOffset() != expectedMin {
		t.Fatalf("reopened queue MinOffset mismatch: expected %d, got %d", expectedMin, qReopened.MinOffset())
	}

	if qReopened.LastOffset() != expectedLast {
		t.Fatalf("reopened queue LastOffset mismatch: expected %d, got %d", expectedLast, qReopened.LastOffset())
	}

	// Truncated offsets must still return ErrOffsetTruncated after reboot
	_, err = qReopened.Read(ctx, 0)
	if !errors.Is(err, ErrOffsetTruncated) {
		t.Fatalf("expected ErrOffsetTruncated after restart, got %v", err)
	}
}

// 6. High-Throughput Concurrent Stress Test (-race safe)
func TestCompact_ConcurrentAppendsReadsTruncates(t *testing.T) {
	// The context is a hang guard, not the stop mechanism: stopSignal ends the
	// run. A deadline near the 2.5s budget would fire while blocked appends and
	// reads were still draining, reporting cancellations as failures.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	q, _ := newTestQueue(t, 512) // Frequent rolling

	var (
		producedCount atomic.Uint64
		stopSignal    atomic.Bool
		wg            sync.WaitGroup
	)

	// Producer Goroutines
	for p := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for !stopSignal.Load() {
				payload := fmt.Appendf(nil, "concurrent-producer-%d-msg-%d", id, i)
				_, err := q.Append(ctx, payload)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					t.Errorf("producer %d failed: %v", id, err)
					return
				}
				producedCount.Add(1)
				i++
			}
		}(p)
	}

	// Consumer Batch Reader Goroutines
	for c := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for !stopSignal.Load() {
				minOff := q.MinOffset()
				lastOff := q.LastOffset()

				if lastOff > minOff {
					start := minOff + (lastOff-minOff)/2
					_, err := q.ReadBatch(ctx, start, 5)
					if err != nil && !errors.Is(err, ErrOffsetTruncated) && !errors.Is(err, ErrOffsetNotFound) {
						t.Errorf("consumer %d error: %v", id, err)
					}
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(c)
	}

	// Compactor Goroutine
	wg.Go(func() {
		for !stopSignal.Load() {
			time.Sleep(10 * time.Millisecond)
			lastOff := q.LastOffset()
			minOff := q.MinOffset()

			if lastOff > minOff+10 {
				target := minOff + (lastOff-minOff)/3
				_, err := q.TruncateBefore(ctx, target)
				if err != nil {
					t.Errorf("compactor error: %v", err)
				}
			}
		}
	})

	// Run for 2.5 seconds under load
	time.Sleep(2500 * time.Millisecond)
	stopSignal.Store(true)
	wg.Wait()

	if producedCount.Load() == 0 {
		t.Fatalf("no messages were produced during concurrent stress test")
	}

	if q.MinOffset() == 0 {
		t.Fatalf("expected MinOffset to advance during concurrent compaction")
	}
}

// 7. Age floor keeps a segment whose records straddle the cutoff
func TestFloorOlderThanSegmentBoundary(t *testing.T) {
	ctx := t.Context()
	q, _ := newTestQueue(t, 256)
	payload := []byte("floor-older-than-segment-boundary") // 31B -> 65B record, 3 per segment

	appendRec := func() {
		if _, err := q.Append(ctx, payload); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Segment A: records 0,1 — then the first cutoff.
	appendRec()
	appendRec()
	time.Sleep(5 * time.Millisecond)
	cutoffA := time.Now()
	time.Sleep(5 * time.Millisecond)

	// Record 2 lands in A after the cutoff: A straddles cutoffA.
	appendRec()
	// Record 3 rolls into segment B — then the second cutoff.
	appendRec()
	time.Sleep(5 * time.Millisecond)
	cutoffB := time.Now()
	time.Sleep(5 * time.Millisecond)

	// Records 4,5 complete B (straddling cutoffB); 6,7 roll into active C.
	appendRec()
	appendRec()
	appendRec()
	appendRec()

	segs := q.Segments()
	if len(segs) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(segs))
	}
	if segs[0].IsActive || segs[1].IsActive || !segs[2].IsActive {
		t.Fatalf("expected segments 0,1 closed and 2 active: %+v", segs)
	}

	// Cutoff inside A: its last record postdates the cutoff, so the whole
	// segment is kept despite containing older records.
	floor, err := q.FloorOlderThan(ctx, cutoffA)
	if err != nil {
		t.Fatalf("FloorOlderThan failed: %v", err)
	}
	if floor != 0 {
		t.Fatalf("straddling segment must be kept: floor = %d, want 0", floor)
	}

	// Cutoff strictly after A and inside B: A is dropped as a whole, B (which
	// straddles the cutoff) is kept.
	floor, err = q.FloorOlderThan(ctx, cutoffB)
	if err != nil {
		t.Fatalf("FloorOlderThan failed: %v", err)
	}
	if floor != segs[1].BaseOffset {
		t.Fatalf("floor = %d, want base of segment B (%d)", floor, segs[1].BaseOffset)
	}

	// A far-future cutoff makes every closed segment eligible; the walk stops
	// before the active segment.
	floor, err = q.FloorOlderThan(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("FloorOlderThan failed: %v", err)
	}
	if floor != segs[2].BaseOffset {
		t.Fatalf("floor = %d, want base of active segment (%d)", floor, segs[2].BaseOffset)
	}
}

// 8. Age floor never reaches the active segment
func TestFloorOlderThanActiveNeverDropped(t *testing.T) {
	ctx := t.Context()
	q, _ := newTestQueue(t, 256)
	payload := []byte("active-never-dropped-payload")

	for range 20 {
		if _, err := q.Append(ctx, payload); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	segs := q.Segments()
	if len(segs) < 3 {
		t.Fatalf("expected at least 3 segments, got %d", len(segs))
	}
	active := segs[len(segs)-1]
	if !active.IsActive {
		t.Fatalf("expected the last segment to be active")
	}

	// A far-future cutoff makes every closed segment eligible...
	floor, err := q.FloorOlderThan(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("FloorOlderThan failed: %v", err)
	}
	// ...but the floor must stop at the active segment.
	if floor != active.BaseOffset {
		t.Fatalf("floor = %d, want active segment base %d", floor, active.BaseOffset)
	}

	// Truncating at that floor removes only closed segments.
	reclaimed, err := q.TruncateBefore(ctx, floor)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}
	if reclaimed <= 0 {
		t.Fatalf("expected positive reclaimed bytes")
	}
	remaining := q.Segments()
	if len(remaining) != 1 || !remaining[0].IsActive {
		t.Fatalf("active segment was incorrectly deleted: %+v", remaining)
	}
	if q.MinOffset() != active.BaseOffset {
		t.Fatalf("MinOffset = %d, want %d", q.MinOffset(), active.BaseOffset)
	}
}

// 9. Size floor drops whole oldest segments only
func TestFloorForSizeDropsWholeSegments(t *testing.T) {
	ctx := t.Context()
	// Fsync keeps segment file sizes stable between the Segments() snapshot
	// and the FloorForSize call, so the cap computed here is exact.
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	if err != nil {
		t.Fatalf("openOffsetStore failed: %v", err)
	}
	cfg := testConfig()
	cfg.MaxSegment = 256

	q, err := openQueue(dir, "floor-for-size-queue", cfg, store)
	if err != nil {
		t.Fatalf("openQueue failed: %v", err)
	}
	t.Cleanup(func() {
		_ = q.Close()
		_ = store.Close()
	})
	payload := []byte("floor-for-size-payload")

	for range 16 {
		if _, err := q.Append(ctx, payload); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	segs := q.Segments()
	if len(segs) < 4 {
		t.Fatalf("expected at least 4 segments, got %d", len(segs))
	}

	// Cap that fits exactly the two newest segments (the active one included,
	// so never dropped): the floor must be the base of the first segment that
	// survives, the third-newest.
	var capBytes int64
	for i := len(segs) - 2; i < len(segs); i++ {
		capBytes += segs[i].SizeBytes
	}
	floor, err := q.FloorForSize(ctx, capBytes)
	if err != nil {
		t.Fatalf("FloorForSize failed: %v", err)
	}
	want := segs[len(segs)-2].BaseOffset
	if floor != want {
		t.Fatalf("floor = %d, want %d", floor, want)
	}
	// The floor is a segment boundary, never a mid-segment offset.
	found := false
	for _, s := range segs {
		if s.BaseOffset == floor {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("floor %d is not a segment boundary", floor)
	}

	reclaimed, err := q.TruncateBefore(ctx, floor)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}
	if reclaimed <= 0 {
		t.Fatalf("expected positive reclaimed bytes")
	}
	if q.MinOffset() != floor {
		t.Fatalf("MinOffset = %d, want %d", q.MinOffset(), floor)
	}
	if got := len(q.Segments()); got != 2 {
		t.Fatalf("expected 2 segments after truncation, got %d", got)
	}
}

// 10. Size floor never drops the active segment
func TestFloorForSizeNeverActive(t *testing.T) {
	ctx := t.Context()
	q, _ := newTestQueue(t, 1024*1024) // single active segment

	for range 5 {
		if _, err := q.Append(ctx, []byte("never-active-payload")); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	segs := q.Segments()
	if len(segs) != 1 || !segs[0].IsActive {
		t.Fatalf("expected a single active segment, got %+v", segs)
	}

	// A cap smaller than the queue itself must not drop the active segment.
	floor, err := q.FloorForSize(ctx, 1)
	if err != nil {
		t.Fatalf("FloorForSize failed: %v", err)
	}
	if floor != q.MinOffset() {
		t.Fatalf("floor = %d, want MinOffset %d", floor, q.MinOffset())
	}
	reclaimed, err := q.TruncateBefore(ctx, floor)
	if err != nil {
		t.Fatalf("TruncateBefore failed: %v", err)
	}
	if reclaimed != 0 {
		t.Fatalf("expected 0 reclaimed bytes, got %d", reclaimed)
	}
	if len(q.Segments()) != 1 {
		t.Fatalf("active segment was incorrectly deleted")
	}

	// A non-positive cap is a no-op.
	floor, err = q.FloorForSize(ctx, 0)
	if err != nil {
		t.Fatalf("FloorForSize failed: %v", err)
	}
	if floor != q.MinOffset() {
		t.Fatalf("floor = %d, want MinOffset %d", floor, q.MinOffset())
	}
}
