package engine

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

type pendingEntry struct {
	pos int64
	len int64
}

type asyncEntry struct {
	seg    *segment
	rec    []byte
	pos    int64
	recLen int64
	offset uint64
	buf    *pooledBuf
}

// wal writes segment bytes through a sink. Two implementations share the
// struct and are chosen at construction: the syscall WAL stages sync batches
// in byte-budgeted buffers behind one flusher goroutine with micro-linger
// group commits, and hands async batches to the sink's drain loop; the ring
// WAL (hosts with io_uring) submits every batch directly to the shared ring
// and has no flusher at all — one reaper per data directory completes every
// queue's operations.
//
// Single writer: exactly one goroutine writes the segment files — the
// syscall WAL's syncLoop for staged batches and the sink's drain loop for
// async batches, or the ring reaper. Producers only stage records via
// push/pushBatch (sync path) or pushAsyncBatch (async path). Sequencing is
// byte-based: pushedBytes and publishedBytes count staged and durable bytes,
// and wait(seq) blocks until publishedBytes reaches the byte offset the push
// returned. Async backpressure is bounded by the WAL's channel depth
// (MaxFlushBytes/4096) or the ring path's slot semaphore.
type wal struct {
	cfg  Config
	sink sink
	ring *uringRing // nil on the syscall path
	m    *walMetrics

	mu             sync.Mutex
	cond           *sync.Cond
	err            error
	asyncNext      uint64
	pushedBytes    int64
	publishedBytes int64
	pendingBytes   int64

	// syscall-path state: staged sync batches, the flusher's double buffers,
	// and the producer-to-flusher async handoff.
	buf           []byte
	pending       []pendingEntry
	swapBuf       []byte
	swapPending   []pendingEntry
	asyncChan     chan []asyncEntry
	wake          chan struct{}
	configChanged chan struct{}

	// ring-path state: a slot semaphore bounds outstanding batches, and a
	// submission-sequence watermark turns out-of-order ring completions back
	// into the byte-based durable guarantee.
	slots      chan struct{}
	inflightWG sync.WaitGroup
	submitSeq  uint64
	watermark  uint64
	completed  map[uint64]int64

	shutdown chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// newWAL builds the WAL for one segment; ring may be nil on hosts without io_uring.
func newWAL(seg *segment, cfg Config, startOffset uint64, ring *uringRing, scopes ...*metrics.Scope) *wal {
	if ring != nil && os.Getenv("MIKYU_FORCE_SYSCALL_WAL") == "" {
		return newRingWAL(seg, cfg, startOffset, ring, scopes...)
	}
	return newSyscallWAL(seg, cfg, startOffset, scopes...)
}

func (w *wal) start() {
	if w.ring != nil {
		w.startRing()
		return
	}
	w.startSyscall()
}

func (w *wal) push(rec []byte, pos int64) (seq int64, wake bool, err error) {
	if w.ring != nil {
		return w.pushRing(rec, pos)
	}
	return w.pushSyscall(rec, pos)
}

func (w *wal) pushBatch(recs [][]byte, positions []int64, idxStart int64) (seq int64, wake bool, err error) {
	if w.ring != nil {
		return w.pushBatchRing(recs, positions, idxStart)
	}
	return w.pushBatchSyscall(recs, positions)
}

func (w *wal) pushAsync(ctx context.Context, e asyncEntry) error {
	return w.pushAsyncBatch(ctx, []asyncEntry{e})
}

func (w *wal) pushAsyncBatch(ctx context.Context, entries []asyncEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if w.ring != nil {
		return w.pushAsyncBatchRing(ctx, entries, int64(entries[0].offset-entries[0].seg.baseOff)*rawIndexEntrySize)
	}
	return w.pushAsyncBatchSyscall(ctx, entries)
}

// UpdateConfig swaps the WAL's byte budget and timing parameters.
func (w *wal) UpdateConfig(maxFlushBytes int64, maxFlushWait, flushLinger time.Duration) {
	if w.ring != nil {
		w.updateConfigRing(maxFlushBytes, maxFlushWait, flushLinger)
		return
	}
	w.updateConfigSyscall(maxFlushBytes, maxFlushWait, flushLinger)
}

func (w *wal) stop() error {
	if w.ring != nil {
		return w.stopRing()
	}
	return w.stopSyscall()
}

// ownsIndex reports whether the WAL's sink writes the segment's index entries alongside the log bytes.
func (w *wal) ownsIndex() bool {
	return w.ring != nil
}

// bytes returns the WAL's committed, flushed, and staged byte counts.
func (w *wal) bytes() (pushed, published, pending int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pushedBytes, w.publishedBytes, w.pendingBytes
}

// channelFill reports the async backlog occupancy as a fraction in [0,1].
func (w *wal) channelFill() float64 {
	if w.ring != nil {
		if capacity := cap(w.slots); capacity > 0 {
			return min(float64(len(w.slots))/float64(capacity), 1)
		}
		return 0
	}
	if capacity := cap(w.asyncChan); capacity > 0 {
		return min(float64(len(w.asyncChan))/float64(capacity), 1)
	}
	return 0
}

// byteFill reports staged WAL bytes as a fraction of the flush budget.
func (w *wal) byteFill() float64 {
	w.mu.Lock()
	pending, maxBytes := w.pendingBytes, w.cfg.MaxFlushBytes
	w.mu.Unlock()
	if maxBytes <= 0 {
		return 0
	}
	return min(float64(pending)/float64(maxBytes), 1)
}

// wait blocks until every byte staged before seq is durable.
func (w *wal) wait(seq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.publishedBytes < seq && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *wal) currentError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *wal) setError(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}
