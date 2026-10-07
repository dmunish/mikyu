# Plan: a `sink` abstraction, then io_uring submission

Below is the full plan. Phase 1 is a refactor that ships on its own. Phase 2 changes performance characteristics and is a real rewrite of the WAL's async submission model — not just a syscall swap.

I want to be honest up front: **for async durability, io_uring's win is small (single-digit percent) and mostly comes from deleting the channel, not from the syscall itself.** The larger, less obvious win is **goroutine consolidation**: replacing N per-queue flusher goroutines with one reaper per data directory. That's the phase 2 change worth doing. I've designed the plan so that phase 1 introduces exactly the seams phase 2 needs, and nothing more.

The plan targets **Go 1.27**. Since the development host is Windows (where io_uring is unavailable), the ring path is implemented but never exercised locally; it is validated on Linux CI, and the portable path is what ships to Windows. There is no benchmark gate: phase 2 lands behind the ring-availability check, and operators on non-Linux hosts get the unchanged syscall WAL.

---

## 1. Design rules this plan follows

These are the rules the repo already obeys; the plan is built to preserve them.

1. **One concept per file.** `sink.go` has the interface and the async implementation. `sink_sync.go` has the portable sync implementation. `sink_sync_linux.go` has only the Linux constructor. `uring_linux.go` has only the ring.
2. **Build tags select at the constructor, never at the call site.** No `if runtime.GOOS == "linux"` anywhere. No `//go:build` in `wal.go`, `manager.go`, or `queue.go`.
3. **The WAL's vocabulary is about the WAL.** After this change, the WAL's public shape mentions `sink`, `flush`, `stop`, `wait`. It does not mention `pwrite`, `fdatasync`, `uring`, `syscall`, or `Durability`.
4. **Fallback is silent.** io_uring unavailability is a host property, not a caller error. Every entry point that could fail falls back to the syscall path without surfacing the ring at all.
5. **Errors originate where they are caused.** A CQE with `res < 0` is `-errno`; the ring translates it to a `syscall.Errno` before it reaches the WAL. The WAL never sees an io_uring-specific error type.
6. **Tests are the contract.** Every existing test in `wal_test.go`, `durability_test.go`, `overlapped_test.go`, `append_test.go`, `append_async_test.go` must pass unchanged on the syscall path. New tests cover the ring-specific paths.
7. **One ring per data directory.** The ring is a property of a `manager.Manager` (a `log/` and `task/` pair under one root), not of an `engine.Manager`. This is what the "one reaper per data directory" claim in the intro requires.

---

## 2. Architecture at a glance

**Before**

```
Queue.AppendBatch
  ├─ encode into pooledBuf
  ├─ commitSync  ──── wal.pushBatch ──── w.buf += bytes
  │                                        w.pending += entry
  │                                        w.cond.Broadcast()
  │                                        (flusher picks up)
  │
  └─ commitAsync ──── wal.pushAsyncBatch ─ channel send
                                            (flusher picks up)
                                            (flusher coalesces runs)
                                            (flusher writes log + index)
                                            (flusher advances asyncNext)

wal flusher goroutine (per queue)
  ├─ select on: asyncChan / ticker / wake / configChanged / shutdown
  ├─ swap buffers
  ├─ writeAt(log)  ── pwrite
  ├─ writeAt(index) ── pwrite
  └─ syncData(log) ── fdatasync (sync mode only)
```

**After**

```
manager.Manager  ──── uringRing (one per data directory; nil on non-Linux)

Queue.AppendBatch
  ├─ encode into pooledBuf
  ├─ submit ──────── wal.submit ──────── sink.Write(batch, done)
  │                                        sink owns both files of one segment
  │                                        ring.Submit([WRITE log, WRITE index] linked)
  │                                        (assign tag, append to pending)
  │
  ├─ (sync mode) wal.waitTag(tag) ─────── handle.Wait(tag)
  │                                        (blocks on cond until reaper fires)
  │
  └─ (async mode) return immediately

ring reaper goroutine (per data directory, not per queue)
  ├─ io_uring_enter(GETEVENTS)  ── blocking wait for ≥1 CQE
  ├─ read CQEs
  ├─ dispatch each CQE to its sink handle
  └─ handle fires its cond
       ├─ async: WAL's done callback advances asyncNext, recycles buffers
       └─ sync:  WAL's waiter wakes
```

The queue's `reservations` FIFO and the `publishedOffset` / `asyncNext` state machine are **unchanged**. They are the WAL's ordering guarantees and stay exactly where they are.

---

## 3. File inventory

**New files (7):**

| File | Build tag | Contents |
|---|---|---|
| `internal/engine/sink.go` | none | `sink` interface, `sinkBatch`, `sinkWrite`, `sinkIndex`, `sinkDone`, `newSink`, `asyncSink` |
| `internal/engine/sink_sync.go` | none | `syncSink` (portable `pwrite`+`fdatasync` over both files) |
| `internal/engine/sink_sync_linux.go` | `linux` | `newDurableSink` → `uringSink` or `syncSink`; `uringSink` |
| `internal/engine/sink_sync_other.go` | `!linux` | `newDurableSink` → `syncSink` |
| `internal/engine/uring.go` | none | `uringOp`, `uringOpKind`, `uringCompletion`, `errUringUnsupported` |
| `internal/engine/uring_linux.go` | `linux` | `uringRing`, `uringSinkHandle`, `newUringRing`, reaper |
| `internal/engine/uring_other.go` | `!linux` | stubs so the package compiles |

**Modified files (4):**

| File | Change |
|---|---|
| `internal/engine/wal.go` | Field `file *os.File` → `sink sink`. `newWAL` builds the sink. `flush` calls `w.sink.Write`. `stop` closes the sink. `pendingEntry` gains `idxPos`. `pushBatch`/`pushAsyncBatch` gain an `idxStart int64` argument. |
| `internal/engine/manager.go` | Open the ring after `os.OpenRoot`; pass it to `openQueue`; close it on `Manager.Close`. Ring is per-`manager.Manager`. |
| `internal/engine/queue.go` | Thread `*uringRing` from the manager to `newWAL`. Compute `idxStart` for each staged batch. Skip the direct `writeIndexBatchAt` when the WAL owns index writes. |
| `internal/engine/segment.go` | `writeIndexAt`/`writeIndexBatchAt` gain a `ringSink`-aware variant, or move to a shared helper used by both sink and queue. Implementation detail; the segment's on-disk format is unchanged. |

**Unchanged (deliberately):** `append.go` (except one call site), `fetch.go`, `read.go`, `compact.go`, `compaction.go`, `offsets.go`, `metrics.go`, `storage.go`. All existing WAL tests pass unchanged on the syscall path.

---

## 4. Phase 1 — the sink abstraction

This phase is a pure refactor. On non-Linux, and on Linux with `MIKYU_DISABLE_URING=1`, behavior is bit-identical to today. It ships on its own and buys a clean seam.

### 4.1 `internal/engine/sink.go`

```go
package engine

import (
	"os"
	"sync"
)

// sink is the WAL's durable byte target for one segment: log records in, and
// — when the sink owns index writes — the segment's .idx entries alongside
// them, out. It owns the two files that segment writes through.
//
// Write returns once the batch is durable (sync mode) or queued (async mode).
// Async sinks invoke done when the batch's bytes are visible; sync sinks
// never invoke done. The byte slices in a batch must remain valid until done
// runs (async) or until Write returns (sync).
//
// Records and Index are made durable under one barrier. A sink that owns
// index writes records them at the same time as the log bytes; a sink that
// does not ignores Index entirely.
type sink interface {
	Write(batch sinkBatch, done sinkDone) error
	Sync() error
	Close() error
}

// sinkBatch is one producer batch: the log records to append, plus any index
// entries that must become durable with them.
type sinkBatch struct {
	Records []sinkWrite
	Index   sinkIndex
}

type sinkWrite struct {
	P   []byte
	Off int64
}

// sinkIndex describes the index slots to write for a batch of records. Empty
// Positions means the caller keeps index writes to itself; a sink that owns
// index writes only writes the index when Positions is non-empty.
type sinkIndex struct {
	Start     int64   // byte offset in the .idx file of the first entry
	Positions []int64 // log positions, one per index entry
	Lengths   []int64 // log lengths, one per index entry
}

// empty reports whether there are no index entries to write.
func (s sinkIndex) empty() bool { return len(s.Positions) == 0 }

// sinkDone runs once per async batch, on any goroutine. A non-nil error means
// the batch failed; the WAL must record it and wake every waiter.
type sinkDone func(err error)

// newSink returns the sink a WAL with the given durability needs. A durable
// queue gets the platform's best sync sink; an async queue gets the portable
// write-only one. ring may be nil (unsupported host, disabled by env, or
// !linux) and durable sinks fall back silently. log and idx are the two files
// of the segment this WAL writes.
func newSink(log, idx *os.File, durable bool, ring *uringRing) sink {
	if !durable {
		return newAsyncSink(log, idx)
	}
	return newDurableSink(log, idx, ring)
}

// --- async sink -------------------------------------------------------------

// asyncSink batches writes onto a channel and lets a single flusher goroutine
// drain them. This is the shape the WAL has always used for async durability;
// phase 2 replaces it on hosts with io_uring.
type asyncSink struct {
	log     *os.File
	idx     *os.File
	ch      chan asyncBatch
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	stopOnce sync.Once
}

type asyncBatch struct {
	records []sinkWrite
	index   sinkIndex
	done    sinkDone
}

func newAsyncSink(log, idx *os.File) *asyncSink {
	s := &asyncSink{
		log:  log,
		idx:  idx,
		ch:   make(chan asyncBatch, asyncSinkDepth),
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go s.loop()
	return s
}

func (s *asyncSink) Write(batch sinkBatch, done sinkDone) error {
	select {
	case s.ch <- asyncBatch{records: batch.Records, index: batch.Index, done: done}:
		return nil
	case <-s.stop:
		return ErrQueueClosed
	}
}

func (s *asyncSink) Sync() error  { return syncData(s.log) }

func (s *asyncSink) Close() error {
	s.stopOnce.Do(func() {
		close(s.stop)
		<-s.done
	})
	return nil
}

// loop is phase 1's placeholder for what phase 2 turns into a ring reaper.
// It writes each batch's log records and, when the batch carries index
// entries, its index entries too, then fires the batch's done callback.
func (s *asyncSink) loop() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case b := <-s.ch:
			// ...
			s.writeBatch(b)
		}
	}
}
```

Note that `asyncBatch` has an `index` field that phase 1 never populates. Phase 2's ring sink uses it to submit the index write as a second SQE under the same tag. Leaving the field in place now means phase 2's diff to this file is small.

`asyncSinkDepth` is the channel depth; the WAL's `asyncChan` depth heuristic (`MaxFlushBytes / 4096`) moves here and becomes an explicit constant the WAL passes in.

### 4.2 `internal/engine/sink_sync.go`

```go
package engine

import "os"

// syncSink is the portable sync-durability sink: pwrite then fdatasync. It
// is the fallback on every platform, including Linux hosts where io_uring is
// unavailable (old kernel, seccomp, io_uring_disabled, MIKYU_DISABLE_URING).
type syncSink struct {
	log *os.File
	idx *os.File
}

func (s *syncSink) Write(batch sinkBatch, _ sinkDone) error {
	for _, w := range batch.Records {
		if _, err := s.log.WriteAt(w.P, w.Off); err != nil {
			return err
		}
	}
	if !batch.Index.empty() {
		if err := writeIndexBatch(s.idx, batch.Index); err != nil {
			return err
		}
	}
	return syncData(s.log)
}

func (s *syncSink) Sync() error  { return syncData(s.log) }
func (s *syncSink) Close() error { return nil }
```

`writeIndexBatch` is a small helper shared by both files: it packs consecutive `rawIndexEntry` records and calls `WriteAt` once. It is the free function the segment's `writeIndexBatchAt` delegates to, so both the queue-side path (phase 1) and the sink-side path (phase 2) share one implementation and one `indexWriteHook` site.

### 4.3 `internal/engine/sink_sync_linux.go`

```go
//go:build linux

package engine

import "os"

// newDurableSink returns an io_uring-backed sink when a ring is available and
// the portable sync sink otherwise. A nil ring (no io_uring on this host, or
// the Manager chose not to open one) falls back silently.
func newDurableSink(log, idx *os.File, ring *uringRing) sink {
	if ring == nil {
		return &syncSink{log: log, idx: idx}
	}
	return &uringSink{log: log, idx: idx, handle: ring.Register()}
}

// uringSink submits one tag per Write: WRITE(log), WRITE(index) linked, then
// FSYNC(log). Write returns once every completion for the tag has arrived.
type uringSink struct {
	log    *os.File
	idx    *os.File
	handle *uringSinkHandle
}

func (s *uringSink) Write(batch sinkBatch, _ sinkDone) error {
	tag := s.handle.NextTag()
	ops := make([]uringOp, 0, len(batch.Records)+2)
	for i, w := range batch.Records {
		ops = append(ops, uringOp{
			Kind: uringOpWrite, File: s.log, Buf: w.P, Off: w.Off,
			// Link every record write to the next operation, so the index
			// write and the fsync cannot be reordered ahead of the log data.
			Link: true,
			Tag:  tag,
		})
		_ = i
	}
	if !batch.Index.empty() {
		ops = append(ops, uringOp{
			Kind: uringOpWrite, File: s.idx,
			Buf: encodeIndexEntries(batch.Index), Off: batch.Index.Start,
			Link: true,
			Tag:  tag,
		})
	}
	ops[len(ops)-1].Link = false
	ops = append(ops, uringOp{Kind: uringOpFsync, File: s.log, DataSync: true, Tag: tag})
	if err := s.handle.Submit(ops); err != nil {
		return err
	}
	return s.handle.Wait(tag)
}

func (s *uringSink) Sync() error  { return s.handle.Ring().Fsync(s.log) }
func (s *uringSink) Close() error { return s.handle.Close() }
```

The exact shape of `Submit` and the index pairing is spelled out in §5.

### 4.4 `internal/engine/sink_sync_other.go`

```go
//go:build !linux

package engine

import "os"

func newDurableSink(log, idx *os.File, _ *uringRing) sink {
	return &syncSink{log: log, idx: idx}
}
```

### 4.5 `internal/engine/uring.go` — shared types, no build tag

```go
package engine

import (
	"errors"
	"os"
)

var errUringUnsupported = errors.New("io_uring unavailable")

type uringOpKind uint8

const (
	uringOpWrite uringOpKind = iota
	uringOpFsync
)

// uringOp is one submission. Fields unused by a Kind are ignored. Tag is
// assigned by the caller's uringSinkHandle and echoed on completion.
type uringOp struct {
	Kind     uringOpKind
	File     *os.File
	Buf      []byte
	Off      int64
	DataSync bool
	// Link requests IOSQE_IO_LINK: the kernel chains this operation to the
	// next SQE in the same submission. A linked operation that fails cancels
	// every operation linked after it.
	Link bool
	Tag  uint32
}

// uringCompletion is one finished operation. Res is bytes written for a
// Write, zero for an Fsync, or a negative errno (translated to syscall.Errno
// by the ring before the sink sees it).
type uringCompletion struct {
	Tag uint32
	Res int64
}

// uringRing and uringSinkHandle are defined in uring_linux.go (real) and
// uring_other.go (stub); their method sets are identical.
```

### 4.6 `internal/engine/uring_linux.go` — the ring

The full file, minus boilerplate, is in the appendix (§A). The essential structure:

```go
//go:build linux

package engine

const (
	uringEntries = 4096 // power of two; bounds outstanding SQEs
	uringSqeSize = 64
	uringCqeSize = 16
	uringCqBatch = 256 // CQEs drained per reaper pass
)

type uringRing struct {
	fd     int
	sqRing []byte
	cqRing []byte
	sqes   []byte
	sqHead *uint32
	sqTail *uint32
	sqMask uint32
	cqHead *uint32
	cqTail *uint32
	cqMask uint32
	cqesOff uintptr

	submitMu sync.Mutex // guards sqTail advance and SQE writes

	// handles is the sink-id table. Slot i is the *uringSinkHandle registered
	// with id i, or nil. The id is recycled through a free list so a long
	// running process with many queue open/close cycles does not exhaust the
	// 32-bit id space.
	mu     sync.Mutex
	handles []*uringSinkHandle
	free    []uint32

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// newUringRing tries io_uring_setup; on any failure it returns
// (nil, errUringUnsupported). Callers treat a nil ring as "no io_uring".
func newUringRing() (*uringRing, error) { ... }

func (r *uringRing) Submit(ops []uringOp) error { ... }
func (r *uringRing) Register() *uringSinkHandle { ... }
func (r *uringRing) Unregister(h *uringSinkHandle) { ... }
func (r *uringRing) Close() error { ... }
func (r *uringRing) loop() { /* reaper */ }
func (r *uringRing) dispatch(cqe uringCqe) { ... }
```

The `loop` is the reaper goroutine:

```go
func (r *uringRing) loop() {
	defer close(r.done)
	var cqes [uringCqBatch]uringCqe
	for {
		n, err := r.waitCQEs()
		if err != nil {
			select {
			case <-r.stop:
				return
			default:
				continue // EINTR or a transient enter failure; retry
			}
		}
		got := r.readCQEs(cqes[:n]) // drain up to uringCqBatch
		for i := range got {
			r.dispatch(got[i])
		}
	}
}
```

`dispatch` walks the CQE's `user_data` — the high 32 bits are the sink id, the low 32 bits are the tag — and calls `handle.complete(tag, res)`. A CQE whose sink id is no longer registered (because its handle was closed concurrently) is dropped: `Wait` has already returned `ErrQueueClosed` for the tag, and no one is listening.

```go
func (r *uringRing) dispatch(cqe uringCqe) {
	sinkID := uint32(cqe.userData >> 32)
	tag := uint32(cqe.userData)
	r.mu.Lock()
	if int(sinkID) >= len(r.handles) {
		r.mu.Unlock()
		return
	}
	h := r.handles[sinkID]
	r.mu.Unlock()
	if h != nil {
		h.complete(tag, cqe.res)
	}
}
```

`uringSinkHandle` is where the concurrency lives:

```go
type uringSinkHandle struct {
	ring *uringRing
	id   uint32
	next atomic.Uint32

	mu     sync.Mutex
	cond   *sync.Cond
	pend   map[uint32]int32 // tag -> outstanding completions
	res    map[uint32]int64 // tag -> first error (0 = ok)
	closed bool
}

// NextTag returns a fresh tag for the next submission. Tags are monotonic
// per handle; a tag is only looked up between Submit and the matching Wait,
// so map entries do not accumulate over the handle's lifetime.
func (h *uringSinkHandle) NextTag() uint32 { return h.next.Add(1) - 1 }

func (h *uringSinkHandle) Wait(tag uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.pend[tag] > 0 && !h.closed {
		h.cond.Wait()
	}
	if h.closed {
		return ErrQueueClosed
	}
	err := h.res[tag]
	delete(h.pend, tag)
	delete(h.res, tag)
	if err != 0 {
		return syscall.Errno(-err)
	}
	return nil
}

// complete is called by the reaper. It decrements the tag's outstanding
// count and signals Wait when it reaches zero. The first error wins: later
// completions for the same tag are ignored once res[tag] is non-zero.
func (h *uringSinkHandle) complete(tag uint32, res int64) {
	h.mu.Lock()
	if n, ok := h.pend[tag]; ok && n > 0 {
		h.pend[tag] = n - 1
	}
	if res < 0 && h.res[tag] == 0 {
		h.res[tag] = res
	}
	if h.pend[tag] == 0 {
		h.cond.Signal()
	}
	h.mu.Unlock()
}
```

`Submit` registers the tag before `ring.Submit` is called, and rolls the registration back if `ring.Submit` fails. This is the failure path that keeps a producer from blocking forever after a transient `io_uring_enter` error:

```go
func (h *uringSinkHandle) Submit(ops []uringOp) error {
	if len(ops) == 0 {
		return nil
	}
	tag := ops[0].Tag
	h.mu.Lock()
	h.pend[tag] += int32(len(ops))
	h.mu.Unlock()
	if err := h.ring.Submit(ops); err != nil {
		h.mu.Lock()
		h.pend[tag] -= int32(len(ops))
		if h.pend[tag] <= 0 {
			delete(h.pend, tag)
			delete(h.res, tag)
		}
		h.mu.Unlock()
		return err
	}
	return nil
}
```

`Close` marks the handle closed, wakes every waiter with `ErrQueueClosed`, and deregisters from the ring. Deregistration happens under the ring's mutex; concurrent completions for the handle see `closed` and are dropped.

```go
func (h *uringSinkHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.cond.Broadcast()
	h.mu.Unlock()
	h.ring.Unregister(h)
	return nil
}
```

That's the whole concurrency story. One mutex per handle, one condition variable, two maps that shrink to zero on every completed submission. No global locks beyond the ring's submit lock and the ring's handle table.

### 4.7 `internal/engine/uring_other.go` — the stub

```go
//go:build !linux

package engine

// uringRing is a stub on every platform without io_uring. newUringRing
// always fails, so Manager.ring is always nil, and durable sinks always
// take the syncSink path.
type uringRing struct{}
type uringSinkHandle struct{}

func newUringRing() (*uringRing, error) { return nil, errUringUnsupported }

func (*uringRing) Submit([]uringOp) error         { return errUringUnsupported }
func (*uringRing) Register() *uringSinkHandle     { return nil }
func (*uringRing) Unregister(*uringSinkHandle)    {}
func (*uringRing) Close() error                   { return nil }
func (*uringSinkHandle) NextTag() uint32          { return 0 }
func (*uringSinkHandle) Wait(uint32) error        { return errUringUnsupported }
func (*uringSinkHandle) Submit([]uringOp) error   { return errUringUnsupported }
func (*uringSinkHandle) Close() error             { return nil }
```

### 4.8 `wal.go` changes (phase 1)

Four edits.

**Field swap.** `file *os.File` is deleted; `sink sink` replaces it. The `wal` no longer holds a raw file handle. `newWAL` takes the segment (`*segment`) so it can hand both files to the sink, plus the ring.

**`newWAL` builds the sink.**

```go
func newWAL(seg *segment, cfg Config, startOffset uint64, ring *uringRing, scopes ...*metrics.Scope) *wal {
	flushBytes := max(cfg.MaxFlushBytes, 4096)
	w := &wal{
		cfg:           cfg,
		sink:          newSink(seg.logFile, seg.idxFile, !cfg.Async(), ring),
		asyncNext:     startOffset,
		// ... unchanged fields ...
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}
```

Callers in `queue.go` change from `newWAL(q.active.logFile, q.cfg, q.nextOffset, q.scope)` to `newWAL(q.active, q.cfg, q.nextOffset, q.ring, q.scope)`.

**`flush` collapses.**

```go
func (w *wal) flush() {
	w.mu.Lock()
	if len(w.buf) == 0 || w.err != nil {
		w.mu.Unlock()
		return
	}
	maxBytes := w.cfg.MaxFlushBytes

	data := w.buf
	entries := w.pending

	w.buf = w.swapBuf[:0]
	w.pending = w.swapPending[:0]
	w.pendingBytes = 0

	select {
	case <-w.wake:
	default:
	}

	w.mu.Unlock()

	start := time.Now()
	batch := sinkBatch{
		Records: make([]sinkWrite, 0, len(entries)),
	}
	for _, e := range entries {
		batch.Records = append(batch.Records, sinkWrite{P: data[e.offset : e.offset+e.len], Off: e.pos})
	}
	// Phase 1 leaves Index empty; the queue writes index entries itself.
	writeErr := w.sink.Write(batch, nil)
	w.m.flushes.Inc()
	w.m.flushDur.Observe(time.Since(start).Seconds())

	w.mu.Lock()
	if writeErr != nil {
		w.err = writeErr
	} else {
		w.publishedBytes += int64(len(data))
	}
	if int64(cap(data)) > 2*maxBytes {
		w.swapBuf = make([]byte, 0, maxBytes)
	} else {
		w.swapBuf = data[:0]
	}
	// ... swapPending swap, exactly as today
	w.m.pending.Set(int64(len(w.pending)))
	w.cond.Broadcast()
	w.mu.Unlock()
}
```

`writerHook` and `writeAtHook` move into `syncSink.Write` and `asyncSink.writeBatch`. `TestSingleWriter` and `TestOverlappedSingleSyscall` continue to observe the same invariants: one goroutine writes the log, and 1000 records land in at most eight `WriteAt` calls.

**`stop` closes the sink.**

```go
func (w *wal) stop() error {
	w.stopOnce.Do(func() {
		close(w.shutdown)
		<-w.done
		w.setError(w.sink.Close())
	})
	return w.err
}
```

That's it. Every existing WAL test passes unchanged on phase 1.

### 4.9 `manager.go` and `queue.go` changes (phase 1)

`Manager` gains a `ring *uringRing` field. `Open` opens it after `os.MkdirAll` and before the two `engine.OpenManager` calls, and passes it to both:

```go
func Open(dir string, defaultCfg engine.Config, perQueue map[Key]engine.Config, regs ...*metrics.Registry) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ring, _ := newUringRing() // nil on unsupported hosts; no error

	logMgr, err := engine.OpenManager(filepath.Join(dir, KindLog.String()), defaultCfg, logCfg, ring, regs...)
	if err != nil {
		closeRing(ring)
		return nil, err
	}
	taskMgr, err := engine.OpenManager(filepath.Join(dir, KindTask.String()), defaultCfg, taskCfg, ring, regs...)
	if err != nil {
		_ = logMgr.Close()
		closeRing(ring)
		return nil, err
	}
	return &Manager{dir: dir, log: logMgr, task: taskMgr, ring: ring}, nil
}

func closeRing(r *uringRing) {
	if r != nil {
		_ = r.Close()
	}
}
```

`Close` closes the ring last, after both engine managers have closed every queue (and therefore every sink handle). The reaper's ring fd is not closed until every sink has been unregistered.

`engine.Manager` stores the ring and passes it to `openQueue`; `openQueue` stores it on the `Queue` and passes it to `newWAL`. That's a signature change propagated through `Create`, `openQueue`, and `newWAL` — six call sites, all mechanical.

---

## 5. Phase 2 — direct submission

This is the phase that changes performance. It is not a small diff to `wal.go`. Read the whole section before starting.

### 5.1 What it replaces

The current async path uses:

- `asyncChan chan []asyncEntry` — the producer-to-flusher handoff.
- `swapBuf` / `swapPending` — the flusher's double-buffer for the coalesced write.
- `wake chan struct{}` — a signal from sync producers to the flusher.
- One flusher goroutine per WAL, driven by a ticker, a wake channel, and a shutdown channel.

Phase 2 removes all of these on hosts with a ring. The producer submits SQEs directly. A reaper per data directory completes them. The `sink` interface stays exactly as phase 1 defined it — `Write(batch, done)` — but the async sink implementation becomes `uringAsyncSink`, and the sync sink becomes a ring-backed writer that also batched index writes into the same tag.

### 5.2 What it does not replace

- `reservations` and the FIFO drain logic in `queue.go` — unchanged.
- `publishedOffset`, `asyncNext`, `w.cond` — unchanged. The reaper calls the same `done` callback that advances them.
- `pooledBuf` recycling — unchanged in principle; the release moves from the flusher to the reaper, but the pool is the same.
- The sync path's `wait(seq)` semantics — unchanged; sync producers now wait on a ring tag instead of a byte sequence, but the observable behavior is identical.

### 5.3 The new async sink

Replace `asyncSink.loop` with a per-WAL `uringAsyncSink`. This is the only file whose implementation changes materially in phase 2.

```go
//go:build linux

package engine

// uringAsyncSink submits each producer batch to the ring as one tag carrying
// a WRITE SQE per record plus an optional WRITE SQE for the batch's index
// entries. It never waits; the reaper calls done once every completion has
// arrived.
type uringAsyncSink struct {
	log    *os.File
	idx    *os.File
	handle *uringSinkHandle
	done   sinkDone

	mu   sync.Mutex
	live map[uint32]asyncPending // tag -> {batch, bufs, lastOffset}
}

type asyncPending struct {
	bufs       []*pooledBuf
	lastOffset uint64
	totalLen   int64
}

func (s *uringAsyncSink) Write(batch sinkBatch, done sinkDone) error {
	tag := s.handle.NextTag()
	ops := make([]uringOp, 0, len(batch.Records)+2)
	for _, w := range batch.Records {
		ops = append(ops, uringOp{Kind: uringOpWrite, File: s.log, Buf: w.P, Off: w.Off, Link: true, Tag: tag})
	}
	if !batch.Index.empty() {
		ops = append(ops, uringOp{Kind: uringOpWrite, File: s.idx,
			Buf: encodeIndexEntries(batch.Index), Off: batch.Index.Start, Link: true, Tag: tag})
	}
	ops[len(ops)-1].Link = false
	if err := s.handle.Submit(ops); err != nil {
		return err
	}
	return nil
}

// complete is called by the reaper through the handle, once per tag.
func (s *uringAsyncSink) complete(tag uint32, err error) { s.done(err) }
```

Wait — there's a subtlety. The `sinkDone` signature from phase 1 is `func(err error)`, called once per submitted batch. That's exactly what we need. The WAL's done closure:

```go
func (w *wal) asyncSubmit(entries []asyncEntry) error {
	batch, bufs, lastOff := w.buildBatch(entries)
	return w.sink.Write(batch, func(err error) {
		w.mu.Lock()
		if err != nil {
			if w.err == nil {
				w.err = err
			}
		} else if lastOff+1 > w.asyncNext {
			w.asyncNext = lastOff + 1
		}
		w.cond.Broadcast()
		w.mu.Unlock()
		for _, b := range bufs {
			b.release(encodeKeep)
		}
	})
}
```

That's it. The reaper completes the ring operation, calls `handle.complete`, which signals any waiters and calls `uringAsyncSink.complete`, which calls `done`. The WAL's closure runs on the reaper goroutine, updates `asyncNext`, and recycles buffers.

The index entries come from the queue. `buildBatch` reads them from the `asyncEntry` list the queue pushed, so no separate path is needed.

### 5.4 Removing the async channel

In `append.go`, the async branch of `commitAsyncGroup` currently does:

```go
q.mu.Unlock()
pushErr := b.w.pushAsyncBatch(ctx, entries)
q.mu.Lock()
```

Phase 2 replaces this with a call that builds the batch and hands it to the sink:

```go
q.mu.Unlock()
pushErr := b.w.submitAsync(ctx, entries, b)
q.mu.Lock()
```

`submitAsync` is the WAL's new entry point. It builds the `sinkBatch` (records plus index), captures the buffer and offset state in a closure, and calls `w.sink.Write(batch, done)`.

The `w.asyncChan` field is deleted. The `w.buf`/`w.pending`/`w.swapBuf`/`w.swapPending` machinery is deleted. `w.wake` is deleted. The flusher goroutine (`syncLoop`, `flush`, `adaptiveFlush`, `drainAndFlushAsync`, `writeRun`, `drainRemainingAsync`) is deleted on ring hosts.

Note that "deleted" here means "not compiled on `//go:build linux` when a ring is present." The `wal` struct itself splits into two implementations:

- `wal_ring.go` (`//go:build linux`): the direct-submission WAL. No channel, no flusher goroutine. Sync producers wait on `handle.Wait(tag)`.
- `wal_syscall.go` (`//go:build !linux`): the current WAL, unchanged. Channel, flusher goroutine, everything.

Both implement the same `flush`, `push`, `pushBatch`, `wait`, `stop`, `currentError`, `bytes`, `channelFill`, `byteFill`, `UpdateConfig`, `setError` methods. The choice is made in `newWAL`:

```go
// newWAL in wal.go (no build tag)
func newWAL(seg *segment, cfg Config, startOffset uint64, ring *uringRing, scopes ...*metrics.Scope) *wal {
	if ring != nil {
		return newRingWAL(seg, cfg, startOffset, ring, scopes...)
	}
	return newSyscallWAL(seg, cfg, startOffset, scopes...)
}
```

This is the same pattern as the sink: two implementations, one signature, chosen at the constructor. `wal.go` becomes a two-line dispatcher.

### 5.5 The ring WAL's submit path

The ring WAL is smaller than the current one because it has no flusher. Here's the essential shape.

```go
//go:build linux

package engine

type wal struct {
	cfg    Config
	sink   sink
	file   *os.File
	idx    *os.File

	mu             sync.Mutex
	cond           *sync.Cond
	err            error

	asyncNext      uint64
	pushedBytes    int64
	publishedBytes int64

	// Sync path: producers submit their own tag and wait for it.
	syncSeq atomic.Int64

	m *walMetrics
}

func (w *wal) submitSync(batch sinkBatch, tag uint32) error {
	ops := make([]uringOp, 0, len(batch.Records)+2)
	for _, sw := range batch.Records {
		ops = append(ops, uringOp{
			Kind: uringOpWrite, File: w.file, Buf: sw.P, Off: sw.Off,
			Link: true, Tag: tag,
		})
	}
	if !batch.Index.empty() {
		ops = append(ops, uringOp{Kind: uringOpWrite, File: w.idx,
			Buf: encodeIndexEntries(batch.Index), Off: batch.Index.Start, Link: true, Tag: tag})
	}
	ops[len(ops)-1].Link = false
	ops = append(ops, uringOp{Kind: uringOpFsync, File: w.file, DataSync: true, Tag: tag})
	return w.handle.Submit(ops)
}

func (w *wal) wait(tag uint32) error { return w.handle.Wait(tag) }
```

The sync path in `append.go` changes from `staged.w.wait(seq)` to `staged.w.wait(tag)`, and the seq is a tag. Nothing else in the sync path changes.

### 5.6 Ordering: asyncNext vs. per-producer completion

The WAL's `asyncNext` must advance monotonically. But the ring can complete out of order. So the async sink needs to hold completions until their predecessors finish, exactly as the current flusher does.

This is the `reservation` FIFO, but the FIFO lives in `queue.go` — the WAL doesn't know about it. In the current design, the flusher's `drainAndFlushAsync` doesn't touch the FIFO; it just writes bytes. The Queue's `drainReservationsLocked` advances `publishedOffset` on the producer side, and the WAL's `asyncNext` advances independently.

Wait — so does the WAL's `asyncNext` need to be monotonic? Let me re-check what it's used for.

Searching for `asyncNext`:

- Set by `drainAndFlushAsync` after each write.
- Read by `wait(seq)` to decide whether to block.

And `w.publishedBytes` is what `wait(seq)` actually checks:

```go
func (w *wal) wait(seq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.publishedBytes < seq && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}
```

So `asyncNext` is dead state. `publishedBytes` is what matters, and it advances by `len(data)` per flush.

In the ring WAL, `publishedBytes` advances by the sum of completed write lengths. If writes complete out of order, `publishedBytes` could be non-monotonic. But we can just not use it — the ring WAL's `wait` takes a tag, not a byte offset.

So the ring WAL doesn't need `asyncNext` at all. The sync producers wait on their tag. The Queue's `publishedOffset` (its own field) advances via the FIFO in `queue.go`, which the ring WAL does not touch.

This makes the ring WAL much simpler than the syscall WAL. It has no FIFO, no `asyncNext`, no byte-sequenced `wait`. It is a thin facade over `sink.Submit` and `sink.Wait`.

### 5.7 Backpressure

The current design bounds outstanding async work by the channel's capacity (`MaxFlushBytes / 4096` slots). The ring's SQ depth (4096) is the natural bound in phase 2.

But we also want to bound the number of outstanding producer batches, because each holds its buffers until completion. That number is bounded by the SQ depth, but the worst case is `4096 / 2 = 2048` batches, more than the current channel capacity (256 slots for a 1 MB byte budget). So we need an explicit bound.

Add a `sem chan struct{}` to the ring WAL with capacity `MaxFlushBytes / MaxPayload`. Producers acquire one slot before submitting and release in the done callback. This preserves the current byte-budget semantics: a full batch budget blocks the producer, sized in slots for the worst-case payload.

```go
type wal struct {
	// ...
	slots chan struct{}
}

func (w *wal) submitAsync(batch sinkBatch, done sinkDone) error {
	select {
	case w.slots <- struct{}{}:
	case <-w.shutdown:
		return ErrQueueClosed
	}
	wrapped := func(err error) {
		<-w.slots
		done(err)
	}
	return w.sink.Write(batch, wrapped)
}
```

The slot is released on completion, not on submission — so the backpressure reflects actual outstanding writes, not just queued ones.

### 5.8 Shutdown

The current `stop` closes the flusher, which drains `asyncChan` and does a final fsync. The ring WAL's `stop`:

1. Close the shutdown channel — no new submissions accepted.
2. Wait for the sink's `Close`, which blocks until every outstanding tag is complete or cancelled.
3. Close the ring handle.

The ring's `Close` cancels all outstanding operations by closing the ring fd. The reaper drains remaining CQEs and marks them complete. The WAL's `done` callbacks fire with `err = ErrQueueClosed`, which sets `w.err`.

That matches the current shutdown contract: "a producer parked on a long-poll wakes with ErrQueueClosed."

**Closing the fd cancels in-flight operations and releases the mmaps.** SQEs already submitted to the kernel may still complete, but their CQEs are lost because the ring's CQ is unmapped. Handles that had outstanding tags see `closed == true` and their `Wait` returns `ErrQueueClosed`; no completion is required.

### 5.9 Fsync correctness under cancellation

If a producer submits WRITE+FSYNC and the ring is closed before completion, the FSYNC may be cancelled with `-ECANCELED`. The handle's `Wait` returns that errno as an error. The WAL's sync producer surfaces it as a write failure — correct.

### 5.10 Sync-path coalescing (deliberately omitted)

I want to be explicit: phase 2's sync path does **not** coalesce fsyncs across producers. Each producer's `AppendBatch` submits its own tag with its own FSYNC. This is the right default because:

- Per-producer durability is what the current API promises.
- On NVMe, the kernel processes fsyncs in parallel up to the device queue depth.
- The alternative (coalesce in the sink) reintroduces the linger sleep and the flusher goroutine, which phase 2 is trying to delete.

If a future workload shows that coalescing helps, it can be added as a ring-level optimization: submit the FSYNC with a small delay, or use `io_uring`'s linked timeout. That is a phase 3 topic and out of scope here.

---

## 6. Testing

### 6.1 Existing tests must pass on both paths

Every test in `wal_test.go`, `durability_test.go`, `overlapped_test.go`, `append_test.go`, `append_async_test.go`, `readbatch_test.go` must pass with both `MIKYU_DISABLE_URING=1` and `MIKYU_DISABLE_URING=` on Linux. CI runs both. The Makefile (or CI config) adds:

```yaml
- run: MIKYU_DISABLE_URING=1 go test ./internal/engine/...
- run: go test ./internal/engine/...
```

On non-Linux (including the Windows development host), only the first effectively runs the same code (the ring is always nil). Windows CI runs the suite once.

### 6.2 New tests

**`internal/engine/uring_linux_test.go`** — build tag `linux`:

- `TestUringWriteThenSync`: open a temp file, submit a small write with linked fsync, verify the bytes are on disk *and* that `Wait(tag)` returned only after the fsync's CQE, not the write's. Assertion: read the file, check bytes; then call `f.Sync()` on a separately opened handle and confirm no further writes occur (i.e., the first fsync already covered the write).
- `TestUringParallelWrites`: submit 64 writes on 64 different temp files, verify all complete and that completions arrive in at least two distinct orders across 100 runs (to exercise out-of-order dispatch).
- `TestUringShortWriteIsError`: submit a write larger than the file's remaining capacity (read-only fd), verify `Wait` returns a real errno and that the corresponding tag's `pend` entry is gone.
- `TestUringCancellationOnClose`: submit a slow write, close the ring, verify every outstanding `Wait` returns `ErrQueueClosed` within 100 ms.
- `TestUringReaperDoesNotLeak`: use `goleak.VerifyTestMain` on the package; the reaper goroutine must exit on `Close`. Add a second assertion: after closing 100 handles, `len(ring.handles)` and `len(ring.free)` are both zero (no slots leaked).
- `TestUringHandleUnregister`: register a handle, close it, then submit from a stale reference and confirm the CQE is dropped without panicking.
- `TestUringSubmitRollbackOnError`: force `ring.Submit` to fail (e.g., by closing the ring fd directly) and confirm the tag's `pend` entry is removed so a subsequent `Wait` does not block.

**`internal/engine/sink_test.go`** — no build tag:

- `TestSinkAsyncMatchesSync`: write the same bytes through both sinks, verify byte-identical files.
- `TestSinkSyncForcesDurability`: write via `syncSink`, reopen the file with `os.O_RDONLY`, verify the bytes are on disk.
- `TestSinkCloseIdempotent`: close twice, no panic.
- `TestSinkIndexWriteIsBatched`: stage an index batch of 1000 entries, verify a single `WriteAt` on the `.idx` file (via `indexWriteHook`) for the whole batch.

**`internal/engine/uring.go` unit tests** — build-tag agnostic:

- `TestUringUnavailableIsSilent`: on any host, `newSink(log, idx, true, nil)` returns a working `syncSink`, not an error.

### 6.3 Race detector

`go test -race ./internal/engine/...` must pass on Linux with the ring. The `uringSinkHandle` is the concurrency-critical type; every field access goes through its mutex. The ring's `Submit` takes `submitMu` around the SQE writes and the `io_uring_enter` call. The CQ ring reads are single-reader (the reaper). The handle table is guarded by `ring.mu` and taken only on registration, deregistration, and dispatch.

Run the race detector on Windows too, even though the ring is nil — the sink abstraction and the split WAL must race-cleanly degrade.

---

## 7. Rollout and rollback

**Phase 1 rollout:**

1. Land the seven new files plus the four modified files. Merge as a single commit.
2. CI runs the engine suite with `MIKYU_DISABLE_URING=1` on Linux and default on Linux.
3. Windows CI runs the suite once; the ring is nil so the sink path is exercised identically to before.
4. If anything breaks, revert the commit. No data-format change, no on-disk state.

**Phase 2 rollout:**

1. Land the `wal_ring.go` / `wal_syscall.go` split behind a build tag and a ring-presence check.
2. Add a temporary `MIKYU_FORCE_SYSCALL_WAL=1` env var that makes `newWAL` return the syscall WAL even when a ring is present. Use it for the initial rollout.
3. Run the full engine suite under both settings and both environments.
4. Run the benchmark matrix on Linux CI as an informational artifact, not a gate. Phase 2 ships regardless of the outcome; the operator can set `MIKYU_FORCE_SYSCALL_WAL=1` to opt out.
5. Two releases later, delete `MIKYU_FORCE_SYSCALL_WAL` and the syscall WAL on Linux.

**Rollback for phase 2** is the same env var: `MIKYU_FORCE_SYSCALL_WAL=1` restores the pre-phase-2 WAL on the next process start. No file-format change, no data migration.

---

## 8. Impact

I want to be honest about the numbers, because the popular story about io_uring is not the story that applies to this codebase.

**Phase 1 (sink abstraction):** zero measurable impact. It is a refactor. The only performance-adjacent change is that sync writes on Linux go through `io_uring`'s WRITE+FSYNC instead of `pwrite`+`fdatasync`, saving one syscall per batch. On NVMe that is well under 1% because `fdatasync` dominates at 50–500 µs. On SATA it is noise.

**Phase 2 (direct submission, shared reaper):** the wins are real but narrower than the popular narrative:

| Change | Expected effect |
|---|---|
| One reaper per data directory instead of one flusher per queue | Fewer goroutines; better cache locality under high queue counts |
| No channel send per producer batch | 50–150 ns saved per batch on the produce path |
| No double-buffer swap | Less allocation churn; the pool still exists but is smaller |
| Kernel-parallel writes | Real on NVMe with QD > 1; none on SATA |
| No `adaptiveFlush` linger for sync | Lower p50/p99 latency for sparse sync producers |
| Producer-side fsync instead of coalesced | **Worse** for high-throughput sync; better for latency |

The last row is the one to watch. If a workload has many sync producers all writing concurrently, the current design coalesces them into one fsync per flush and phase 2 does one fsync per producer. On a device with queue depth 1 (SATA), phase 2 will be slower. On NVMe with queue depth 64, it will be faster. Operators who see a regression on their hardware set `MIKYU_FORCE_SYSCALL_WAL=1` and file a report; the ring path is not forced on anyone.

---

## 9. Non-goals

- **No `IORING_SETUP_SQPOLL`.** Requires privileges, burns a CPU, and is the wrong default for a library.
- **No `IORING_OP_READ` on the read path.** Reads go through the mmap; there is no syscall to save.
- **No `IORING_OP_SEND_ZC` on the wire.** `sendfile(2)` via `io.CopyN` is already zero-copy.
- **No `IORING_OP_ACCEPT` on the server.** The `net` package does not expose the raw listener fd in a way that would let us; the added complexity is not justified by the current traffic profile.
- **No coalescing of fsyncs across producers.** See §5.10.
- **No change to `queues.json`, segment files, index files, offset store, or the wire protocol.** This is an internal engine change; nothing on disk changes.
- **No change to the sink interface between phase 1 and phase 2.** `sinkBatch.Index` is populated in phase 2 on ring hosts; every other caller passes the zero value.

---

## Appendix A: `uring_linux.go` outline

The full file is ~400 lines. The parts worth seeing in the plan:

```go
//go:build linux

package engine

const (
	uringEntries = 4096

	uringOffSQRing = 0
	uringOffCQRing = 0x8000000
	uringOffSQEs   = 0x10000000

	uringSqeFLink      = 1 << 2
	uringSqeIOLink     = 1 << 1 // IOSQE_IO_HARDLINK, if we ever need it
	uringOpWriteByte   = 1
	uringOpFsyncByte   = 3
	uringFsyncDataSync = 1
	uringEnterGetEvents = 1 << 0
)

func newUringRing() (*uringRing, error) {
	if os.Getenv("MIKYU_DISABLE_URING") != "" {
		return nil, errUringUnsupported
	}
	var params ioUringParams
	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(uringEntries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, errUringUnsupported
	}
	r := &uringRing{fd: int(fd), stop: make(chan struct{}), done: make(chan struct{})}
	// mmap sq, cq, sqes using params.
	// Fill the SQ array: slot i holds index i, so a SQE submission does not
	// need to search for a free slot.
	go r.loop()
	return r, nil
}

// Submit writes the SQEs and issues one io_uring_enter. The SQE writes are
// sequenced before the tail store, and the store is a release, so the kernel
// observes the SQEs before it observes the advanced tail. submitMu is held
// across both so concurrent producers do not interleave their SQEs; under
// heavy multi-producer load the kernel batches the SQEs from different
// producers naturally because io_uring_enter drains the whole SQ.
//
// io_uring_enter with to_submit=len(ops) may block if the SQ is full; the
// SQ depth bounds the number of outstanding submissions per ring and the
// WAL's slot semaphore keeps producers well under it.
func (r *uringRing) Submit(ops []uringOp) error {
	r.submitMu.Lock()
	defer r.submitMu.Unlock()
	tail := atomic.LoadUint32(r.sqTail)
	for i, op := range ops {
		slot := (tail + uint32(i)) & r.sqMask
		sqe := (*ioUringSqe)(unsafe.Pointer(&r.sqes[uintptr(slot)*uringSqeSize]))
		*sqe = r.encodeSQE(op)
	}
	atomic.StoreUint32(r.sqTail, tail+uint32(len(ops)))
	_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), uintptr(len(ops)), 0, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// Register allocates a sink id, stores the handle at that slot, and returns
// it. The caller must call Unregister when the handle is no longer live.
func (r *uringRing) Register() *uringSinkHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	var id uint32
	if n := len(r.free); n > 0 {
		id = r.free[n-1]
		r.free = r.free[:n-1]
	} else {
		id = uint32(len(r.handles))
		r.handles = append(r.handles, nil)
	}
	h := &uringSinkHandle{ring: r, id: id}
	h.cond = sync.NewCond(&h.mu)
	h.pend = make(map[uint32]int32)
	h.res = make(map[uint32]int64)
	r.handles[id] = h
	return h
}

// Unregister clears the handle's slot and returns it to the free list.
func (r *uringRing) Unregister(h *uringSinkHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if int(h.id) >= len(r.handles) || r.handles[h.id] != h {
		return
	}
	r.handles[h.id] = nil
	r.free = append(r.free, h.id)
}

// Close closes the ring fd. In-flight operations may complete before the
// kernel tears down the SQ, but their CQEs are lost with the mmaps; every
// handle that had a tag outstanding sees ErrQueueClosed from its Wait.
func (r *uringRing) Close() error {
	r.stopOnce.Do(func() {
		close(r.stop)
		_ = unix.Close(r.fd)
		<-r.done
	})
	return nil
}

func (r *uringRing) loop() {
	defer close(r.done)
	cqes := make([]uringCqe, uringCqBatch)
	for {
		n, err := r.waitForCQEs(cqes)
		if err != nil {
			select {
			case <-r.stop:
				return
			default:
				continue
			}
		}
		for i := range n {
			r.dispatch(cqes[i])
		}
		r.advanceCqHead(n)
	}
}
```

`waitForCQEs` is `io_uring_enter(r.fd, 0, 1, IORING_ENTER_GETEVENTS, ...)`. One syscall per completion batch. This is the reaper's only syscall, and it blocks in the kernel until at least one CQE is available — no polling, no timer.

---

## Appendix B: what `wal_ring.go` actually looks like

For clarity, the ring WAL is roughly 200 lines, versus the current WAL's ~600. It has no flusher, no channel, no buffer swap, no linger. It has:

- `push` / `pushBatch` — the sync producer's path, which submits a tag and waits.
- `submitAsync` — the async producer's path, which submits and returns, with the queue's `done` closure captured.
- `wait` — the sync producer's blocking wait.
- `stop` — closes the sink.
- `bytes`, `channelFill`, `byteFill`, `UpdateConfig`, `currentError`, `setError` — unchanged in signature, simplified in body.

That is the entire surface. The complexity the current WAL carries — the two-channel handoff, the double buffer, the byte-sequence `wait`, the `asyncNext` monotonic counter, the `adaptiveFlush` linger — all of it exists because the syscall path needs it. On a host with a ring, none of it is needed. That is the actual architectural payoff of phase 2, and it is why this plan is structured the way it is: phase 1 introduces the seam so that phase 2 can replace a small file instead of rewriting the WAL in place.