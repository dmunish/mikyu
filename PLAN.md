# mikyu performance plan

## 0. Benchmarking and commit protocol

**Every phase follows the same measurement discipline.** The machine will have a browser and editor running, so per-run variance is high. We cancel drift by alternating before/after runs and taking medians.

### Per-phase measurement protocol

Run from a scratch directory (`mkdir bench-scratch && cd bench-scratch`) so nothing collides:

```powershell
# BEFORE (right before starting the phase, working tree clean)
go build -o ..\mikyu.exe .\cmd\mikyu
..\mikyu.exe bench run --duration 30s --workers 4 --batch 64 > before-1.txt
..\mikyu.exe bench run --duration 30s --workers 4 --batch 64 > before-2.txt

# (implement the phase)

# AFTER (immediately after, before any other edits)
go build -o ..\mikyu.exe .\cmd\mikyu
..\mikyu.exe bench run --duration 30s --workers 4 --batch 64 > after-1.txt
..\mikyu.exe bench run --duration 30s --workers 4 --batch 64 > after-2.txt
```

Interleave them (`before-1`, `after-1`, `before-2`, `after-2`) if you can — this cancels slow thermal and background drift better than sequential. Extract the `MSGS/S` and `BYTES/S` lines from each, take the **median** of the two befores and the two afters, and compute `ratio = after_median / before_median`.

**Noise floor:** on a warm machine, ±5-10% is normal. **A phase that claims under 1.05x must say so and justify keeping itself.** Infrastructure phases (15, 20) declare "1.00x, infrastructure only" up front.

### Commit format

```
[perf] <packages>: <ratio>x; <one-liner>

Before: <before-median> MSGS/S
After:  <after-median> MSGS/S
```

Subject only in the `[perf]` format; body carries the raw numbers so the ratio is auditable. If the phase is a regression on one platform but a win on another, use the primary platform's ratio in the subject and add a `Cross-platform: ...` line in the body.

### Revert rule

A phase that shows no measurable win **and** has no explanatory reason (bottleneck elsewhere) is reverted before the next phase starts. Infrastructure phases are exempt.

### Test discipline

Every phase adds tests to the package it touches. Tests must pass with **only that phase merged**, even if later phases are absent. New tests never import code introduced by later phases.

---

## Tier A — Obvious waste

Windows-impacting, cross-platform, high-confidence. Do these first, one at a time, in order.

### Phase 1 — Batch WAL record writes

- **Packages:** `internal/engine`
- **Expected win:** 1.5–2.5x on Windows, 1.2–1.5x on POSIX
- **Platforms:** all

**Problem.** `drainAndFlushAsync` writes each `asyncEntry` with its own `w.writeAt(e.rec, e.pos)`. On Windows each `os.File.WriteAt` is `SetFilePointerEx + WriteFile`.

**Change.** In `internal/engine/wal.go`, rewrite `drainAndFlushAsync` to coalesce all entries into a single contiguous buffer and issue one `writeAt` per segment-run. Steps:

1. After draining the channel, group `entries` by `e.seg` preserving order. Within a group, positions are guaranteed contiguous (`pos[i] == pos[i-1] + recLen[i-1]`) because positions are assigned under `q.mu`.
2. For each group: allocate `data := make([]byte, 0, totalLen)`, append every `e.rec`, then one `w.writeAt(data, group[0].pos)`.
3. Do **not** change `asyncNext` accounting or `w.m.flushes.Inc()` placement.
4. Leave `commitSync` untouched — phase 1 is async-path only, so the win is isolated.

**Tests.** Add `TestWALAsyncBatchesWrites` in `wal_test.go`. Install a `writeAtHook func(off int64)` alongside the existing `writerHook`; push 1000 entries via `pushAsync`, wait for the flusher to drain, assert `len(recordedOffsets) <= 8` (per-batch, allowing for segment splits). Also assert every offset is written exactly once.

**Acceptance.**
- [ ] All existing `engine` tests pass.
- [ ] New test passes.
- [ ] Bench ratio reported in the commit body.
- [ ] Commit: `[perf] engine: <ratio>x; batch WAL record writes into one syscall per flush`

---

### Phase 2 — Batch index writes

- **Packages:** `internal/engine`
- **Expected win:** 1.15–1.4x on top of phase 1
- **Platforms:** all

**Problem.** `writeIndexAt` is called once per record inside the async loop and once per record inside `commitSync`. Each call on Windows is two syscalls.

**Change.** In `internal/engine/wal.go` (`drainAndFlushAsync`) and `internal/engine/append.go` (`commitSync`):

1. For each segment-run, allocate `idx := make([]byte, len(run)*rawIndexEntrySize)`.
2. Fill each slot with `rawIndexEntry{pos, recLen}.MarshalBinary`.
3. One `seg.idxFile.WriteAt(idx, int64(run[0].offset-seg.baseOff)*rawIndexEntrySize)`.

**Tests.** Add `TestWALAsyncBatchesIndexWrites` mirroring phase 1's test but hooking `idxFile.WriteAt`. Assert ≤ 4 index writes for 1000 entries. Add `TestCommitSyncBatchesIndexWrites` in `append_test.go`.

**Acceptance.**
- [ ] All existing tests pass.
- [ ] Both new tests pass.
- [ ] Commit: `[perf] engine: <ratio>x; batch index entries into one write per flush`

---

### Phase 3 — Release `q.mu` before blocking on the async channel

- **Packages:** `internal/engine`
- **Expected win:** 1.15–1.6x under contention
- **Platforms:** all

**Problem.** `AppendBatch` holds `q.mu` through `commitAsync`, which calls `w.pushAsync` per record. When the async channel is full, the producer blocks while holding `q.mu`; every other producer stalls behind it.

**Change.** This is the trickiest phase. Do it exactly as written:

1. **Change the async channel's payload type** in `internal/engine/wal.go`:
   ```go
   asyncChan chan []asyncEntry   // was chan asyncEntry
   ```
   Capacity stays `max(1, int(cfg.MaxFlushBytes)/4096)`.

2. **Add `pushAsyncBatch`** (replaces `pushAsync`):
   ```go
   func (w *wal) pushAsyncBatch(ctx context.Context, entries []asyncEntry) error {
       if err := ctx.Err(); err != nil { return err }
       select {
       case w.asyncChan <- entries:
           return nil
       default:
       }
       w.m.blockEvents.Inc()
       w.m.blockedProducers.Add(1)
       defer w.m.blockedProducers.Add(-1)
       select {
       case w.asyncChan <- entries:
           return nil
       case <-ctx.Done():
           return ctx.Err()
       case <-w.shutdown:
           return ErrQueueClosed
       }
   }
   ```

3. **Add a `reservedPos int64` field to `queue`**, initialized to 0. Under `q.mu`, position computation becomes `q.active.pos + q.reservedPos + stagedOffsetWithinBatch`. Increment `q.reservedPos += b.totalLen` when reserving. Decrement on commit or rollback.

4. **Restructure `commitAsync`** in `append.go`. The current function does push+cache+reserve together. Split into three:
   - `reserveAsync(b)` — under `q.mu`: compute `b.positions[]`, `b.offsets[]`, advance `nextOffset`, `q.reservedPos += b.totalLen`. (This is the reservation; nothing blocks.)
   - `pushAsyncBatch(ctx, b)` — **outside `q.mu`**: one `w.pushAsyncBatch` call with all entries, then per-entry `q.cache.Put` (the ring cache has its own lock; safe outside `q.mu`).
   - `commitAsync(b)` — under `q.mu`: `b.seg.pos += b.totalLen`, `q.publishedOffset = last+1`, `q.reservedPos -= b.totalLen`.

5. In `AppendBatch`, replace the current `commitAsync` call sites with the three-step sequence: hold the lock for `reserveAsync`, unlock, `pushAsyncBatch`, relock for `commitAsync`. On `pushAsyncBatch` failure, only `reserveAsync`'s offset side effects remain — the segment positions are safely rolled back because `reservedPos` is decremented and `seg.pos` was never advanced.

6. In the flusher, change the receive to slice form; after receiving one slice, non-blocking-drain more slices into `all` before writing (preserving the current coalescing behavior).

**Tests.**
- `TestAppendBatchDoesNotHoldLockOnChannelBlock`: fill `asyncChan` to capacity, spawn a goroutine appending a second batch that blocks on the channel, then in the same test acquire `q.mu` and confirm it's available within 10ms.
- `TestAppendBatchRollbackOnCtxCancel`: cancel the context mid-push; assert `q.reservedPos` returns to 0 and `q.active.pos` is unchanged.
- `TestAppendBatchPreservesOffsetsUnderContention`: 8 goroutines, 100 batches each, assert every offset from 0 to N is present and readable.

**Acceptance.**
- [ ] All existing tests pass.
- [ ] All three new tests pass, including under `-race`.
- [ ] Commit: `[perf] engine: <ratio>x; release queue mutex before blocking on async channel`

---

### Phase 4 — Cheap record IDs

- **Packages:** `internal/engine`
- **Expected win:** 1.05–1.15x
- **Platforms:** all

**Problem.** `uuid.NewV7()` calls `crypto/rand` per record; on Windows this routes through `ProcessPrng` (8.4s CPU in your profile).

**Change.** In a new `internal/engine/recordid.go`:

1. Process-scoped state:
   ```go
   type recordIDGen struct {
       prefix [8]byte        // filled once at process start from crypto/rand
       seq    atomic.Uint64  // monotonic
   }
   var idGen recordIDGen
   func init() { rand.Read(idGen.prefix[:]) }
   ```

2. `func newRecordID() [16]byte` returns `[prefix | seq.BigEndian]`. Sortable, unique across restarts (as long as the prefix differs), no per-record entropy cost.

3. In `append.go`, replace `id := uuid.NewV7()` with `id := newRecordID()`. Change `encodeRecord`'s signature from `uuid.UUID` to `[16]byte` (or `string`) and update call sites in `encode.go`, `checkpoint.go`, and `encode_test.go`.

4. Keep the 16-byte layout so the on-disk format doesn't change; the disk format doesn't care how the bits were generated.

5. Remove `github.com/google/uuid` from `go.mod` if nothing else uses it.

**Tests.** `TestRecordIDIsMonotonic` (successive IDs strictly increase in the seq field), `TestRecordIDPrefixIsProcessStable` (two ID generators in the same process share the prefix; the current `idGen` is process-scoped so this checks a re-export), `TestRecordIDRoundTripsThroughDisk` (encode/decode a record, extract ID, compare).

**Acceptance.**
- [ ] All existing tests pass.
- [ ] `uuid` dependency removed if unused.
- [ ] Commit: `[perf] engine: <ratio>x; replace crypto/rand uuid/v7 with process-scoped 128-bit monotonic IDs`

---

### Phase 5 — Gate the ring cache and drop the per-append clone

- **Packages:** `internal/engine`
- **Expected win:** 1.05–1.10x on produce-only, 1.20x+ on mixed
- **Platforms:** all

**Problem.** Every async append does `q.cache.Put(Record{..., Payload: bytes.Clone(payloads[...])})` — 6.1s of `bytes.Clone` in your profile, all wasted on produce-only runs.

**Change.** In `internal/engine/cache.go` and `queue.go`:

1. Add `hasReader atomic.Bool` to `queue`. Set it `true` on the first `Read`/`ReadBatch`/`Fetch` call.
2. In `commitAsync`/`pushAsyncBatch`, only call `q.cache.Put` when `hasReader.Load()` is true.
3. In `ringCache.Put`, drop the clone: take the caller's `Record` as-is and store the slice by reference. Callers must guarantee the slice won't be mutated after `Put`. In `append.go`, the `rec` passed to `pushAsyncBatch` is a fresh allocation that the flusher writes and then discards; passing its payload slice to the cache is safe.

**Tests.**
- `TestCacheGatedOnFirstRead`: run a queue with appends but no reads; assert `cache.stats()` shows 0 bytes used. Then do a `Read`, do another append, assert cache is now populated.
- `TestCachePayloadAliasesEncodedRecord`: append once, mutate nothing, read twice, assert same bytes both times. Then verify via `unsafe.Pointer` that the cached payload and the WAL payload are the same slice (test-only assertion using `reflect.ValueOf(...).Pointer()`).

**Acceptance.**
- [ ] All existing tests pass.
- [ ] Both new tests pass.
- [ ] Commit: `[perf] engine: <ratio>x; gate ring cache on first reader, drop per-append clone`

---

## Tier B — Runtime, build, self-tuning defaults

### Phase 6 — `sync.Pool` and per-P buffer caches

- **Packages:** `internal/engine`
- **Expected win:** 1.10–1.20x

**Problem.** Per-append: `encodeRecord` allocates a fresh `[]byte`, `drainAndFlushAsync` allocates the coalesced buffer (phase 1), `commitSync` allocates the coalesced index slice (phase 2).

**Change.**

1. Add `internal/engine/pool.go` with `encodePool sync.Pool` returning `*[]byte` sized up to `MaxPayload + minRecordSize`. Reuse across appends. Never return buffers larger than 4× `MaxPayload` to the pool (drop them to GC).
2. Same pattern for the phase-1 coalesce buffer and phase-2 index buffer.
3. In `wal.go`, add per-P `swapBuf`/`swapPending` (already exist but capped); confirm they don't hold capacity larger than `2 * MaxFlushBytes`.

**Tests.** `TestEncodeBufferReused` — alloc count assertion via `testing.AllocsPerRun` on `encodeRecord` batch of 1000, expect `< 5` allocations.

**Commit:** `[perf] engine: <ratio>x; pool encode buffers and coalesce slabs`

---

### Phase 7 — PGO and build flags

- **Packages:** build system, `cmd/mikyu`
- **Expected win:** 1.05–1.15x

**Change.**

1. Add a `Makefile` (or `scripts/release.ps1` / `scripts/release.sh`) target that runs `bench run --duration 30s` under `-cpuprofile`, merges into `default.pgo` via `go tool pprof -proto`, and commits `default.pgo` at the repo root (Go picks it up automatically).
2. Add release build flags: `GOAMD64=v3`, `GOARM64=v8.2`, `GORISCV64=rva22u64`, `-trimpath`, `-buildvcs=true`.
3. Document in `docs/build.md`. Do **not** ship the PGO profile captured during a bench run that includes the observer or `payloadFor` costs — capture with `--metrics=false --quiet` (add those flags if needed).

**Tests.** CI must build with the profile; `go build` from a clean checkout uses `default.pgo` automatically.

**Commit:** `[perf] build: <ratio>x; ship PGO profile and GOAMD64=v3 by default`

---

### Phase 8 — Green Tea GC, size-specialized malloc, `weak` pointers

- **Packages:** `internal/engine`
- **Expected win:** 1.03–1.08x

**Change.**

1. Verify `go.mod` says `go 1.27` and the toolchain is 1.27+. Confirm no `GOEXPERIMENT=nogreenteagc` in env; document in `docs/build.md` that these are required.
2. Replace the ring cache's manual eviction-on-oversize path with `weak.Pointer`-gated slots: a slot whose record is only weakly reachable can be reclaimed by GC before explicit eviction. This gives the ring cache a "reclaim under memory pressure" mode without changing its shape.
3. Confirm `GOEXPERIMENT=nosizespecializedmalloc` is not set in CI.

**Tests.** `TestRingCacheRespectsWeakPressure` — allocate 10× `maxBytes` worth of records into a small ring, `runtime.GC()` twice, assert `curBytes <= maxBytes` even without a `Put` that would trigger eviction.

**Commit:** `[perf] engine: <ratio>x; use weak pointers in the ring cache, confirm Green Tea GC defaults`

---

## Tier C — Platform-native syscalls

### Phase 9 — Windows async I/O via `FILE_FLAG_OVERLAPPED`

- **Packages:** `internal/engine`
- **Expected win:** 1.3–1.8x on Windows
- **Platforms:** Windows

**Problem.** `os.OpenFile` doesn't set `FILE_FLAG_OVERLAPPED`; every `WriteAt`/`ReadAt` on the returned handle is two syscalls.

**Change.**

1. Add `internal/engine/handle_windows.go` with `openOverlapped(path string, flags int) (*os.File, error)` that calls `windows.CreateFile` with `FILE_FLAG_OVERLAPPED` and wraps the handle via `os.NewFile(uintptr(h), path)`. The handle supports Go's async I/O integration on 1.25+ — the runtime's IOCP owns completions.
2. In `internal/engine/segment.go`, add a `//go:build windows` shim `openSegmentFile` that uses `openOverlapped`; on other platforms it's `os.OpenFile`.
3. Confirm with `go tool trace` (or a synthetic test) that `WriteAt` is now one kernel transition. If Go still issues `SetFilePointerEx`, fall back to `windows.WriteFile` with an `OVERLAPPED` struct directly — add `pwrite`/`pread` in the shim.
4. Note that `os.File.Sync()` on an overlapped handle uses `FlushFileBuffers`, which is synchronous. That's fine; the write path is async, the fsync path is not.

**Tests.** `TestOverlappedHandleRoundTrip` (Windows-only): write, fsync, close, reopen via plain `os.OpenFile`, read back. Plus `TestOverlappedSingleSyscall`: use `WriteAtHook` to count syscalls; batch of 1000 → `<= 8` kernel writes.

**Acceptance.** All existing Windows tests pass; bench shows the expected Windows improvement; no change on Linux/macOS.

**Commit:** `[perf] engine: <ratio>x; open Windows segment handles with FILE_FLAG_OVERLAPPED`

---

### Phase 10 — POSIX `pwritev`/`preadv`

- **Packages:** `internal/engine`
- **Expected win:** 1.10–1.25x on Linux/macOS/BSD
- **Platforms:** POSIX

**Change.** In `internal/engine/handle_posix.go` (new, `//go:build !windows`), add `pwritev(fd uintptr, iovs [][]byte, off int64) (int, error)` using `unix.Pwritev`. In `segment.go`'s write path, replace `WriteAt(data, pos)` with `pwritev` when `data` is large. Because phase 1 already coalesces to one buffer, `pwritev` with `len(iovs) == 1` is functionally `pwrite` — **the win here is smaller than the phase name suggests**. Skip unless the phase-1 profile shows residual per-write overhead.

**Reconsider.** If phase 1 already gets one syscall per flush on POSIX, `pwritev` adds nothing. **Phase 10 is optional and should be benchmarked against a "skip" decision.**

**Commit if kept:** `[perf] engine: <ratio>x; use pwritev/preadv on POSIX for batched writes`

---

### Phase 11 — POSIX `fadvise`/`madvise`

- **Packages:** `internal/engine`
- **Expected win:** 1.02–1.08x, hard to measure
- **Platforms:** Linux/macOS/BSD

**Change.**

1. After `enableMMap` on a closed segment: `unix.Madvise(mmapData, unix.MADV_SEQUENTIAL)`.
2. After `TruncateBefore` frees a segment: nothing (unmapping is enough).
3. In `fetch.go`, before a long-tail read: `unix.Fadvise(fd, int64(start), 0, unix.FADV_WILLNEED)`.
4. In `compact.go`, after rewriting a segment: `unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED)`.

**Tests.** `TestMadviseHintsAreIssued` (assert hook was called with expected offset ranges). Bench is unlikely to show a clean signal; keep if no regression.

**Commit:** `[perf] engine: <ratio>x; fadvise/madvise hints on closed segments`

---

## Tier D — Memory structures

### Phase 12 — mmap-backed active segment for reads

- **Packages:** `internal/engine`
- **Expected win:** 1.15–1.30x on read-heavy workloads
- **Platforms:** all

**Problem.** Today the active segment uses `ReadAt`. On all three platforms, `mmap(MAP_SHARED, PROT_READ)` on a file being appended to via `WriteAt` gives the reader a coherent view once the write is visible.

**Change.**

1. In `segment.go`, add `remapActive() error` that `mmap`s the active segment's log file read-only. Because it's still being written, the mapping must be refreshed when `pos` grows past the current map length. Track `activeMappedLen int64`; on each read that goes past it, unmaps and re-maps with the new file size.
2. In `read.go`, prefer `mmapData[pos:pos+len]` when the active segment's mapping covers the range.
3. On Windows, `FlushViewOfFile` is not required for the reader to see writes made via `WriteFile` on the same file — the OS cache is coherent. On POSIX, `msync` is not required for the same reason. Add a comment; no extra syscall needed.
4. When the segment rolls, phase-12's mapping becomes the standard closed-segment mapping.

**Tests.** `TestActiveSegmentReadsViaMmap` — write 100 records, read all 100, assert `segment.mmapData != nil` and `segment.activeMappedLen > 0`. Compare read latency distribution before/after via `bench run --mode consume --preload 512MiB`.

**Commit:** `[perf] engine: <ratio>x; mmap active segment for zero-copy reads`

---

### Phase 13 — Async-mode mmap write-through

- **Packages:** `internal/engine`
- **Expected win:** 1.10–1.20x on Windows, 1.30x on Linux
- **Platforms:** all, but Windows needs both `FlushViewOfFile` and `FlushFileBuffers` — measure first

**Change.** When `cfg.Async()` is set:

1. Open the active segment's log file with `mmap(MAP_SHARED, PROT_READ|PROT_WRITE)`.
2. The producer writes the record bytes into `mmapData[pos:pos+recLen]` via `copy`.
3. The flusher, once per tick, calls `msync(MS_SYNC)` (POSIX) or `FlushViewOfFile` + `FlushFileBuffers` (Windows).
4. Index writes still go through `writeIndexAt` because they use a separate file (they're small).
5. When the segment rolls, unmap the write mapping and remap read-only.

**Caveat.** On Windows, `FlushFileBuffers` is the durability barrier and cannot be skipped. Measure Windows carefully — if `FlushViewOfFile + FlushFileBuffers` costs more than one batched `WriteFile`, keep the existing `WriteAt` path on Windows and gate phase 13 to POSIX.

**Tests.** `TestMmapWriteThroughSyncsOnFlush` — write async, `fsync` via the flush tick, kill the process, reopen, verify records present. Plus a bench comparing sync duration.

**Commit:** `[perf] engine: <ratio>x; mmap write-through for async durability`

---

### Phase 14 — Per-slot seqlock ring cache

- **Packages:** `internal/engine`
- **Expected win:** 1.20–1.40x on read-heavy mixed workloads
- **Platforms:** all

**Change.** In `internal/engine/cache.go`:

1. Each slot gains `seq atomic.Uint64`. Writer: `seq.Store(odd)`, write fields, `seq.Store(even)`.
2. Reader: read `seq1`, if odd retry; read fields; read `seq2`; if `seq1 != seq2` retry.
3. `Put` must serialize on the same slot. With `cache.Put` still called under `q.mu` (from phase 3), two writers cannot race on the same slot within one process. The seqlock is only needed for the reader/writer race.
4. Keep `EvictBefore`/`Evict` under the mutex; those are slow paths.

**Tests.** `TestRingCacheConcurrentReaderWriter` under `-race` with 1 writer, 8 readers, 1M ops. Also `TestRingCacheSeqlockRetries` — instrument the retry counter and assert it stays under a small bound.

**Commit:** `[perf] engine: <ratio>x; per-slot seqlock ring cache`

---

## Tier E — Async I/O backends

Linux/BSD-leaning; Windows gets equivalent functionality from phase 9.

### Phase 15 — `internal/engine/aio` package

- **Packages:** `internal/engine/aio` (new)
- **Expected win:** 1.00x, infrastructure only
- **Platforms:** all

**Change.** Introduce the interface:

```go
type Backend interface {
    // Submit queues an async write; the completion is delivered via C().
    SubmitWrite(fd uintptr, data []byte, off int64) (handle uint64, err error)
    SubmitFsync(fd uintptr) (handle uint64, err error)
    // C returns the completion channel; capacity is the in-flight window.
    C() <-chan Completion
    // Close shuts down the backend and drains in-flight submissions.
    Close() error
}
type Completion struct {
    Handle uint64
    N      int
    Err    error
}
```

Ship the **sync fallback** (`aio_sync.go`): `SubmitWrite` does `pwrite` inline and pushes a `Completion` immediately. The engine uses `aio.Backend` in `wal.go` and `segment.go`, but with the sync backend it behaves exactly as today. **No behavioural change; this phase exists so 16 and 17 slot in.**

**Tests.** `TestSyncBackendMatchesDirectWrite` — write via `aio.SyncBackend`, read back via `os.File`, assert equal bytes. `TestSyncBackendCloseDrainsInFlight`.

**Commit:** `[perf] engine: 1.00x; introduce aio interface with sync fallback`

---

### Phase 16 — io_uring backend (Linux 5.1+)

- **Packages:** `internal/engine/aio`
- **Expected win:** 1.2–1.5x on Linux
- **Platforms:** Linux only

**Change.** Add `aio_uring_linux.go` (build-tagged). Use a pure-Go library — `github.com/iceber/iouring-go` is the safest starting point; verify it doesn't require cgo and it uses `IORING_SETUP_SQPOLL` on capable kernels. Wire the flusher to submit an entire batch with one `io_uring_submit` and to reap completions from the shared CQ ring.

**Tests.** `TestIoUringBackendMatchesSyncBackend` — same payload set, same ordering. `TestIoUringFallbackOnOldKernel` — mock the kernel version check and verify a graceful fallback to sync.

**Commit:** `[perf] engine: <ratio>x; io_uring backend for Linux`

---

### Phase 17 — kqueue backend (Darwin/BSD)

- **Packages:** `internal/engine/aio`
- **Expected win:** 1.00x, mostly used for socket/transport, not file I/O
- **Platforms:** Darwin/BSD

**Change.** kqueue has no async file I/O primitive. The backend is a real submit-and-reap for sockets only; file I/O falls through to the sync backend. This phase mostly serves the remote-daemon transport in later work.

**Commit:** `[perf] engine: 1.00x; kqueue backend for Darwin/BSD (sockets only; file I/O falls through to sync)`

---

## Tier F — Architecture

### Phase 18 — Range-chunked sharding

- **Packages:** `internal/engine`
- **Expected win:** 1.5–2.5x on single-queue high-throughput workloads
- **Platforms:** all

**Design.** WAL `n` of `N` owns offsets `[k·N·B + n·B, k·N·B + (n+1)·B)` for chunk size `B` and rotation `k`. Writes parallelize (each WAL owns its own chunk), global order preserved, reads within a chunk are sequential.

**Change.** This is the phase where the `Topology` abstraction (from the earlier discussion) **emerges naturally**; do not pre-design it in a prior phase. Concretely:

1. Add `N int` and `B int64` to `engine.Config` (default `N=1`, `B=1<<20`).
2. `Queue` holds a `[]*wal` (one per shard). The reservation path gains a shard selector: `shard = (nextOffset / B) % N`.
3. Read path: for `Read(offset)`, `shard = (offset / B) % N`; dispatch to that segment's reader.
4. Flusher: one goroutine per shard, each with its own channel and coalescing logic. The reservation path remains one lock, but the *content* of a reservation belongs to one shard.
5. **Global order:** offsets remain globally monotonic, so a reader can iterate from `minOffset` forward and always see a sorted stream, even though consecutive reads may land on different shards. This is the key innovation that Kafka does not have (Kafka's per-partition log is single-writer).

**Tests.**
- `TestShardedWritesAreGloballyOrdered` — 4 shards, 100k writes, verify every offset appears exactly once and `Read(i) < Read(i+1)`.
- `TestShardedReadsAreSequentialWithinChunk` — read a chunk-sized range, verify sequential access within the chunk and one hop per chunk boundary.
- `TestShardedRolloverAtChunkBoundary` — verify a chunk that ends mid-batch causes a shard boundary.

**Commit:** `[perf] engine: <ratio>x; range-chunked sharding with N flushers per queue`

---

### Phase 19 — Zero-copy read transport

- **Packages:** `internal/server`, `internal/transport/protocol`
- **Expected win:** 1.15–1.40x for payloads ≥ 16 KiB on the remote path
- **Platforms:** all (per-platform primitive)

**Change.**

1. In `segment.go`, keep a second fd per closed segment (`fdForSendfile *os.File`) alongside the read handle.
2. In `server/handlers_log.go`, `handleFetch`/`handleReadBatch`: when the client's connection is a `*net.TCPConn` and the payload exceeds 16 KiB, use `sendfile` (Linux), `TransmitFile` (Windows), or `sendfile` on Darwin/BSD — routing through a per-platform `internal/server/sendfile_{os}.go`.
3. Wire framing (offset, ID, timestamp, group, length) is still `write` — only the payload is `sendfile`'d.
4. Bench with `--payload-size 64KiB` or larger; small payloads will show no win.

**Tests.** `TestSendfileMatchesCopy` — remote-fetch 1000 records of 64 KiB, assert byte-equality with the copy path. Plus a hook `sendfileHook` to assert the syscall was used.

**Commit:** `[perf] server: <ratio>x; sendfile/TransmitFile for remote reads`

---

### Phase 20 — Long-tail synthesis

- **Packages:** `internal/engine`, `internal/server`
- **Expected win:** 1.05–1.15x, cumulative

A grab-bag, each item measurable independently:

1. **NUMA-aware shard assignment** (Linux): pin shards to NUMA nodes via `runtime.LockOSThread` + `sched_setaffinity`.
2. **Huge pages** on ring cache and mapping buffers: `madvise(MADV_HUGEPAGE)` on Linux; `VirtualAlloc` with `MEM_LARGE_PAGES` on Windows (requires SeLockMemoryPrivilege — make it optional).
3. **SIMD checksums** via `simd/archsimd` (Go 1.26+): replace `crc32.ChecksumIEEE` with a vectorized CRC32C on amd64/arm64.
4. **Sparse + tiered index**: for queues > 1 GiB, build a sparse top-level index (Kafka-style `.index`) over the dense per-segment index. Speeds up seeks in large queues.
5. **Adaptive readahead**: track recent read patterns per queue; issue `fadvise(WILLNEED)` for the next 4 MiB of the anticipated scan.

**Tests.** Each item gets its own test. SIMD CRC needs a golden vector comparison against `crc32.ChecksumIEEE`.

**Commit:** `[perf] engine: <ratio>x; NUMA, huge pages, SIMD CRC, tiered index, adaptive readahead`

---

## Cross-cutting notes

**Revert-friendly phases.** Phases 1–5 each stand alone. Phases 6–8 build on 1–5 but can be reverted individually. Phase 9 is Windows-only and safe to revert. Phases 12, 13, 14 are independent. Phase 15 must be kept if 16 or 17 are kept (they implement its interface). Phase 18 depends on 15. Phase 19 depends on 12. Phase 20 items are independent.

**When a phase underperforms.** Don't force the ratio. If phase 3 shows 1.05x instead of the expected 1.4x, that's evidence the channel isn't the bottleneck at this workload — record the actual number, keep the change if it doesn't regress anything, and update the plan's expectation for the next phase based on the new evidence.

**Total phases: 20.** Critical path for Windows: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 9 → 12 → 14 → 15 → 18. Everything else is either a Linux-specific win (16, 17) or architectural polish (11, 13, 19, 20).