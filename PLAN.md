# mikyu append-path performance improvements

Each phase has eight sections:

1. **Goal** — one sentence. What this phase accomplishes.
2. **Preconditions** — which phases must already be merged, and which invariants must hold. If a precondition is unmet, stop and revisit the previous phase; do not improvise.
3. **Files** — every file the phase touches, with the function or type inside it that is the primary edit point.
4. **Changes** — a step-by-step list of the edits, with signatures.
5. **Tests** — the existing tests that must stay green, plus the new tests this phase introduces, with names and assertions.
6. **Bench** — the exact commands to run before/after, plus the specific profile artefact that must move.
7. **Rollback** — how to revert, and how long the compatibility shim stays in the tree.
8. **Gotchas** — the non-obvious failure modes; each is a real thing that has burned someone on this codebase or a comparable one.

Read the ground rules below before you touch anything. They are not advice.

---

## Ground rules (unchanged, expanded)

- **One phase per commit.** Each commits message is `[perf] <packages> : <one line summary of edit and improvement ratio>`.
- **One benchmark command.** Iterate with `.\mikyu bench run --duration 10s --warmup 3s`. Accept a phase with `.\mikyu bench run --duration 1m --warmup 5s`. Both run on AC power with a warm cache and nothing else on the machine.
- **Profile every phase.** `go test ./internal/engine -bench=BenchmarkQueueAppend -benchtime=30s -cpuprofile=cpu.out -mutexprofile=mutex.out`. Then `go tool pprof -top -nodecount=20 cpu.out` and `go tool pprof -top -nodecount=10 mutex.out`. Save both to the commit description. Do not merge a phase whose profile does not visibly move the intended target.
- **Never break these tests:**
  - `internal/engine/queue_test.go: TestQueueRealCrashRecovery`
  - `internal/engine/queue_test.go: TestQueueUngracefulCrashRecovery`
  - `internal/queue/taskqueue/taskqueue_test.go: TestTaskQueueCheckpointRecovery`
  - `internal/engine/queue_test.go: TestQueueAppendBatchAcrossSegmentRoll`
  - `internal/engine/compact_test.go: TestCompact_CrashRecoveryAfterTruncation`
  - Any phase touching the write path must additionally keep `internal/node/durability_test.go: TestRegistryWriteFsyncsDirectory` and `internal/engine/durability_test.go: TestDirectoryFsyncSites` green.
- **Never chain phases.** Phase 3 depends on Phase 2 being merged *and measured*. Phase 4 depends on Phase 3 being merged *and measured*. If you find yourself wanting to "also do a quick fix for the thing I saw", open a follow-up commit. Bisectability is worth more than your time.
- **Never invent a new file format.** Every phase that touches the record, index, or offset layouts must either preserve them bit-identically or explicitly bump a version. Phases 2, 5, and 8 preserve; the deferred `crc32.Castagnoli` item does not.

---

## Phase 0 — Baseline and profile

### Goal

Capture the number every later phase will be compared against, and the three profiles that tell you which later phase is load-bearing. Without this, you will ship Phases 4, 5, and 6 on faith and discover Phase 2 alone captured 90% of the win.

### Preconditions

None. This is the first phase.

### Files

No production code changes. The following files' benchmarks and profiles are the deliverables:

- `internal/bench/bench.go` — `BenchmarkQueueAppend`, driven by `mikyu bench run`.
- `internal/engine/metrics_test.go` — `BenchmarkQueueAppendWithMetrics`, `BenchmarkQueueAppendWithoutMetrics`.
- `internal/engine/stress_bench_test.go` — `BenchmarkProduceOnly`, `BenchmarkMixedWorkload`, `BenchmarkBatchOperations`, `BenchmarkHighConcurrencyStress`.
- `internal/engine/wal_test.go` — `BenchmarkWALFlushAllocations`.

### Changes

1. **Fixing the bench host.** Record the exact machine in the commit: `Dell Latitude 7390, i5-8350U, Windows 10 build NNNNN`. Confirm the process runs on AC power (`powercfg /batteryreport` shows AC). Disable Windows Defender's real-time protection for the `mikyu-bench` directory only.
2. **Warm-up run.** Run the bench once with `--warmup 10s --duration 5s` and discard the result. This populates the page cache and lets the Go runtime's GC settle.
3. **Three CLI bench runs.** `.\mikyu bench run --duration 1m --warmup 5s`, three times. Record `throughput_msgs_per_sec`, `latency_corrected.p50_ns`, `latency_corrected.p99_ns`, and `throughput_bytes_per_sec`. The current expectation is ~68k msg/s, p50 ≈ 1 ms, p99 ≈ 19 ms.
4. **CPU profile.**
   ```powershell
   go test ./internal/engine -bench=BenchmarkQueueAppendWithMetrics -benchtime=30s -cpuprofile=cpu.out -mutexprofile=mutex.out -blockprofile=block.out -memprofile=mem.out
   go tool pprof -top -nodecount=30 cpu.out > cpu.baseline.txt
   go tool pprof -top -nodecount=15 mutex.out > mutex.baseline.txt
   go tool pprof -top -nodecount=15 block.out > block.baseline.txt
   ```
5. **Escape analysis.** `go build -gcflags=-m -o NUL ./internal/engine ./internal/queue/logqueue > escapes.baseline.txt 2>&1`. Grep for `escapes to heap` in the encode and append paths. This is how a later phase catches an accidental escape.
6. **Scheduler trace.** `$env:GODEBUG="schedtrace=1000"; .\mikyu bench run --duration 10s 2>sched.baseline.txt`. Extract the maximum `M<N>` value from the trace. Record it.
7. **Syscall baseline.** Run Process Monitor filtered on `Path ends with .log AND Path contains mikyu-bench`. Record `Operations per second` for `WriteFile` and `ReadFile`. Expect ~136k/s across all files.

### Tests

None new. Confirm all named crash-recovery tests pass on the baseline build.

### Bench

This *is* the bench.

### Rollback

N/A.

### Gotchas

- **Do not run the baseline on battery.** Windows throttles the CPU aggressively when on battery; the entire phase table will be wrong.
- **Do not skip the warm-up run.** The first bench run after a cold build has different GC and page-cache behavior, and the number will be ~30% lower.
- **Save `escapes.baseline.txt`.** Several later phases will change escape behavior. Without this file, you cannot prove a phase did not regress it.

### Expected top items in `cpu.baseline.txt`

- `hash/crc32.ChecksumIEEE` — the IEEE polynomial, ~5% of CPU.
- `crypto/rand.Read` via `uuid.NewV7` — on Windows this lands in `BCryptGenRandom`.
- `internal/engine.(*wal).writeAt` and `syscall.WriteFile` — the WAL write syscall.
- `sync.(*Mutex).Unlock` for `q.mu` — the queue's own lock.
- `sync.(*Mutex).Unlock` for `ringCache.mu`.

If any of these is *not* in the top 10, stop and re-profile; your baseline is not the one the plan was written against.

---

## Phase 1 — Windows overlapped file handles

### Goal

Make the WAL's `WriteAt` and the index's `WriteAt` non-blocking from the Go scheduler's perspective on Windows, so the runtime can park fewer M's and the IOCP path handles completion.

### Preconditions

Phase 0 complete, baseline captured.

### Files

- `internal/engine/segment.go` — `openSegment`, `enableMMap`, `destroy`, `Close`. Currently opens `logFile` and `idxFile` via `root.OpenFile(logName, os.O_CREATE|os.O_RDWR, 0o644)`.
- **New file** `internal/engine/segment_windows.go`.
- **New file** `internal/engine/segment_other.go` with `//go:build !windows`.

### Changes

1. **Extract file opening into build-tagged helpers.** In `segment.go`, replace the two `root.OpenFile` calls inside `openSegment` with:
   ```go
   logFile, err := openSegmentFile(root, logName, true)
   idxFile, err := openSegmentFile(root, idxName, true)
   ```
   The third parameter is `create`; both call sites currently pass `os.O_CREATE|os.O_RDWR`.

2. **`segment_other.go`** implements the non-Windows path trivially:
   ```go
   //go:build !windows
   package engine
   import "os"
   func openSegmentFile(root *os.Root, name string, create bool) (*os.File, error) {
       flags := os.O_RDWR
       if create { flags |= os.O_CREATE }
       return root.OpenFile(name, flags, 0o644)
   }
   ```

3. **`segment_windows.go`** implements the overlapped path. `os.Root.OpenFile` does **not** accept Windows-specific flags — the plan's original phrasing ("add `FILE_FLAG_OVERLAPPED` to the flags passed to `root.OpenFile`") is not possible through the public API. The correct approach is to resolve the name through the root for safety, then call `windows.CreateFile` directly:
   ```go
   //go:build windows
   package engine
   import (
       "os"
       "unsafe"
       "golang.org/x/sys/windows"
   )
   func openSegmentFile(root *os.Root, name string, create bool) (*os.File, error) {
       // Fail closed if the root would not permit it.
       if _, err := root.Stat(name); err != nil {
           if !create { return nil, err }
       }
       // Resolve to a path the OS can open, then CreateFile.
       // root.Name() is the directory; join manually.
       full := root.Name() + `\` + name
       p, err := windows.UTF16PtrFromString(full)
       if err != nil { return nil, err }
       access := uint32(windows.GENERIC_READ | windows.GENERIC_WRITE)
       share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE)
       var disp uint32 = windows.OPEN_EXISTING
       if create { disp = windows.OPEN_ALWAYS }
       h, err := windows.CreateFile(p, access, share, nil, disp,
           windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OVERLAPPED, 0)
       if err != nil { return nil, err }
       return os.NewFile(uintptr(h), name), nil
   }
   ```
   The `FILE_FLAG_OVERLAPPED` value is `0x40000000`; `windows.FILE_FLAG_OVERLAPPED` is the named constant.

4. **Comment the safety tradeoff.** The `root.Stat` call is a *check*, not a *guarantee*: a hostile process could in principle swap the directory entry between the stat and the `CreateFile`. That is acceptable here because the data directory is process-private. Write that comment; a future reader will assume otherwise.

5. **`mmap.Map` on an overlapped handle.** Confirm that `enableMMap` still works — on Windows, mmap on an overlapped handle is legal and the existing code path is unchanged.

### Tests

**Must stay green:** every crash-recovery test listed in the ground rules, plus `internal/engine/segment_test.go: TestSegmentMMap`, `internal/engine/segment_test.go: TestSegmentCloseUnmapsMMap`, `internal/engine/queue_test.go: TestQueueReadUsesMMapOnRolledSegments`.

**New tests:**

- `internal/engine/segment_windows_test.go: TestOpenSegmentFileOverlapped` — assert the returned `*os.File`'s `Stat()` reports a valid mode and that a write-then-read round-trips a known byte sequence. This is a smoke test, not a behavioral one.
- `internal/engine/segment_windows_test.go: TestOpenSegmentFileRespectsRoot` — assert that `openSegmentFile(root, "../escape.log", false)` fails (via the `root.Stat` check).

### Bench

```powershell
.\mikyu bench run --duration 1m --warmup 5s
$env:GODEBUG="schedtrace=1000"
.\mikyu bench run --duration 10s 2>sched.phase1.txt
```

**Expected deltas.** Throughput 1.2–2× (this is a real, known Windows win). p50 unchanged or slightly better. p99 tighter. Max `M<N>` in `sched.phase1.txt` should fall from O(100s) to O(10s) at steady state. If throughput improves by less than 20%, stop and re-profile before assuming Phase 2's premise still holds.

**Profile artefact that must move.** `syscall.WriteFile` sample count in `cpu.out` should drop; `runtime.park` sample count should drop by a factor of 5–10.

### Rollback

Revert the two build-tagged files and the two call sites in `openSegment`. One commit, one revert.

### Gotchas

- **Do not attempt to use `os.Root.OpenFile` with a Windows flag.** The API does not accept one. If you find yourself reaching for `syscall.O_...` to smuggle `FILE_FLAG_OVERLAPPED` through, you are on the wrong path.
- **Windows 10 with an SSD is fast; Windows 10 with a spinning disk is not.** If the target hardware has a spinning disk, the overlapped win will be smaller than the plan predicts. Record the disk model in the commit.
- **Overlapped handles cannot be used with `ReadAt` on some older Windows versions.** The Go runtime handles this since 1.25, but if the target OS is pre-Win10-1903, the phase may need a version check. Record the Windows build number in the commit.

---

## Phase 2 — Batched WAL writes

### Goal

Replace per-record channel sends, per-record `WriteAt`, and per-record `writeIndexAt` with per-batch operations. On the current code, a batch of 64 records performs 64 channel sends, 64 `writeAt` calls, and 64 `writeIndexAt` calls; this phase collapses them to 1, 1, and 1.

### Preconditions

Phase 1 merged and measured. All crash-recovery tests green.

### Files

- `internal/engine/wal.go` — `asyncEntry`, `pushAsync`, `drainAndFlushAsync`, `syncLoop`, `flush`, `newWAL`, `wal.bytes`, `wal.channelFill`.
- `internal/engine/append.go` — `(*Queue).AppendBatch`, the local `commitAsync` and `commitSync` closures, the `stagedBatch` type.
- `internal/engine/encode.go` — `encodeRecord`.
- `internal/spec/derive.go` — `FlushLinger`.
- `internal/engine/wal_test.go` — `TestWALWakeTokenLossRace`, `TestWALByteBackpressure`, `TestWALPushAndWait`, `TestWALBatch`.

### Changes

This phase is large. Break it into four sequential commits within the same commit; each commit must leave the test suite green.

#### Commit 2a — Introduce `batch` type and byte-bounded channel

1. **New type in `wal.go`:**
   ```go
   type batch struct {
       recs     [][]byte    // one slice per record, all sub-slices of payload
       offsets  []uint64    // logical offsets, parallel to recs
       positions []int64    // on-disk byte positions, parallel to recs
       seg      *segment
       basePos  int64       // position of recs[0] on disk
       recLen   int64       // sum of len(recs[i])
       payload  []byte      // the single backing buffer from the pool
   }
   ```
   The `payload` field's ownership transfers to the flusher on a successful push; the flusher returns it to the pool.

2. **Change `asyncChan`'s element type.** In `newWAL`:
   ```go
   // before: asyncChan chan asyncEntry, cap = max(1, int(cfg.MaxFlushBytes)/4096)
   // after:
   batchBytes := cfg.MaxFlushBytes // each batch is at most one flush budget
   chanCap := max(4, 4*int(cfg.MaxFlushBytes/batchBytes))
   asyncChan: make(chan *batch, chanCap)
   ```
   At `MaxFlushBytes=4 MiB`, `chanCap = 16` batches × 4 MiB = 64 MiB in flight worst case. This is fine; a real batch is bounded by the batch size (`w.Batch` × `MsgSize`), which the caller controls.

3. **Add a `sync.Pool` for payload buffers** at package scope in `wal.go`:
   ```go
   var batchPool = sync.Pool{
       New: func() any { b := make([]byte, 0, 1<<20); return &b },
   }
   ```
   Size the pool at 1 MiB. Larger batches fall back to `make` and are not returned to the pool.

#### Commit 2b — Batch the append path

4. **`encodeRecord` gains an output buffer parameter:**
   ```go
   // before: func encodeRecord(id uuid.UUID, ts time.Time, group, payload []byte) []byte
   // after:  func encodeRecordInto(dst []byte, id uuid.UUID, ts time.Time, group, payload []byte) []byte
   ```
   The function appends to `dst` rather than allocating. Keep the old `encodeRecord` as a thin wrapper for callers that still allocate (compaction, task-queue records, tests). The escape analysis file from Phase 0 tells you which callers matter.

5. **In `AppendBatch` (append.go):** allocate one payload buffer for the whole batch:
   ```go
   var buf []byte
   if p := batchPool.Get(); p != nil {
       buf = (*p)[:0]
   } else {
       buf = make([]byte, 0, 1<<20)
   }
   ```
   Reserve capacity for `sum(len(payloads[i])) + len(payloads)*recordOverhead`. Then encode each record into `buf`:
   ```go
   for i, payload := range payloads {
       id := uuid.NewV7()
       before := len(buf)
       buf = encodeRecordInto(buf, id, ts, group, payload)
       recs[i] = buf[before:len(buf)]
   }
   ```

6. **`commitAsync` closure** takes the whole batch. Replace the per-record loop with one call:
   ```go
   commitAsync := func(b *batch) error {
       if err := b.w.pushAsyncBatch(ctx, b); err != nil { return err }
       for i := range b.recs {
           q.cache.Put(Record{Offset: b.offsets[i], ID: ..., Timestamp: ts, Group: group, Payload: bytes.Clone(payloads[startIdx+i])})
       }
       b.seg.pos += b.recLen
       last := b.offsets[len(b.offsets)-1]
       if last+1 > q.publishedOffset { q.publishedOffset = last + 1 }
       return nil
   }
   ```
   Note: `q.cache.Put` still allocates via `bytes.Clone`. Phase 3 removes the per-record allocation; do not fix it here.

7. **`pushAsyncBatch`** in `wal.go` replaces `pushAsync`:
   ```go
   func (w *wal) pushAsyncBatch(ctx context.Context, b *batch) error {
       if err := ctx.Err(); err != nil { return err }
       select {
       case w.asyncChan <- b:
           return nil
       default:
       }
       w.m.blockEvents.Inc()
       w.m.blockedProducers.Add(1)
       defer w.m.blockedProducers.Add(-1)
       select {
       case w.asyncChan <- b:
           return nil
       case <-ctx.Done():
           return ctx.Err()
       case <-w.shutdown:
           return ErrQueueClosed
       }
   }
   ```

#### Commit 2c — Batch the flusher

8. **`drainAndFlushAsync` takes a `*batch` and drains up to 3 more with a spin budget:**
   ```go
   func (w *wal) drainAndFlushAsync(first *batch) {
       batches := []*batch{first}
       total := first.recLen
       for i := 0; i < 3; i++ {
           select {
           case b := <-w.asyncChan:
               batches = append(batches, b)
               total += b.recLen
               if total >= w.cfg.MaxFlushBytes { goto WRITE }
           default:
               goto WRITE
           }
       }
   WRITE:
       // ...
   }
   ```

9. **One `WriteAt` per segment.** Group batches by segment. In the common case all batches share `w.seg`; the roll case is rare. For each segment group:
   - Concatenate payloads into one `w.buf` slice.
   - Compute the group's `basePos` (the `positions[0]` of the first batch's first record).
   - Call `w.writeAt(concatenated, basePos)` once.
   - Call `w.file.Sync()` once (sync path only).

10. **One `WriteAt` per segment for the index.** Accumulate `(pos, len)` pairs into a reused scratch buffer of `rawIndexEntrySize * totalRecords` bytes. One `idxFile.WriteAt(scratch, baseOff*rawIndexEntrySize)` per segment group.

11. **`writeAt` remains the only WAL writer.** Keep the `writerHook` call at the top of `writeAt`; `TestSingleWriter` depends on it.

12. **Return buffers to the pool.** After a successful write, iterate `batches` and `batchPool.Put(&b.payload)`. If a write failed, do not return the payload — the batch is dead and the buffer may be in a torn state.

#### Commit 2d — Linger reduction

13. **In `internal/spec/derive.go`, change `FlushLinger`:**
    ```go
    // before: return max(time.Duration(0), wait.Duration()/8)
    // after:
    return max(time.Duration(0), wait.Duration()/128)
    ```
    At `MaxFlushWait=10ms`, this is 78 µs — within the plan's 50–100 µs target. Do not go below 50 µs; below that the flusher spins and you trade latency for CPU.

14. **Document the tradeoff** in the `FlushLinger` doc comment: "the derived linger bounds how long a batch waits before flushing; below ~50µs the flusher's own scheduling dominates." Update the `--max-flush-wait` help text in `internal/ui/command/help.go` if it mentions the derived linger.

### Tests

**Must stay green:** all crash-recovery tests, plus `TestWALPushAndWait`, `TestWALBatch`, `TestWALWakeTokenLossRace`, `TestWALByteBackpressure`, `TestWALErrorPropagation`, `TestQueueAppendBatchAcrossSegmentRoll`, `TestQueueConcurrentBatchExactOffsets`.

**New tests:**

- `internal/engine/wal_test.go: TestPushAsyncBatchSingleWrite` — assert one `writeAt` call for a batch of N records by installing a counting `writerHook`.
- `internal/engine/wal_test.go: TestPushAsyncBatchSpansSegments` — push a batch that straddles a roll; assert the flush splits into two `writeAt` calls, one per segment.
- `internal/engine/wal_test.go: TestBatchBufferReturnedToPool` — after a successful flush, assert `batchPool.Get` returns a buffer of the expected capacity.
- `internal/engine/append_test.go: TestAppendBatchAllocatesOneBufferPerBatch` — assert `testing.AllocsPerRun` for a batch of 64 × 1 KiB records is under 100. The 100 accounts for the `batch` struct, the `recs`/`offsets`/`positions` slices, and the `q.cache.Put` clones. Phase 3 will drive this lower.

### Bench

```powershell
.\mikyu bench run --duration 1m --warmup 5s
go test ./internal/engine -bench=BenchmarkQueueAppendWithMetrics -benchtime=30s -cpuprofile=cpu.phase2.out
go tool pprof -top -nodecount=30 cpu.phase2.out > cpu.phase2.txt
```

**Expected deltas.** Throughput 3–5×. p50 down to ~100–200 µs. p99 under 5 ms. Syscalls/s from ~136k to ~2k (Process Monitor on the `.log` files). CPU profile top item shifts from `writeAt`/`io` to `crc32.ChecksumIEEE`.

**Profile artefact that must move.** `internal/engine.(*wal).writeAt` sample count drops by ~64×. `internal/engine.(*segment).writeIndexAt` drops by ~64×. `runtime.chansend` drops proportionally.

### Rollback

Keep the old `asyncEntry` type and the old `pushAsync` behind `walLegacyAsync bool` for one commit cycle. Both paths share `syncLoop`; the loop dispatches on the flag. Remove the flag in the next commit (Phase 3's commit is a fine place to do it).

### Gotchas

- **`TestQueueAppendBatchAcrossSegmentRoll` is the highest-risk test for this phase.** A batch that spans a roll must split into two groups and both must be written before `wait(seq)` returns. If you find the test flaky, the batch grouping is wrong; do not "fix" the test.
- **The `batch` struct's `payload` field's ownership must be unambiguous.** The pusher owns it until `pushAsyncBatch` returns; the flusher owns it after. A double-return to the pool is a data corruption bug that will surface as a CRC failure, not as a crash.
- **`w.buf` is no longer a single batch.** The old `flush` swap-buffer optimization no longer applies. Remove `w.swapBuf` and `w.swapPending` or the struct will grow unbounded.
- **`TestWALWakeTokenLossRace` will fail if you forget to drain the wake channel.** The wake channel's contract is unchanged, but the batch version has more places to lose a token. Run this test 100 times with `-count=100` before merging.
- **`FlushLinger` is read in `newWAL` and `ApplyConfig`.** Both must pick up the new derivation; the change is in `spec.FlushLinger`, which both call. Verify `TestApplyConfigLiveEffect` still passes.

---

## Phase 3 — Sharded ring cache

### Goal

Remove `ringCache.mu` as a serialization point by splitting the cache into 64 shards, each with its own lock and its own byte budget.

### Preconditions

Phase 2 merged and measured. `cache.mu` is now visible in the mutex profile because the producer is no longer waiting on the WAL.

### Files

- `internal/engine/cache.go` — `ringCache`, `newRingCache`, `Put`, `Get`, `stats`, `SetMaxBytes`, `EvictBefore`, `Evict`, `recBytes`, `nextPow2`.
- `internal/engine/cache_test.go` — `TestRingCacheByteCap`, `TestRingCacheEvictionAccountsBytes`, `TestRingCacheSlotBounds`.

### Changes

1. **New type:**
   ```go
   const ringCacheShards = 64
   const ringCacheShardBits = 6
   const ringCacheShardMask = ringCacheShards - 1

   type ringCache struct {
       shards [ringCacheShards]*ringCacheShard
   }

   type ringCacheShard struct {
       mu       sync.Mutex
       slots    []Record
       mask     uint64
       maxBytes int64
       curBytes int64
       head     uint64
       started  bool
   }
   ```

2. **`newRingCache(maxBytes int64)`** allocates 64 shards, each with `maxBytes/64` budget and `nextPow2(maxBytes/64/ringCacheAvgRecBytes)` slots (clamped to `[minRingCacheSlots/4, maxRingCacheSlots/4]` so the aggregate slot count matches the old single-shard count).

3. **`Put`** routes to `shard := r.shards[msg.Offset & ringCacheShardMask]`. The shard-local logic is identical to the old single-shard logic; only the `mu` and the head are per-shard.

4. **`Get`** routes the same way.

5. **`stats()`** sums `cur` and `max` across shards. The caller (`internal/engine/metrics.go: storageSnapshotLocked`) expects aggregate values; no interface change.

6. **`SetMaxBytes`** and **`EvictBefore`** iterate all shards, taking each shard's lock in turn. These are cold paths.

7. **`Evict`** takes a variadic list of offsets and routes each to its shard.

8. **Do not change `recBytes` or `nextPow2`.** They are used by the shard-local logic unchanged.

### Tests

**Must stay green:** `TestRingCacheByteCap` (must be updated to account for the per-shard budget), `TestRingCacheEvictionAccountsBytes`, `TestRingCacheSlotBounds`.

**New tests:**

- `internal/engine/cache_test.go: TestRingCacheShardIsolation` — put a record at offset `0`, then a record at offset `ringCacheShards` (the next shard), assert both are retrievable and that evicting one does not affect the other.
- `internal/engine/cache_test.go: TestRingCacheAggregateBudget` — put `ringCacheShards * shardBudget` records; assert `stats()` reports a `cur` no larger than `max`.
- `internal/engine/cache_test.go: TestRingCacheShardedConcurrentPutGet` — 32 goroutines doing interleaved puts and gets; the race detector must stay quiet and every put must be readable.

### Bench

```powershell
.\mikyu bench run --duration 1m --warmup 5s
go test ./internal/engine -bench=BenchmarkQueueAppendWithMetrics -benchtime=30s -mutexprofile=mutex.phase3.out
go tool pprof -top -nodecount=15 mutex.phase3.out
```

**Expected deltas.** Throughput 1.1–1.3×. p99 tighter. `cache.mu` (or `ringCache.mu`) disappears from the top 5 in the mutex profile; `q.mu` remains.

**Profile artefact that must move.** The `ringCache.mu` contention figure drops by ~64×.

### Rollback

Revert `cache.go` to the single-shard version. The shard count is a const; nothing else changes shape.

### Gotchas

- **The old `TestRingCacheSlotBounds` asserted exact shard capacity.** With 64 shards the assertion must become "aggregate slots ≥ old aggregate slots" rather than "slots == N". Update the test rather than the shard sizing.
- **Sharding by low bits means a batch of 64 sequential offsets all land in distinct shards.** That is correct and desirable: 64 records in 64 shards means no shard sees more than one record per batch. If you shard by high bits the whole batch lands in one shard and the lock is not relieved.
- **`SetMaxBytes(0)` is called by `TestCacheHitMissCounters` to force cache misses.** Confirm the sharded version honors a zero budget. This test is load-bearing for the read-path metrics.
- **The `head` per shard is not the same as the global `head`.** A shard that has never seen a `Put` has `started=false` and `head=0`; the first `Put` sets `head` to the record's offset. Preserve that logic.

---

## Phase 4 — Release `q.mu` on the append hot path

### Goal

Stop holding `q.mu` across the channel send. Under Phase 2 the channel rarely fills, so this is not a throughput win on its own; the win is in the mutex profile and in the p99 tail.

### Preconditions

Phases 1–3 merged and measured. The WAL's channel rarely blocks; the sharded cache can absorb the moved work.

### Files

- `internal/engine/append.go` — `(*Queue).AppendBatch`, `commitAsync`.
- `internal/engine/queue.go` — `roll`, `Close`, `loadSegments`.
- `internal/engine/wal.go` — the in-flight tracking.
- `internal/engine/offset_api.go` — `LastOffset`, `CommitOffset`, `Committed`.

### The ordering constraint, spelled out

Under the current code, `q.mu` makes three things atomic as a unit:

1. Offset assignment: `q.nextOffset++` per record.
2. Position assignment: `q.active.pos + staged.totalLen`.
3. Channel push (`commitAsync` or the sync-path `commitSync`).

If you release `q.mu` after (1) and (2) but before (3), a second producer can reserve *overlapping* positions: it reads `q.active.pos` before the first producer's `commitAsync` has advanced it, because the current code advances `q.active.pos` inside `commitAsync`, which runs under `q.mu`.

The correct fix is to move the advance of `q.active.pos` and `q.nextOffset` into the reservation phase, before the release:

### Changes

1. **Introduce a `reservation` struct** in `wal.go`:
   ```go
   type reservation struct {
       batches  []*batch
       bytes    int64
       done     chan struct{} // closed by the flusher when all batches land
   }
   ```

2. **`AppendBatch` reservation phase (under `q.mu`):**
   - Validate payload sizes (unchanged).
   - Encode records into the pooled buffer (unchanged).
   - Assign offsets: `offsets[i] = q.nextOffset; q.nextOffset++`.
   - Assign positions: `positions[i] = q.active.pos + staged.totalLen; staged.totalLen += recLen`.
   - **Advance `q.active.pos` by `staged.totalLen` immediately.**
   - Build the `*batch`.
   - Create a `*reservation` and register it with the WAL's in-flight set.
   - Release `q.mu`.

3. **Push phase (no lock held):**
   ```go
   if err := w.pushAsyncBatch(ctx, b); err != nil {
       w.abortReservation(r) // decrement in-flight, wake Close/roll
       return nil, err
   }
   ```

4. **Completion phase (reacquire `q.mu` only to publish):**
   ```go
   q.mu.Lock()
   if last := offsets[len(offsets)-1]; last+1 > q.publishedOffset {
       q.publishedOffset = last + 1
   }
   q.broadcastLocked()
   q.mu.Unlock()
   ```
   The `raise-if-higher` is a plain compare; out-of-order batches are fine because `publishedOffset` is monotonic.

5. **WAL in-flight set.** Add to `wal`:
   ```go
   inflight sync.WaitGroup // or a counter with a cond
   ```
   `pushAsyncBatch` calls `w.inflight.Add(1)` before the channel send. `drainAndFlushAsync`, after the write and index updates, calls `w.inflight.Done()`. `w.stop()` becomes:
   ```go
   func (w *wal) stop() error {
       w.stopOnce.Do(func() {
           close(w.shutdown)
           <-w.done
           w.inflight.Wait()
       })
       return w.err
   }
   ```

6. **`roll()`.** Currently:
   ```go
   if err := q.w.stop(); err != nil { return err }
   // truncate q.active.logFile to q.active.pos
   ```
   `stop()` now waits on the in-flight set, so all reserved batches are written before the truncate. **Do not** additionally change the truncate target.

7. **`Close()`.** Currently:
   ```go
   if q.w != nil { q.w.stop() }
   for _, seg := range q.segments { seg.Close() }
   ```
   `stop()` now covers the in-flight set. Verify with `TestQueueOperationsAfterCloseReturnErrQueueClosed`.

8. **`loadSegments()`.** The active segment is opened and its `pos` set from `seg.recover()`. No change; the reservation logic does not run until after load.

9. **`offset_api.go`.** `LastOffset` reads `q.publishedOffset` under `RLock`. Unchanged in this phase; Phase 6 makes it atomic.

### Tests

**Must stay green:** all crash-recovery tests, plus `TestQueueAppendBatchAcrossSegmentRoll`, `TestQueueConcurrentIndexDiskOrdering`, `TestQueueConcurrentProduceDuringRoll`, `TestQueueOperationsAfterCloseReturnErrQueueClosed`, `TestShutdownWakesParkedFetch`.

**New tests:**

- `internal/engine/queue_test.go: TestAppendBatchConcurrentReservations` — spawn N producers, each doing M batches of K records. After quiescence, assert `q.active.pos` equals the sum of all reserved batch sizes, and that `q.publishedOffset` equals the sum of all pushed batch sizes. Assert no two producers ever observed the same `(offset, position)` pair (collect them into a set with a mutex).
- `internal/engine/queue_test.go: TestAppendBatchReservationAbortOnContextCancel` — start a batch with a context that is canceled mid-push; assert `w.inflight` drops to zero and `q.active.pos` is not permanently ahead of `publishedOffset` (the reserved-but-not-pushed bytes are reclaimed).
- `internal/engine/queue_test.go: TestCloseDrainsReservations` — start a slow producer, call `Close` from another goroutine, assert `Close` blocks until the producer's reservation lands, then returns and the segment is truncated to the correct size.

### Bench

```powershell
.\mikyu bench run --duration 1m --warmup 5s
go test ./internal/engine -bench=BenchmarkQueueAppendWithMetrics -benchtime=30s -mutexprofile=mutex.phase4.out
```

**Expected deltas.** Throughput 1.2–1.5×. p50 ~50–100 µs. p99 under 3 ms. `q.mu` drops off the top 5 in the mutex profile.

**Profile artefact that must move.** `sync.(*Mutex).Unlock` for `q.mu` drops by 5–10× in the mutex profile. `schedtrace` M count stays flat.

### Rollback

Keep the reservation logic behind `walReservation bool` for one commit cycle. Both paths share the `AppendBatch` entry; the dispatch is at the top. Reverting is a bool flip.

### Gotchas

- **`q.active.pos` is now ahead of what is written.** Any code that reads `q.active.pos` and assumes it reflects on-disk state is now wrong. Grep for `active.pos` and audit each reader. The known readers are `roll` (truncate target), `Close` (truncate target), and `onDiskSizeLocked` (metrics). The metrics reader over-reports by the size of in-flight batches; that is acceptable and is a small, temporary skew.
- **The sync path is unchanged in this phase.** `pushBatch` + `wait` still hold `q.mu` across the flush. This is deliberate: the sync path has its own reservation logic and should be handled in a follow-up after the async path is proven correct.
- **`q.broadcastLocked` is called under `q.mu` in the completion phase.** A reader parked in `Fetch` wakes, reacquires `RLock`, and reads `publishedOffset`. That is unchanged.
- **`TestQueueConcurrentIndexDiskOrdering` asserts strictly increasing `pos` in the on-disk index.** With reservations, a producer that reserved early but pushed late will write a higher `pos` earlier in the file than a producer that reserved late and pushed early. The *index file* is per-offset, not per-write-order, so the assertion still holds. If it fails, the index write is racing the batch write — check that `writeIndexAt` is called from `drainAndFlushAsync` only.

---

## Phase 5 — Deterministic record IDs

### Goal

Remove `crypto/rand` (`BCryptGenRandom` on Windows) from the append hot path.

### Preconditions

Phase 4 merged and measured. Phase 5 is independent of the lock work but should land after it so its effect is isolated.

### Files

- `internal/engine/encode.go` — the `encodeRecord` function and its callers.
- **New file** `internal/engine/id.go` — the generator.
- **New file** `internal/engine/id_test.go`.

### Changes

1. **Before writing any code, benchmark Go 1.27's `uuid` package.** Write a throwaway `_test.go` in a scratch directory:
   ```go
   func BenchmarkUUIDStd(b *testing.B) {
       for b.Loop() { _ = uuid.NewV7() }
   }
   func BenchmarkUUIDSplitMix(b *testing.B) {
       var s uint64
       for b.Loop() { _ = splitmix64Next(&s) }
   }
   ```
   Run `go test -bench=BenchmarkUUID -benchtime=10s`. If stdlib `uuid.NewV7()` is within 10% of `splitmix64Next`, **use the stdlib**. The plan's preference for SplitMix64 is a fallback, not a mandate.

2. **If SplitMix64 wins, add `internal/engine/id.go`:**
   ```go
   package engine

   import (
       "crypto/rand"
       "encoding/binary"
       "sync"
       "sync/atomic"
   )

   type idGenerator struct {
       state atomic.Uint64
   }

   var globalIDGen idGenerator
   var idSeedOnce sync.Once

   func nextID() string {
       idSeedOnce.Do(func() {
           var seed [8]byte
           if _, err := rand.Read(seed[:]); err != nil {
               panic("mikyu: crypto/rand failed at id seed: " + err.Error())
           }
           globalIDGen.state.Store(binary.BigEndian.Uint64(seed[:]))
       })
       s := globalIDGen.state.Add(0x9E3779B97F4A7C15)
       // splitmix64 finalizer
       z := s
       z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
       z = (z ^ (z >> 27)) * 0x94D049BB133111EB
       z = z ^ (z >> 31)
       // v7 layout: 48-bit Unix-ms prefix, 12-bit rand, 62-bit rand
       ms := uint64(time.Now().UnixMilli())
       var buf [16]byte
       binary.BigEndian.PutUint16(buf[0:2], uint16(ms>>32))
       binary.BigEndian.PutUint32(buf[2:6], uint32(ms))
       binary.BigEndian.PutUint16(buf[6:8], uint16(z>>48)|0x7000) // version 7
       binary.BigEndian.PutUint16(buf[8:10], uint16(z>>32)|0x8000) // variant
       binary.BigEndian.PutUint32(buf[10:14], uint32(z>>16))
       binary.BigEndian.PutUint16(buf[14:16], uint16(z))
       return string(buf[:]) // or format as hex string, matching the old uuid.String()
   }
   ```
   The precise byte layout must match `uuid.NewV7()`'s string output so that `decodeRecord` still parses it. Check `uuid.UUID.String()`'s format and match it. The simplest correct path is to construct a `uuid.UUID` from the 16 bytes and call `.String()`, which costs one allocation but is still cheaper than `NewV7`.

3. **Change `encodeRecord`'s signature** to take the ID as `[16]byte` (or `uuid.UUID`) so the caller can supply a deterministically generated one:
   ```go
   // before: func encodeRecord(id uuid.UUID, ts time.Time, group, payload []byte) []byte
   // after:  func encodeRecordInto(dst []byte, id uuid.UUID, ts time.Time, group, payload []byte) []byte
   ```
   The `id` parameter's construction moves to the caller:
   ```go
   // in AppendBatch:
   id := nextUUID() // the SplitMix64-based constructor
   ```

4. **`nextUUID()` panics on seed failure.** `crypto/rand.Read` is documented as "guaranteed never to fail" since Go 1.24, so the panic is a should-never-happen. Panic is correct: silently falling back to a weak seed would violate the durability contract.

### Tests

**New tests:**

- `internal/engine/id_test.go: TestNextIDUnique1M` — generate 1,000,000 IDs, assert `len(set) == 1_000_000`.
- `internal/engine/id_test.go: TestNextIDVersionAndVariant` — for 1000 IDs, parse each as a UUID and assert `Version() == 7` and `Variant() == RFC4122`.
- `internal/engine/id_test.go: TestNextIDMonotonicWithinMillisecond` — freeze the clock with `testing/synctest`, generate 10,000 IDs, assert the 48-bit ms prefix is constant and the remaining bits are strictly increasing.
- `internal/engine/id_test.go: TestNextIDStringFormatMatchesStdlib` — generate one ID via `nextID()` and one via `uuid.NewV7()`, assert `len(a) == len(b)` and the version/variant nibbles match.

### Bench

```powershell
go test ./internal/engine -bench=BenchmarkUUID -benchtime=10s
.\mikyu bench run --duration 1m --warmup 5s
go test ./internal/engine -bench=BenchmarkQueueAppendWithMetrics -benchtime=30s -cpuprofile=cpu.phase5.out
```

**Expected deltas.** `BCryptGenRandom` and its `syscall` samples disappear from the CPU profile. Throughput 1.05–1.15× at `batch=64`. Larger at `batch=1` — this is why it matters for the sync path even though the current bench does not exercise it.

**Profile artefact that must move.** `crypto/rand.Read`, `BCryptGenRandom`, and the `syscall` frames under them disappear.

### Rollback

Revert `id.go` and the `encodeRecord` caller. One-line change in `AppendBatch`.

### Gotchas

- **Do not change the on-disk 16-byte layout.** `decodeRecord` reads a fixed 16-byte UUID at `offUUID`; if you change the byte order or the length, every existing segment becomes unreadable. `TestQueueRealCrashRecovery` will catch this, but only if you run it.
- **The v7 layout is a lie if you don't set the version and variant bits.** A UUID with version bits `0x0000` is a nil UUID fragment and `uuid.Parse` will reject it. Set `0x7000` on byte 6 and `0x8000` on byte 8.
- **`crypto/rand.Read` failing is a panic, not an error.** Do not plumb an error return through `AppendBatch` for this. If the OS's entropy source fails, the process is in an undefined state.

---

## Phase 6 — Concurrency hygiene

### Goal

Clean up the wakeup protocol and remaining shared state. These are small wins that become visible only after Phases 2–5 have removed the big ones.

### Preconditions

Phases 1–5 merged and measured.

### Files

- `internal/engine/queue.go` — `broadcastLocked`, `q.notify`, `q.closed`, `q.publishedOffset`, `q.nextOffset`, `q.minOffset`.
- `internal/engine/wal.go` — `w.notify`, `w.wake`.
- `internal/engine/fetch.go` — `Fetch`.
- `internal/engine/read.go` — `Read`, `ReadBatch`.
- `internal/engine/offset_api.go` — `LastOffset`, `CommitOffset`, `Committed`.
- `internal/engine/compact.go` — `MinOffset`, `Segments`.

### Changes

This phase has two independent sub-changes. Land them in one commit but as separate commits.

#### Commit 6a — `sync.Cond` for the wakeup protocol

1. **`Queue` struct:** replace `notify chan struct{}` with `notify *sync.Cond`. Initialize it in `openQueue`: `q.notify = sync.NewCond(&q.mu)`.
2. **`broadcastLocked`:** becomes `q.notify.Broadcast()`. Callers already hold `q.mu`; `sync.Cond.Broadcast` requires the lock, so the call site is unchanged.
3. **`Fetch`:** replace the `select` on `q.notify` with `q.notify.Wait()`. The `Wait` unblocks and re-checks `publishedOffset` and `closed`. Preserve the existing `q.closed` check at the top of the loop.
4. **`wal` struct:** replace `w.notify chan struct{}` with `w.notify *sync.Cond` (on `w.mu`) and `w.wake chan struct{}` with `w.wake *sync.Cond`. `flush` calls `w.cond.Broadcast()` (already does) and `w.wake.Broadcast()`.
5. **`pushAsync`:** replace the non-blocking `select` on `w.asyncChan` with a `for` loop around `cond.Wait`; the shutdown and ctx checks move into the wait loop. This is subtly different: the current code does one non-blocking try, then blocks on a select. The cond version must do the same: try, then wait on the cond with a ctx-aware watcher goroutine. If this is more complex than the channel version, keep the channel and skip this sub-commit; the win is small.

#### Commit 6b — atomic scalars

6. **`Queue` struct:** change the following fields:
   ```go
   // before:
   closed          bool          // guarded by q.mu
   publishedOffset uint64        // guarded by q.mu
   nextOffset      uint64        // guarded by q.mu
   minOffset       uint64        // guarded by q.mu
   // after:
   closed          atomic.Bool
   publishedOffset atomic.Uint64
   nextOffset      atomic.Uint64
   minOffset       atomic.Uint64
   ```
7. **`LastOffset`:** becomes `return q.publishedOffset.Load()`. No lock.
8. **`MinOffset`:** becomes `return q.minOffset.Load()`. No lock.
9. **`Read`:** reads `q.closed.Load()` and `q.minOffset.Load()`. The cache-hit fast path is unchanged; the segment-locate path still takes `RLock` because `q.segments` is a slice that can be reallocated by `roll`.
10. **`ReadBatch`:** same treatment.
11. **`CommitOffset` / `Committed`:** read `q.closed.Load()` and call `q.offsets` (whose lock is separate).
12. **`Close`:** `q.closed.Store(true)`.
13. **Run `go fix ./...` with the `atomictypes` modernizer.** It converts the temporary `atomic.Uint64(&q.nextOffset, ...)` form (if you wrote it that way) to `q.nextOffset.Add(...)`.

### Tests

**Must stay green:** `TestQueueOperationsAfterCloseReturnErrQueueClosed`, `TestShutdownWakesParkedFetch`, `TestShutdownWakesTail`, `TestShutdownWakesParkedDequeue`, `TestLogQueueTailBugProof`, `TestWALWakeTokenLossRace`, `TestWALByteBackpressure`.

**New tests:**

- `internal/engine/queue_test.go: TestLastOffsetNoLock` — assert that `LastOffset` does not take `q.mu` by running 1000 concurrent `LastOffset` calls while a writer holds `q.mu`; assert the readers complete in under a millisecond.
- `internal/engine/queue_test.go: TestMinOffsetNoLock` — same for `MinOffset`.

### Bench

```powershell
.\mikyu bench run --duration 1m --warmup 5s
go test ./internal/engine -bench=BenchmarkQueueAppendWithMetrics -benchtime=30s -cpuprofile=cpu.phase6.out
```

**Expected deltas.** 1.05–1.2×, mostly on p99. Fewer `runtime.newobject` calls (the per-batch channel allocation is gone). Fewer `runtime.chansend`/`runtime.chanrecv` samples.

### Rollback

Commit 6a and 6b are independent. Revert either.

### Gotchas

- **`sync.Cond` requires the lock to be held at `Wait` time.** The current `Fetch` does not hold `q.mu` when it selects on `q.notify`. The cond version must acquire `q.mu` before `Wait`, which means the surrounding loop must be restructured. Do not skip this; a cond waited on without the lock is a race that the race detector will catch only under load.
- **`atomic.Bool` is not copyable.** If `Queue` is ever copied, the atomic will be copied with its state. It isn't copied today; add a `//nolint:copylocks` comment if the linter complains.
- **Do not change `crc32.ChecksumIEEE` in this phase.** See Deferred.

---

## Phase 7 — GC tuning

### Goal

Stop the collector from paying for transient allocations that Phases 2–5 should have mostly eliminated.

### Preconditions

Phases 1–6 merged and measured. The allocation rate in `mem.baseline.txt` should have dropped 5–10×.

### Files

- `internal/bench/bench.go` — `Run`, `buildEnvironment`.
- `internal/bench/options.go` — `Options`, the new `GC` field.
- `internal/ui/command/bench.go` — `BenchFlagSet`, the new `--gc` flag.

### Changes

1. **Add a `GC bool` field** to `bench.Options`. When set, `Run` writes `GOGC=400` and `GOMEMLIMIT=<80% of current RSS>` to the process environment before the first `mikyu.Open`.

2. **Read RSS at flag-set time.** `runtime.ReadMemStats` gives `Sys`, not RSS. Use the `internal/bench/host.go: readHostStats` function (which already reads RSS) to get the current figure.

3. **`--gc` is off by default.** The plan does not ship tuned defaults. If a user wants the tuned run, they pass `--gc`.

4. **Add the flag to `benchRunFlags`** in `internal/ui/command/bench.go` so `TestBenchFlagSetCoversEveryParam` and `TestBenchHelpListsEveryWorkloadFlag` continue to pass. The flag is a presentation flag, not a workload param; add it to `ExtraFlags` in the `bench run` verb in `internal/ui/command/verb.go: benchVerb`.

5. **`gctrace` is not a shipped feature.** The user runs with `GODEBUG=gctrace=1` manually; the `--gc` flag does not set it.

### Tests

**New tests:**

- `internal/bench/gc_test.go: TestGCEnabledSetsEnv` — with `GC: true`, assert `os.Getenv("GOGC") == "400"` and `GOMEMLIMIT` is set, after `Run` returns (the env persists for the process).
- `internal/bench/gc_test.go: TestGCDisabledLeavesEnvAlone` — with `GC: false`, assert `GOGC` is unchanged.

### Bench

```powershell
$env:GODEBUG="gctrace=1"
.\mikyu bench run --duration 10s 2>gc.before.txt
.\mikyu bench run --duration 10s --gc 2>gc.after.txt
.\mikyu bench run --duration 1m --warmup 5s --gc
```

**Expected deltas.** Fewer GC cycles, longer average pause, same total pause. Throughput 1.05–1.15×. Do not chase more.

### Rollback

Unset the env vars. The flag is off by default.

### Gotchas

- **`GOMEMLIMIT` is a soft limit; it does not prevent OOM.** If the tuned run OOMs, lower the limit. Document this in the flag help.
- **Do not change the shipped defaults.** A future maintainer will read this phase as "ship GOGC=400" if you are not explicit. Be explicit in the flag's help text and in the commit description.

---

## Phase 8 — Task-queue offset store batching

### Goal

Stop `OffsetStore.Commit` from fsyncing per task-queue acknowledgment. On the log-produce bench this phase shows nothing; on the task bench it is 3–5×.

### Preconditions

Phases 1–7 merged and measured. The log-produce bench should be at 400–800k msg/s before you start this phase.

### Files

- `internal/engine/offsets.go` — `OffsetStore`, `Commit`, `recover`, `Close`, `compactLocked`.
- `internal/engine/offsets_test.go`.

### Changes

1. **Add a background flusher.** In `openOffsetStore`, after `recover`, start:
   ```go
   go s.flushLoop()
   ```
   `flushLoop` runs on a `time.Ticker` at 50 ms, plus a `flushSignal chan struct{}` for size-triggered flushes.

2. **`Commit` no longer fsyncs.** It writes to the buffered writer (`s.w`) and calls `s.w.Flush()` — that is, the bytes reach the OS, but no `file.Sync()`. It then signals `flushSignal` if the pending count reaches 256.

3. **`flush`** does:
   ```go
   s.mu.Lock()
   if s.pending == 0 { s.mu.Unlock(); return nil }
   if err := s.w.Flush(); err != nil { s.mu.Unlock(); return err }
   if err := s.file.Sync(); err != nil { s.mu.Unlock(); return err }
   s.pending = 0
   s.mu.Unlock()
   return nil
   ```

4. **`Close`** calls `s.flush()` before closing the file. Add `s.closed` flag and stop the flush loop.

5. **`recover`** is unchanged: it reads the same file format.

6. **`compactLocked`** is unchanged; it operates on the in-memory map.

7. **Document the durability tradeoff** in the `OffsetStore` doc comment: "commits are batched in memory and fsynced every 50 ms or every 256 commits; on crash, up to 50 ms of group-offset commits are lost. This is the same window Kafka's `auto.commit.interval.ms` accepts by default."

### Tests

**Must stay green:** `TestOffsetStoreCommitFetch`, `TestOffsetStoreRecovery`, `TestOffsetStoreTornRecord`.

**New tests:**

- `internal/engine/offsets_test.go: TestOffsetStoreFlushOnClose` — commit 10 offsets, call `Close`, reopen, assert all 10 are present.
- `internal/engine/offsets_test.go: TestOffsetStoreFlushLoop` — commit one offset, wait 100 ms, kill the process (simulate with a second `openOffsetStore` on the same dir after `Close`), assert the commit survived.
- `internal/engine/offsets_test.go: TestOffsetStoreSizeTrigger` — commit 256 offsets in a tight loop without sleeping, assert a `Sync` occurred (count via a hook) before the 50 ms ticker.

### Bench

```powershell
.\mikyu bench run --kind task --duration 1m --warmup 5s
.\mikyu bench run --duration 1m --warmup 5s    # log produce: expect unchanged
```

**Expected deltas.** Task bench 3–5×. Log-produce bench unchanged. If the log-produce bench moves, something is wrong — the log path does not call `OffsetStore.Commit` except on group commits.

### Rollback

Revert `offsets.go`. The on-disk format is unchanged; no migration.

### Gotchas

- **`OffsetStore.Fetch` reads `s.offsets` (the in-memory map), not the file.** Callers see the committed value immediately, even before the fsync. This is intentional and matches the pre-phase behavior; do not change it.
- **The flush loop must drain on `Close`.** A `Close` that returns before the last flush lands loses data on a clean shutdown, which is worse than losing it on a crash. Test this explicitly.
- **Windows file handles are closed by the OS on process exit; Linux is not.** The flush loop's `file.Sync()` is a no-op on a closed handle. Guard against a `Close` that races the flush loop.

---

## Deferred (with reasons, expanded)

Each item below is a real improvement. Each is deferred because it is either (a) gated on a phase not yet merged, (b) Linux-only and the current target is Windows, or (c) a single-file change whose bench does not move the CLI number. Land them opportunistically, not as phases.

### `crc32.Castagnoli`

~4% win at most, and it changes the on-disk checksum. The correct landing is a record-format version bump: add a magic byte at the top of each segment's first record, or a per-queue registry entry. That bump also gives you a place to add the migration path for `crc32.IEEE`-written segments. Do not bundle with any of Phases 1–8; do it as a standalone with its own handoff.

### Read-path zero-copy iterator

Only matters for `consume` and `mix` benches. `internal/engine/read.go: ReadBatch` currently copies each record's payload into a `[]byte` per record; a zero-copy iterator would yield sub-slices of the mmap. This changes `engine.Storage.ReadBatch`'s contract (the caller now owns a view into the mmap, which must not outlive the segment) and needs its own handoff. Do it after the produce path is settled.

### `sendfile`/`writev` egress

Gated on the read-path phase. Do not start it before then.

### io_uring, `O_DIRECT`, `sync_file_range`

Linux-only. Not applicable to the current target hardware. Each is a multi-week project. Land them when Linux becomes a primary target.

### Thread-per-core shard ownership

Preconditioned on Phases 1–6 landing and on `--queues 3` showing superlinear scaling. If a 3-queue bench on the current build does not scale superlinearly, this phase will not either. Measure first: `.\mikyu bench run --queues 3 --duration 1m --warmup 5s`. If throughput does not exceed 3× the single-queue figure, the bottleneck is still shared state and thread-per-core will not help.

### `unique.Handle` key index

`internal/engine/compaction.go: buildKeyIndex` currently uses `map[string]uint64`, which interns every key. `unique.Handle[string]` would canonicalize and reduce memory. Real improvement, but it does not move the CLI bench. Land opportunistically.

### `weak.Pointer` cache entries

`internal/engine/cache.go` holds `Record` values with strong payload references. `weak.Pointer` would let the GC reclaim cold entries under memory pressure. Real improvement, but not on the hot path.

### Goroutine-leak endpoint

`internal/node/node.go` could expose `/debug/pprof/goroutineleak`. Nice for operations, not a performance win. Land opportunistically.

### `--workers 8`

Confirmed flat on this hardware. Leave the default at 4. Re-evaluate if the target hardware changes.

---

## Expected outcome after Phases 1–7

| Metric | Baseline | After Phases 1–7 | After Phase 8 (task bench) |
|---|---|---|---|
| Throughput (log produce) | 68k msg/s | 400–800k msg/s | unchanged |
| Throughput (task) | ~40k msg/s | ~150k msg/s | 500–800k msg/s |
| p50 | 998 µs | 50–150 µs | unchanged |
| p99 | 19.4 ms | 1–3 ms | unchanged |
| Syscalls/s (WAL) | ~136k | ~2k | ~2k |
| Syscalls/s (offset store) | ~40k | ~40k | ~1k |
| M parks/s | high | near zero | near zero |
| Top mutex caller | `q.mu` | none | none |
| GC cycles/s | ~5 | ~1 | ~1 |
| `BCryptGenRandom` samples | visible | zero | zero |

## How to know a phase failed

If any single phase does not move the number by at least 10% on the CLI bench **and** does not visibly move the profile, stop and re-profile before continuing. The two most common failure modes:

1. **You merged a phase that captured no win because the previous phase already captured it.** Example: Phase 3 (sharded cache) after Phase 2 (batched WAL) — if Phase 2 reduced per-record cache puts to per-batch cache puts, the sharding win is smaller than the plan predicts. The fix is not to revert Phase 3; the fix is to update the plan's expected deltas and move on.
2. **You merged a phase whose win was masked by a downstream bottleneck.** Example: Phase 4 (release `q.mu`) after Phase 2 — if the WAL channel is still filling, releasing `q.mu` just moves the block. Check `wal_block_events` in the metrics before and after; if it does not drop, the phase did not work.

The rule is: measure between every phase, and if the profile does not move, stop and figure out why before touching the next phase.