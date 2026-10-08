# Performance Run — Results Log

Sister document to [PLAN.md](PLAN.md): what was attempted, what shipped, what was
reverted, and what we learned. One line per outcome; commit hashes are traceable.

## Shipped

- Phase 1 — batch WAL record writes: **1.91x** produce (`cec97a4`)
- Phase 2 — batch index writes: **2.40x** produce on top of phase 1 (`04a3e4c`)
- Phase 3 — release `q.mu` before blocking on the async channel: **1.10x** (`a5ca229`)
- Phase 5 — gate ring cache on first reader, drop per-append clone: **1.08x mix** (`863fc86`)
- Phase 6 — pool encode/coalesce/index buffers: **1.09x** (`6f02bf6`)
- Phase 9 — Windows `FILE_FLAG_OVERLAPPED` segment handles: **1.08x** (`e6e512c`)
- Phase 12 (replaced) — batched `ReadBatch`: **4.0x consume, 1.11x mix** (`6fb32f8`)
- Phase 14 — lock-free ring-cache reads (per-slot atomic pointer, not the
  planned seqlock: Go's memory model forbids its plain field reads): **1.03x mix** (`e2bd57b`)
- Sync wake fix — wake the flusher on every sync append: **~2x sync** (`bd5d41c`)
- Bench default workers 4 → 8 (`b1bd19e`)
- Cheap record IDs — process-scoped counter, no per-record `crypto/rand`: **1.11x async, 1.15x sync** (`39d2741`)
- Encode outside `q.mu`: **1.16x sync-16**, flat elsewhere (`965da8a`)
- `iow.Writer` phase 1 — interface + portable pool (`51bde14`)
- `iow.Writer` phase 2 — WAL routed through the pool: async **+5.7%**,
  p99 18.2→7ms (`6f8b72b`)
- `iow.Writer` phase 3 — io_uring + IOCP + WAL collapse (`f6c2479`): Windows
  async **1.3x** over the pool (250k vs 183–193k), p99 8.7→1.8ms; Linux async
  **+17–30%**, sync p50 **−30%**
- Ring gate fix — kernel release read from `/proc`, so the ring engages at all
  (`4187e29`)

## Attempted and reverted

- Phase 4 v1 (cheap IDs) — no win while the pipeline was syscall-bound; re-landed
  for real once phases 1/2/6 exposed the CPU path (`39d2741`)
- Phase 12 (mmap the active segment) — on Windows, views created *before* writes
  force every reader page fault through the section/cache round trip; consume
  collapsed 13x. Reverted; replaced by batched reads
- Phase 13 (mmap write-through) — dirty mapped pages are written back in small
  chunks by the OS, starving subsequent reads: consume −65%. Reverted
- Phase 18 (range-chunked sharding) — built, measured 1.00x (group commit already
  amortizes fsyncs; the reservation path stays one lock), reverted per plan
  discipline. Design preserved below
- klauspost CRC32: no gains on noisy benchmark runs, actively slower on isolated BenchmarkEncodeRecord-8
  by ~22% (mean of 10 runs) with more spread
- IOCP under `DataSync` — overlapped submission buys nothing when a batch
  serializes on its own fsync: ~55k vs the pool's 63–67k, p50 8.6–9.6 vs
  6.8–7.1ms. The port now serves `Buffered` only; sync stays on the pool
- Capping the WAL's async slot budget to shorten the tail — `max` unchanged and
  p90/p99 worse (1.1/1.8 → 1.6/3.6ms). Reverted

## Key findings

- **Windows rule:** bulk streaming belongs in the cache manager (`WriteFile`);
  mapped views on a file being written are a documented slow path (phases 12, 13)
- **Sync cadence was ticker-driven:** producers only woke the flusher at the
  1 MiB budget, so flushes ran on the 10 ms `MaxFlushWait` ticker and the
  flusher idled ~65% of its life. Wake-on-every-sync-append fixed it
- **Group commit converts concurrency into sync throughput**; 16 workers regress
  async by 18% (`q.mu` convoy) — workers default stays 8
- **Device profile (this laptop):** 361 MiB/s single-stream fsync; 1.72x
  aggregate with 4 concurrent fsync streams; engine synced ceiling ~104 MiB/s,
  bounded by worker-side staging, not fsync parallelism
- **Bench noise is ±10–20%** (browser/thermal/bimodal scheduler states);
  interleave before/after pairs and compare medians, never single runs
- **The ring was dead code on every real kernel:** `strings.Cut` on the `uname`
  release split at the *first* dot, so any 3+ component version
  (`5.15.0-generic`, `4.19.0-21-amd64`) failed `Atoi` and the factory silently
  fell back to the pool. A green test run hid it — the parity subtest for the
  ring simply did not appear. Read `/proc/sys/kernel/osrelease` now, which also
  fixes the linux/arm64 build
- **`max` does not rank writers:** it is one sample of 8.4M and swung 5.9s→8.7s
  across identical binaries. Per 8.4M appends the port had ~205 samples over
  100ms against the pool's ~260 and the pre-Phase-1 sink's ~280, with the best
  p99 and p999. Counts and percentiles decide, not the worst sample
- **Multi-second tails are device writeback, not the write path:** ~30 per 8 GiB
  run in every writer, matching the 32 segment rolls that flush a sealed 256 MiB
  segment
- **Micro-benchmarks hide engine interactions:** the two Windows writers were
  equal in isolation (7.2 vs 7.5 ms per batch) and 20% apart inside the engine
- **Ordered delivery must advance its frontier after invoking:** firing callbacks
  outside the lock let a preempted setter's batch be overtaken, breaking the
  one invariant the WAL's byte watermark depends on

## Where things stand (this machine)

- Produce: ~30k → ~160–180k msgs/s async (~6x); ~95–123k sync (was 25k)
- Windows produce now splits by writer: async **246–250k** on the completion
  port (pool 183–193k, pre-Phase-1 241k); sync **63–67k** on the pool, port
  declined for `DataSync`
- Linux produce (WSL2, virtual disk): ring **121–147k** async, **25–27k** sync
  at p50 13.5–15.2ms; pool 93–113k async at p99 23–31ms
- Consume (cold 512 MiB backlog): 343k → **1.35M+ msgs/s** (~4x)
- Sync path fsync latency floor (~2.7 ms) is now the per-generation cost;
  worker-side staging (`q.mu` hold, encode) is the remaining serialization point

## Open items

- **18b (per-shard files):** designed (rank-based index addressing, per-shard
  reservation frontiers, truncation holes) — est. 3–5 days; pays only on NVMe
  sync-durability targets. Precondition: land cheap IDs + lock-scope work first
  (cheap IDs now done; `q.mu` staging loop remains)
- Fsync-generation grouping: linger/budget interplay re-measurable after any
  staging-path change
- 18b's sibling idea: expose sharding through `spec.Table` only when a real
  deployment target exists
- **Linux numbers are from WSL2's virtual disk** (`/dev/sdd`); its tails are poor
  across the board, so read them as writer-vs-writer on identical storage.
  Confirm on bare metal before quoting them
- **The Windows sync regression's root cause is still unexplained:** equal in
  isolation, −20% inside the engine. Bounded out of `DataSync` rather than
  understood
- The IOCP `max` exceeds the pool's (5.9–8.7s vs 2.9s) on saturating runs;
  writeback explains it by inference, not by instrumentation
- `IORING_SETUP_SQPOLL`, registered buffers, and `IOSQE_IO_LINK` fsync are not
  shipped; the ring currently uses plain `io_uring_enter` submits, so its Linux
  win has headroom left
