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
- `iow` phase 1 — one `Writer` interface + portable worker pool (`51bde14`)
- `iow` phase 2 — WAL writes through `iow.Writer` (`6f8b72b`)
- `iow` phase 3 — io_uring (Linux) and overlapped I/O (Windows), WAL
  collapsed to one implementation (`f6c2479`)
- `iow` fix — read kernel version from `/proc` so the ring actually starts
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
- Overlapped I/O under **sync** (Windows): **15–20% slower** than the pool
  (~55k vs ~63–67k msgs/s). A sync batch waits on one fsync anyway, so
  overlapped buys nothing and the extra hand-off costs. Kept for async only
- Capping async in-flight batches to shrink the kernel queue: `max` unchanged,
  p90/p99 **worse** (1.1/1.8ms → 1.6/3.6ms). Reverted

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
- **One fastest writer does not exist.** The best writer depends on durability
  *and* OS: Linux ring wins both modes, Windows overlapped I/O wins async only.
  Each platform picks its own; callers never choose
- **`max` alone misleads.** It is one sample out of 8.4M and swings 5.9s→8.7s
  between identical builds. Judge by p99/p999 plus how many samples cross a
  threshold. The multi-second tails seen on every writer are the segment roll
  flushing 256 MiB on a busy disk (~30 per 8 GiB run), not a writer flaw
- **A green test run can hide a dead code path.** The ring read the kernel
  version wrong and silently declined on nearly every Linux kernel, so tests
  passed while running the pool. Verify the specialization actually engaged
  (check for its subtest) — a passing suite proved nothing here

## Where things stand (this machine)

- Produce: ~30k → ~160–180k msgs/s async (~6x); ~95–123k sync (was 25k)
- Consume (cold 512 MiB backlog): 343k → **1.35M+ msgs/s** (~4x)
- Sync path fsync latency floor (~2.7 ms) is now the per-generation cost;
  worker-side staging (`q.mu` hold, encode) is the remaining serialization point

Writer choice, 8 GiB produce, 8 workers, batch 64, 1 KiB records:

| | async | sync |
|---|---|---|
| Windows (this laptop) | overlapped **250k** vs pool 183–193k | pool **63–67k** vs overlapped 53–59k |
| Linux (WSL2, virtual disk — absolute numbers soft) | ring **121–147k** vs pool 93–113k | ring **25–27k**, p50 13–15ms vs pool 17–21ms |

## Open items

- **18b (per-shard files):** designed (rank-based index addressing, per-shard
  reservation frontiers, truncation holes) — est. 3–5 days; pays only on NVMe
  sync-durability targets. Precondition: land cheap IDs + lock-scope work first
  (cheap IDs now done; `q.mu` staging loop remains)
- Fsync-generation grouping: linger/budget interplay re-measurable after any
  staging-path change
- 18b's sibling idea: expose sharding through `spec.Table` only when a real
  deployment target exists
- **Linux ring needs a bare-metal run.** All Linux numbers so far come from a
  WSL2 virtual disk; verify the ring wins on real hardware before trusting it
- **Why Windows sync loses is still not fully explained.** The pool and the port
  cost the same per write+fsync in isolation, so the gap only appears inside the
  engine. Worth one profiling pass before building on it
