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

## Where things stand (this machine)

- Produce: ~30k → ~160–180k msgs/s async (~6x); ~95–123k sync (was 25k)
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
