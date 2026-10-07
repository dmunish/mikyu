# `iow.Writer` — the complete cross-platform story

## 1. The principle

Three concerns are fused into the current WAL: *what to write* (records plus index entries), *how to write concurrently* (one flusher or the io_uring ring), and *what durability boundary to wait for* (page cache or fsync). Untangling them yields one interface with a portable reference implementation and per-platform specializations that drop in behind a single factory.

The interface is small enough that a platform can implement it in a few hundred lines. The pool is the reference every specialization is tested against.

## 2. The layers

```
┌────────────────────────────────────────────────────────────────┐
│ engine.Queue                                                    │
│   reservation, roll, offsets, key index                         │
├────────────────────────────────────────────────────────────────┤
│ wal                                                             │
│   staging, byte budget, durable watermark (one impl)            │
├────────────────────────────────────────────────────────────────┤
│ iow.Writer                                                      │
│   bounded, ordered, concurrent writes                           │
│   ├── pool           (all platforms — the reference)            │
│   ├── ring  (linux)  (io_uring; IORING_SETUP_SQPOLL)            │
│   ├── iocp  (windows)(overlapped I/O + completion port)         │
│   ├── kaio  (freebsd)(POSIX AIO via EVFILT_AIO)                 │
│   ├── ports (solaris)(event ports + aio)                        │
│   └── pool   (darwin, openbsd, netbsd, dragonfly, plan9, js…)   │
├────────────────────────────────────────────────────────────────┤
│ os.File + fdatasync / F_FULLFSYNC / FlushFileBuffers / …        │
└────────────────────────────────────────────────────────────────┘
```

Each layer depends only downward. The WAL never learns which specialization is below it.

## 3. The interface

New package, `internal/iow`.

```go
// Package iow provides bounded, ordered, concurrent writes to a set of files.
//
// A Writer accepts batches of writes at explicit offsets and reports
// completion for each. Multiple batches may be in flight at once, so the
// device stays busy while the OS throttles. Completions are delivered in
// submission order, which lets callers advance a monotonic durable
// watermark without a reorder buffer of their own.
//
// The same interface serves the WAL's log and index files, the offset
// store, and any future append-only destination. Platform specializations
// (io_uring on Linux, IOCP on Windows, POSIX AIO on FreeBSD) are drop-in
// implementations; the portable goroutine pool runs everywhere.
package iow

import (
    "context"
    "errors"
    "os"
)

// Write is one buffer at one offset of one file.
type Write struct {
    F   *os.File
    Buf []byte
    Off int64
}

// Durability is the boundary at which a batch's done callback fires.
type Durability int

const (
    // Buffered: done fires once the bytes are in the OS page cache.
    Buffered Durability = iota
    // DataSync: done fires once the bytes are on stable storage.
    DataSync
)

// Options configures a Writer.
type Options struct {
    // Workers is the number of batches that may be writing concurrently.
    // Zero selects GOMAXPROCS. One worker reproduces the old single-drain
    // sink; N workers keep the device queue populated across OS writeback
    // throttling. The payoff is under Buffered; under DataSync the device
    // serializes fsyncs anyway.
    Workers int

    // Outstanding bounds submitted-but-not-completed batches. Zero selects
    // Workers * 4. Write blocks while this many batches are in flight.
    Outstanding int

    // Durability selects the completion boundary.
    Durability Durability

    // SyncFiles names the files a DataSync completion must fsync. Empty
    // means every file written by the batch. The WAL passes only its log
    // file, because the index is rebuilt from the log on crash.
    SyncFiles []*os.File
}

// Writer is the durable-write contract. Safe for concurrent use.
//
// Ordering: a batch's done fires only after every earlier batch's done has
// already fired. Callers rely on this to maintain a strictly monotonic
// durable offset.
//
// Lifecycle: done fires exactly once for every Write that returns nil,
// including writes in flight when Close or a failure ends the Writer. After
// Close or a failure, Write returns immediately with ErrClosed or the
// recorded error; done is still called in that case, before Write returns.
type Writer interface {
    // Write submits one batch. Blocks while Outstanding batches are in
    // flight, or until ctx ends. done may be nil.
    Write(ctx context.Context, batch []Write, done func(error)) error

    // Sync waits for outstanding writes and fsyncs every file.
    Sync() error

    // Close waits for outstanding writes, fsyncs, and closes the files.
    // Idempotent. Returns the first error recorded by any batch.
    Close() error
}

// ErrClosed reports a Write after Close or after a fatal batch error.
var ErrClosed = errors.New("iow: writer is closed")

// New returns the best Writer the platform offers. The factory asks each
// platform implementation in turn; the pool is the final fallback.
func New(files []*os.File, opts Options) (Writer, error)
```

The entire public surface is `Writer`, `Write`, `Options`, `Durability`, `ErrClosed`, and `New`.

## 4. Platform support matrix

| GOOS | Primitive | Filter/flags | Status |
|---|---|---|---|
| linux | `io_uring` | `IORING_SETUP_SQPOLL`, `IORING_OP_WRITE`, `IORING_OP_FSYNC` | Phase 3 |
| windows | IOCP | `FILE_FLAG_OVERLAPPED`, `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` | Phase 3 |
| freebsd | POSIX AIO | `aio_write` + `EVFILT_AIO` | Phase 3 (optional) |
| solaris, illumos | Event ports | `port_associate` + POSIX AIO | Post-1.0 |
| darwin | — | kqueue has no file-write filter | pool |
| openbsd, netbsd, dragonfly | — | no `EVFILT_AIO` | pool |
| aix, js/wasm, plan9 | — | — | pool |

The pool is the only implementation on darwin, openbsd, netbsd, dragonfly, aix, js/wasm, and plan9. That is not a gap; it is the design's correctness guarantee. Every platform that can do better does; every platform that cannot still works.

### Why darwin is in the pool

macOS's `kqueue` man page states plainly for `EVFILT_AIO`: *"This filter is currently unsupported."* The history is that Apple ported kqueue from FreeBSD but did not connect it to the AIO subsystem when AIO arrived, and never reconnected them. There is no kernel primitive that lets an unprivileged process submit an asynchronous write to a regular file on macOS and be notified by kqueue when it completes. `aio_write` exists but cannot be wired to `EVFILT_AIO`. The pool is not a fallback for macOS; it is the correct answer.

This matters for the design: the plan does not ship a `kaio_darwin.go` and lie to the reader about what it does. It says so in a comment and moves on.

## 5. The pool — the reference implementation

Every platform has this. Every specialization is tested against it. It is the smallest correct answer to "write batches, in order, with bounded concurrency."

```go
type pool struct {
    files     []*os.File
    syncFiles []*os.File
    durable   bool

    jobs    chan *job
    workers int

    mu        sync.Mutex
    cond      *sync.Cond
    submitted uint64
    delivered uint64
    pending   map[uint64]completion
    err       error

    closeOnce sync.Once
    closing   chan struct{}
    wg        sync.WaitGroup
}

type job struct {
    seq    uint64
    writes []Write
    done   func(error)
}

type completion struct {
    err  error
    done func(error)
}

func newPool(files []*os.File, opts Options) *pool {
    workers := opts.Workers
    if workers <= 0 {
        workers = runtime.GOMAXPROCS(0)
    }
    outstanding := opts.Outstanding
    if outstanding <= 0 {
        outstanding = workers * 4
    }
    p := &pool{
        files:     files,
        syncFiles: opts.SyncFiles,
        durable:   opts.Durability == DataSync,
        jobs:      make(chan *job, outstanding),
        workers:   workers,
        pending:   make(map[uint64]completion, outstanding),
        closing:   make(chan struct{}),
    }
    p.cond = sync.NewCond(&p.mu)
    for range workers {
        p.wg.Add(1)
        go p.run()
    }
    return p
}
```

### Worker loop

```go
func (p *pool) run() {
    defer p.wg.Done()
    var ready []completion // reused across jobs; no allocation after warmup
    for {
        select {
        case j := <-p.jobs:
            ready = p.enqueue(j.seq, p.execute(j), j.done, ready[:0])
            for _, c := range ready {
                if c.done != nil {
                    c.done(c.err)
                }
            }
        case <-p.closing:
            return
        }
    }
}

func (p *pool) execute(j *job) error {
    for _, w := range j.writes {
        if _, err := w.F.WriteAt(w.Buf, w.Off); err != nil {
            p.setError(err)
            return err
        }
    }
    if !p.durable {
        return nil
    }
    for _, f := range p.syncTargets(j.writes) {
        if err := syncData(f); err != nil {
            p.setError(err)
            return err
        }
    }
    return nil
}
```

### Ordered delivery

```go
// enqueue records a completed job and appends every completion that is now
// ready, in submission order, to buf. Callers fire the returned callbacks
// outside p.mu.
func (p *pool) enqueue(seq uint64, err error, done func(error), buf []completion) []completion {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.pending[seq] = completion{err: err, done: done}
    for {
        c, ok := p.pending[p.delivered]
        if !ok {
            return buf
        }
        delete(p.pending, p.delivered)
        p.delivered++
        buf = append(buf, c)
    }
}
```

The callbacks fire outside `p.mu`, which removes the lock-inversion hazard entirely. The WAL's `done` acquires `w.mu` freely.

### Submission, Sync, Close

```go
func (p *pool) Write(ctx context.Context, batch []Write, done func(error)) error {
    p.mu.Lock()
    if p.err != nil || isClosed(p.closing) {
        err := p.closedErrorLocked()
        seq := p.submitted
        p.submitted++
        p.pending[seq] = completion{err: err, done: done}
        p.mu.Unlock()
        if done != nil {
            done(err)
        }
        return err
    }
    seq := p.submitted
    p.submitted++
    p.mu.Unlock()

    j := &job{seq: seq, writes: batch, done: done}
    select {
    case p.jobs <- j:
        return nil
    case <-p.closing:
        p.finishInline(seq, p.closedError(), done)
        return p.closedError()
    case <-ctx.Done():
        p.finishInline(seq, ctx.Err(), done)
        return ctx.Err()
    }
}

func (p *pool) Sync() error {
    p.mu.Lock()
    for p.submitted > p.delivered && p.err == nil {
        p.cond.Wait()
    }
    err := p.err
    p.mu.Unlock()
    if err != nil {
        return err
    }
    for _, f := range p.files {
        if err := syncData(f); err != nil {
            return err
        }
    }
    return nil
}

func (p *pool) Close() error {
    p.closeOnce.Do(func() {
        p.mu.Lock()
        close(p.closing)
        for p.submitted > p.delivered {
            p.cond.Wait()
        }
        err := p.err
        p.mu.Unlock()
        p.wg.Wait()
        if err == nil {
            for _, f := range p.files {
                if serr := syncData(f); serr != nil && err == nil {
                    err = serr
                }
            }
        }
        for _, f := range p.files {
            if cerr := f.Close(); cerr != nil && err == nil {
                err = cerr
            }
        }
        p.mu.Lock()
        p.err = err
        p.mu.Unlock()
    })
    p.mu.Lock()
    defer p.mu.Unlock()
    return p.err
}
```

The pool is roughly 240 lines including comments.

## 6. Linux: io_uring

The kernel primitive is `io_uring`, which submits and completes via two shared ring buffers rather than syscalls per operation. The submission queue (SQ) holds `IORING_OP_WRITE` and `IORING_OP_FSYNC` entries; the completion queue (CQ) reports `(user_data, res)` pairs.

Two flags matter:

- `IORING_SETUP_SQPOLL` creates a kernel thread that polls the submission queue, eliminating the `io_uring_enter` syscall entirely when the kernel thread is awake. This is the difference between "fast" and "no syscalls on the submit path."
- `IORING_FSYNC_DATASYNC` on `IORING_OP_FSYNC` maps to `fdatasync` rather than `fsync`, matching the engine's current behavior.

```go
// internal/iow/ring_linux.go
//go:build linux

type ring struct {
    fd      int
    sq      *sqRing
    cq      *cqRing
    files   []*os.File
    durable bool
    syncSet []*os.File

    mu      sync.Mutex
    seq     uint64
    pending map[uint64]*ringJob
    settled uint64
    err     error
}

type ringJob struct {
    remaining int
    err       error
    done      func(error)
}

func newRingWriter(files []*os.File, opts Options) (Writer, bool) {
    fd, err := ioUringSetup(ringEntries, ioUringSetupSQPoll)
    if err != nil {
        return nil, false
    }
    // mmap the SQ ring, CQ ring, and SQE array at their protocol offsets.
    r := &ring{fd: fd, files: files, durable: opts.Durability == DataSync,
               syncSet: opts.SyncFiles, pending: make(map[uint64]*ringJob)}
    r.mapRings()
    go r.reap()
    return r, true
}

func (r *ring) Write(ctx context.Context, batch []Write, done func(error)) error {
    r.mu.Lock()
    if r.err != nil {
        err := r.err
        r.mu.Unlock()
        if done != nil { done(err) }
        return err
    }
    seq := r.seq
    r.seq++
    r.pending[seq] = &ringJob{remaining: len(batch) + boolInt(r.durable), done: done}
    r.mu.Unlock()

    // One SQE per write; for DataSync, one linked fsync per file.
    for _, w := range batch {
        r.submitWrite(w.F.Fd(), w.Buf, w.Off, seq)
    }
    if r.durable {
        for _, f := range r.syncTargets(batch) {
            r.submitFsync(f.Fd(), seq)
        }
    }
    return nil
}
```

The reaper goroutine reads completions from the CQ and decrements each job's `remaining` counter. When it reaches zero, `done` fires with the job's error. Completions are reordered into `settled` order exactly as the pool does, but the ring's completion queue makes the reordering window smaller (typically a few microseconds) than the pool's (the pool's workers can finish batches microseconds apart even with no device contention).

Three things the plan should carry in the code:

- **Graceful fallback.** `io_uring_setup` returns `ENOSYS` on pre-5.1 kernels and `EPERM` under some seccomp profiles. On any error the factory falls back to the pool. There is no build tag that says "Linux means io_uring"; Linux means "try io_uring, else pool."
- **SQE array indices.** The kernel gives the submission queue a `ring_mask`; the SQE at index `tail & ring_mask` is the next free slot. The pool's `sync.Pool` for SQEs is unnecessary; the ring itself is the pool.
- **Registered buffers.** `IORING_REGISTER_BUFFERS` plus `IORING_OP_WRITE_FIXED` eliminates per-write pinning on the submit path. The plan leaves this as a follow-up; it does not change the interface.

## 7. Windows: IOCP with overlapped I/O

The kernel primitive is the I/O completion port. Files are opened with `FILE_FLAG_OVERLAPPED`; each handle is associated with a completion port via `CreateIoCompletionPort`; writes go out via `WriteFile` with an `OVERLAPPED` structure; completions arrive via `GetQueuedCompletionStatusEx` on a pool of threads.

Go 1.25 added support for overlapped handles passed to `os.NewFile`: the runtime associates them with its own IOCP on the first I/O method call, and `File.Fd` disassociates them again before returning the handle. That is the enabling change. Without it, a handle can only be associated with one IOCP at a time, and the pool would deadlock on the runtime's port.

The critical optimization on non-socket handles is `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`:

```go
// internal/iow/iocp_windows.go
//go:build windows

func newIOCPWriter(files []*os.File, opts Options) (Writer, bool) {
    port, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 0)
    if err != nil {
        return nil, false
    }
    for _, f := range files {
        if _, err := windows.CreateIoCompletionPort(windows.Handle(f.Fd()), port, 0, 0); err != nil {
            windows.CloseHandle(port)
            return nil, false
        }
        // Non-socket handles can use SetFileCompletionNotificationModes
        // without problems.
        if err := windows.SetFileCompletionNotificationModes(
            windows.Handle(f.Fd()),
            windows.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS|windows.FILE_SKIP_SET_EVENT_ON_HANDLE,
        ); err != nil {
            windows.CloseHandle(port)
            return nil, false
        }
    }
    w := &iocp{port: port, files: files, durable: opts.Durability == DataSync}
    w.start(opts.Workers)
    return w, true
}
```

`SetFileCompletionNotificationModes` with `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` is safe to call on non-socket handles unconditionally; the Go runtime already does this in `internal/poll`. The flag suppresses the completion-packet queue when the I/O finishes synchronously, which is the common case for page-cache writes. Only genuinely pending operations reach `GetQueuedCompletionStatusEx`.

The worker side is the standard IOCP pattern:

```go
func (w *iocp) run() {
    var entries [64]windows.OverlappedEntry
    for {
        n, err := windows.GetQueuedCompletionStatusEx(w.port, entries[:], windows.INFINITE, false)
        if err != nil {
            w.setError(err)
            return
        }
        for i := uint32(0); i < n; i++ {
            e := entries[i]
            job := overlappedToJob(e.Overlapped)
            w.complete(job, e.BytesTransferred, e.Internal)
        }
    }
}
```

Two details the Go runtime learned and the plan should carry:

- **High-resolution timers.** `GetQueuedCompletionStatusEx` has a ~16 ms timeout resolution. When a caller needs a sub-millisecond timeout, associate a wait completion packet with the port and signal it from a high-resolution timer. The plan does not need this today — `Write`'s only timeout is `ctx` and the caller supplies it — but a junior engineer reading the plan should know the escape hatch exists.
- **Cancellation.** `CancelIoEx(handle, overlapped)` is the cancellation mechanism. The pool's `ctx` path maps to it: on context cancel, call `CancelIoEx` and let the completion arrive with `ERROR_OPERATION_ABORTED`, which the job records as `ctx.Err()`.

On Windows 8 and later, `GetQueuedCompletionStatusEx` is always available; Go 1.11 dropped support for earlier versions, so the plan assumes it.

## 8. FreeBSD: POSIX AIO via EVFILT_AIO

FreeBSD's kqueue supports `EVFILT_AIO`, which fires when an `aio_read` or `aio_write` submitted with `SIGEV_KEVENT` completes. The submission carries the kqueue descriptor and the completion carries an `EVFILT_AIO` event with the `aiocb` pointer in `ident`.

The submission shape is unusual: unlike every other kqueue event source, the AIO operation is submitted to the kernel directly via `aio_write`, and the kqueue descriptor is carried inside the `aiocb`'s `sigevent` structure:

```c
struct sigevent sev = {0};
sev.sigev_notify = SIGEV_KEVENT;
sev.sigev_notify_kqueue = kq_fd;
sev.sigev_value.sival_ptr = job;
```

The plan's FreeBSD implementation is a Phase 3 optional. If it ships, it uses `golang.org/x/sys/unix`'s `AioWrite` and `Kevent`; if it does not, FreeBSD falls back to the pool like macOS. The interface does not change either way.

## 9. Solaris and illumos: event ports

Solaris's `port_associate` and `port_getn` are the completion model; POSIX AIO (`aio_write` with `SIGEV_PORT`) is the submission mechanism. This is the same shape as FreeBSD's EVFILT_AIO with different names. The plan does not ship a `ports_solaris.go`; Solaris and illumos fall back to the pool. The pattern is listed in the support matrix so a future contributor knows where to look.

## 10. The unified WAL

```go
type wal struct {
    writer iow.Writer

    mu             sync.Mutex
    cond           *sync.Cond
    err            error
    stagedBytes    int64
    pushedBytes    int64
    publishedBytes int64

    buf      []byte
    pending  []pendingEntry
    readyBuf []byte
    readyPending []pendingEntry
    wake     chan struct{}
    stop     chan struct{}
    done     chan struct{}
    stopOnce sync.Once
}
```

`pushBatch` appends to `buf`/`pending` under `w.mu`, returns `seq = pushedBytes`, and blocks the producer on `cond.Wait` while `stagedBytes >= 2 * MaxFlushBytes`. That is the async backpressure; the Writer's `Outstanding` bound is a second, smaller bound behind it.

The flusher is one goroutine regardless of which Writer is in use:

```go
func (w *wal) flush() {
    w.mu.Lock()
    if len(w.buf) == 0 || w.err != nil {
        w.mu.Unlock()
        return
    }
    data := w.buf
    entries := w.pending
    w.buf, w.pending = w.readyBuf[:0], w.readyPending[:0]
    w.stagedBytes = 0
    w.mu.Unlock()

    total := int64(len(data))
    _ = w.writer.Write(w.flushCtx, w.writerBatch(data, entries), func(err error) {
        w.mu.Lock()
        defer w.mu.Unlock()
        if err != nil {
            w.err = err
        } else {
            w.publishedBytes += total
        }
        w.cond.Broadcast()
    })

    w.mu.Lock()
    w.readyBuf, w.readyPending = data[:0], entries[:0]
    w.mu.Unlock()
}
```

`w.publishedBytes` advances unconditionally on success because the ordered completion contract from `iow.Writer` guarantees that the callback for batch *N* fires only after batch *N-1*'s callback fired. That is the one-line invariant the whole design buys.

## 11. The sink, decomposed

```go
// sink renders a WAL batch into the flat write list iow.Writer consumes.
type sink struct {
    log, idx *os.File
}

func (s *sink) writes(b sinkBatch) []iow.Write {
    out := make([]iow.Write, 0, len(b.Records)+1)
    for _, r := range b.Records {
        out = append(out, iow.Write{F: s.log, Buf: r.P, Off: r.Off})
    }
    if !b.Index.empty() {
        out = append(out, iow.Write{F: s.idx, Buf: encodeIndex(b.Index), Off: b.Index.Start})
    }
    return out
}
```

The `ownsIndex` branch on `wal` disappears. The pool dispatches to the correct file because `iow.Write` carries it.

## 12. What this deletes

| File | Fate |
|---|---|
| `engine/sink.go` | Replaced by a 15-line adapter in `wal.go`. |
| `engine/sink_sync.go` | Becomes `iow/pool.go`. |
| `engine/sink_sync_linux.go` | Ring path moves to `iow/ring_linux.go`. |
| `engine/sink_sync_other.go` | Deleted. |
| `engine/uring_linux.go` | Moves to `iow/ring_linux.go`. |
| `engine/uring_other.go` | Replaced by `iow/ring_other.go`. |
| `engine/uring.go` | Merged into `iow/ring.go`. |
| `engine/wal_ring.go` | Merged into `engine/wal.go`. |
| `engine/wal_syscall.go` | Merged into `engine/wal.go`. |
| `engine/sync_*.go` | Move to `iow/sync_*.go`. |

Net: eight files removed, eight added (`iow/writer.go`, `iow/pool.go`, `iow/ring.go`, `iow/ring_linux.go`, `iow/ring_other.go`, `iow/iocp_windows.go`, `iow/iocp_other.go`, `iow/sync_*.go`). The `wal` package shrinks by half.

---

## 13. Phase plan

Three phases. Each ends with a green tree and a commit. Test, vet, and fix run on the changed packages only. A bench runs before and after any phase that changes runtime behavior.

### Phase 1 — add the `iow` package

**Deliverable.** A new package with no callers, fully tested. The pool is the only implementation; the platform factories are stubs that return `false`.

**Files added**

- `internal/iow/writer.go` — the interface, `Options`, `Durability`, `ErrClosed`, `New`.
- `internal/iow/pool.go` — the reference pool.
- `internal/iow/writer_test.go`, `internal/iow/pool_test.go`.
- `internal/iow/sync_linux.go`, `sync_darwin.go`, `sync_windows.go`, `sync_other.go` — the fsync variants, moved verbatim from `engine`.
- `internal/iow/platform_other.go` — `newPlatformWriter` returns `false` on every platform.

**Tests to write**

- **Ordered delivery.** Submit 1000 batches from 4 goroutines through a fake `*os.File` shim that delays later batches longer than earlier ones; assert `done` fires in `seq` order.
- **Bounded submission.** Fill the pool with a slow fake; assert the next `Write` blocks; cancel its context and assert it returns `ctx.Err()` and `done` fired with `ctx.Err()`.
- **Failure propagation.** Fake fails batch *K*; assert batches <*K* delivered success, batch *K* delivered error, every later `Write` returns the same error immediately, and every in-flight batch's `done` fires with the error.
- **Close during flight.** Block on a slow batch, call `Close` in another goroutine, assert `Close` blocks until the batch completes.
- **Idempotent Close.** Call `Close` twice; second returns the same error.
- **DataSync boundary.** Inject `syncData` via a package-level `var syncData = syncDataDefault`; assert every file in a batch is fsynced once, and that `SyncFiles` restricts the set.
- **Short write.** Fake `WriteAt` returns `(0, err)`; assert the batch error is recorded and the pool enters the failed state.
- **No lock re-entry.** In a `done` callback, submit a new `Write`; assert it completes without deadlock.

**Verification**

```
go test ./internal/iow/...
go vet  ./internal/iow/...
go fix  ./internal/iow/...
```

**No bench.** No callers, no runtime change.

**Commit.** `[feat] iow: add the Writer interface and the portable pool`

---

### Phase 2 — rewrite `sink` and switch the syscall WAL to the pool

This phase fixes the tail. It touches the live write path, so it gets a bench before and after.

**Baseline bench (before touching code)**

```
mikyu bench run --mode produce --dir /tmp/mikyu-before --bytes 4GiB \
  --workers 8 --batch 64 --durability async --out /tmp/bench/before-async
mikyu bench run --mode produce --dir /tmp/mikyu-before-sync --bytes 4GiB \
  --workers 8 --batch 64 --durability sync  --out /tmp/bench/before-sync
```

Copy the two run directories somewhere they cannot be clobbered.

**Files changed**

- `internal/engine/sink.go` — reduce to a pure `iow.Write` adapter. Delete the `sink` interface.
- `internal/engine/wal_syscall.go` — replace `newSink` with `iow.New`; replace `w.sink.Write` with `w.writer.Write`. Delete the `asyncChan`/`drainAndFlushAsync`/`writeRun` paths; the pool owns the channel now.
- `internal/engine/wal.go` — remove the `if w.ring != nil` branches that reach the syscall sink. The ring path stays until Phase 3.

**Files deleted**

- `engine/sink_sync_linux.go`, `engine/sink_sync_other.go`, `engine/sink_sync.go`.

**Work item order**

1. Rewrite `sink.go` to expose `writes(batch) []iow.Write`.
2. Replace `newSyscallWAL`'s construction with `iow.New`:
   ```go
   writer, err := iow.New(
       []*os.File{seg.logFile, seg.idxFile},
       iow.Options{
           Workers:     runtime.GOMAXPROCS(0),
           Outstanding: 0,
           Durability:  durabilityFromConfig(cfg),
           SyncFiles:   []*os.File{seg.logFile},
       },
   )
   ```
   `SyncFiles` preserves the "log is the durable file" rule.
3. Delete the async channel and the double-buffer swap from `wal_syscall.go`. Producers stage into `w.buf`/`w.pending`; the flusher submits to `w.writer`.

**Verification**

```
go test ./internal/engine/...
go test ./internal/node/...
go vet  ./internal/engine/... ./internal/node/...
go fix  ./internal/engine/... ./internal/node/...
```

`TestSingleWriter`, `TestWALStopFlushesPending`, `TestWALConcurrentPush`, and `TestWALByteBackpressure` still pass because the pool upholds the same invariants.

**After bench**

```
mikyu bench run --mode produce --dir /tmp/mikyu-after --bytes 4GiB \
  --workers 8 --batch 64 --durability async --out /tmp/bench/after-async
mikyu bench run --mode produce --dir /tmp/mikyu-after-sync --bytes 4GiB \
  --workers 8 --batch 64 --durability sync  --out /tmp/bench/after-sync
```

**Compare**

```
mikyu bench compare /tmp/bench/before-async/<id> /tmp/bench/after-async/<id>
mikyu bench compare /tmp/bench/before-sync/<id>  /tmp/bench/after-sync/<id>
```

**Success criteria**

- `throughput` no longer hits exactly zero in any one-second bucket under `--durability async`.
- `latency_uncorrected.p99` drops; `max_ns` drops below a few hundred milliseconds.
- `p50` moves from `<1ns` to a small non-zero value. This is expected, not a regression.
- Sync numbers are unchanged.

If the numbers do not move, do not proceed to Phase 3. Investigate first.

**Commit.** `[feat] engine: route the WAL through iow.Writer`

---

### Phase 3 — port the platform specializations

**Baseline bench.** Same commands as Phase 2, run at the end of Phase 2 so the "before" is the pool-only implementation.

**Files added**

- `internal/iow/ring_linux.go` — the io_uring implementation with `IORING_SETUP_SQPOLL`.
- `internal/iow/ring_other.go` — `newPlatformWriter` returns `false`.
- `internal/iow/iocp_windows.go` — the IOCP implementation with `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`.
- `internal/iow/iocp_other.go` — `newPlatformWriter` returns `false`.
- `internal/iow/kaio_freebsd.go` (optional) — POSIX AIO via `EVFILT_AIO`.
- `internal/iow/kaio_other.go` — `newPlatformWriter` returns `false`.

**Files changed**

- `internal/engine/wal.go` — one struct, one implementation. Delete `wal_syscall.go` and `wal_ring.go`. The `pushBatch`/`pushAsyncBatch`/`UpdateConfig`/`stop` bodies move into `wal.go` and lose their ring branches.
- `internal/iow/writer.go` — `New` calls each platform factory in order.

**Files deleted**

- `engine/uring.go`, `engine/uring_linux.go`, `engine/uring_other.go`, `engine/uring_linux_test.go`.
- `engine/wal_syscall.go`, `engine/wal_ring.go`.
- `engine/sink_sync_linux.go`, `engine/sink_sync_other.go`, `engine/sink_sync.go`, `engine/sink.go`.
- `engine/sync_darwin.go`, `engine/sync_fdatasync.go`, `engine/sync_windows.go`, `engine/sync_dir*.go`.

**Verification**

```
go test ./internal/iow/... ./internal/engine/...
go test ./internal/node/... ./internal/daemon/...
go vet  ./internal/iow/... ./internal/engine/...
go fix  ./internal/iow/... ./internal/engine/...
```

The `iow` package's parity tests run the pool and the ring on Linux, the pool and IOCP on Windows, the pool and KAIO on FreeBSD, and the pool everywhere. On platforms without a specialization, only the pool runs; that is the parity guarantee.

**After bench**

Same commands, third output directory.

**Compare**

```
mikyu bench compare /tmp/bench/after-async/<id> /tmp/bench/final-async/<id>
mikyu bench compare /tmp/bench/after-sync/<id>  /tmp/bench/final-sync/<id>
```

**Success criteria**

- Ring and IOCP hosts match or beat the pool on both async and sync.
- No regression on non-specialized platforms.
- `go test ./...` passes at HEAD with the deleted files gone.

**Commit.** `[feat] iow, engine: add io_uring and IOCP, collapse the WAL`

---

## 14. Honest caveats

**The OS dirty-page throttle is not eliminated.** The pool keeps the device queue full when the throttle lifts; it does not prevent the throttle. The throughput column will show a plateau at the sustained rate rather than a spike to zero.

**The pool's reorder buffer means a slow batch blocks fast batches behind it.** Under the throttle this is invisible. At steady state it is invisible because batches complete within microseconds of each other. In-order delivery is the correct choice because it makes the contract a property of the type rather than a burden on every caller.

**`Workers > 1` pays off under `Buffered`.** Under `DataSync` the filesystem serializes fsyncs regardless. The plan preserves the current default of one sync per batch.

**`syncData` is platform-specific.** `F_FULLFSYNC` on macOS is roughly ten times the cost of `fdatasync` on Linux. The `Durability` enum makes this a visible choice at construction.

**macOS has no kernel AIO.** This is not a gap in the plan; it is a fact about the platform. The pool is the correct answer.

**The ring is not the ring of the future.** What ships in Phase 3 is `IORING_OP_WRITE` + `IORING_OP_FSYNC` with `IORING_SETUP_SQPOLL`. What does not ship: `IORING_REGISTER_BUFFERS` with `IORING_OP_WRITE_FIXED`, `IORING_SETUP_IOPOLL` for NVMe, and the `io-wq` worker-pool tuning. All three are measured improvements; none changes the interface. They are follow-ups.

**The IOCP implementation is not the IOCP implementation of the future either.** What ships: `CreateIoCompletionPort`, `GetQueuedCompletionStatusEx`, `FILE_FLAG_OVERLAPPED`, `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`, `CancelIoEx`. What does not ship: `SetFileCompletionNotificationModes` on socket handles (unsafe with non-IFS LSPs, and unnecessary here because the files are regular files), `NtAssociateWaitCompletionPacket` for sub-millisecond timer resolution (not needed until `Write` gains a deadline), and `GetOverlappedResultEx` for alertable waits. Each is a follow-up that slots in without changing the interface.

**`iow.Pool` is the reference implementation.** Its tests define the contract every specialization must satisfy. Keep it small; resist the temptation to specialize it.