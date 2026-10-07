//go:build windows

package iow

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// iocpPollTimeout is the reaper's blocking timeout; a wake packet ends the wait.
const iocpPollTimeout = uint32(windows.INFINITE)

// sharedPort opens the process-wide completion port on first use; nil means this host declines overlapped I/O.
var sharedPort = sync.OnceValue(newPort)

// newIOCPWriter returns a Writer backed by overlapped I/O, or false when the host has no usable completion port.
func newIOCPWriter(files []*os.File, opts Options) (Writer, bool) {
	if os.Getenv("MIKYU_DISABLE_IOCP") != "" {
		return nil, false
	}
	p := sharedPort()
	if p == nil {
		return nil, false
	}
	return p.newWriter(files, opts), true
}

// iocpPort is one completion port shared by every Writer, reaped by a single goroutine.
type iocpPort struct {
	handle windows.Handle
	mu     sync.Mutex
	// inflight roots every outstanding OVERLAPPED: the kernel writes into it at
	// completion, so it must stay reachable from Go memory until then.
	inflight map[*iocpOp]struct{}
	writers  map[uintptr]*iocpWriter
	keys     uintptr
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

// newPort opens the process-wide port; nil when the host refuses one.
func newPort() *iocpPort {
	if err := windows.LoadSetFileCompletionNotificationModes(); err != nil {
		return nil
	}
	h, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 0)
	if err != nil {
		return nil
	}
	p := &iocpPort{
		handle:   h,
		inflight: make(map[*iocpOp]struct{}),
		writers:  make(map[uintptr]*iocpWriter),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go p.loop()
	return p
}

// track roots an operation until its completion arrives.
func (p *iocpPort) track(op *iocpOp) {
	p.mu.Lock()
	p.inflight[op] = struct{}{}
	p.mu.Unlock()
}

// release unroots a completed operation; the kernel is done with its memory.
func (p *iocpPort) release(op *iocpOp) {
	p.mu.Lock()
	delete(p.inflight, op)
	p.mu.Unlock()
}

// register allocates a completion key for one writer.
func (p *iocpPort) register(w *iocpWriter) uintptr {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys++
	key := p.keys
	p.writers[key] = w
	return key
}

// unregister drops a writer from the completion-key table.
func (p *iocpPort) unregister(key uintptr) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.writers, key)
}

// loop reaps completions and hands each one to the batch that issued it.
func (p *iocpPort) loop() {
	defer close(p.done)
	for {
		var qty uint32
		var key uintptr
		var ov *windows.Overlapped
		err := windows.GetQueuedCompletionStatus(p.handle, &qty, &key, &ov, iocpPollTimeout)
		if err != nil {
			if errors.Is(err, windows.ERROR_ABANDONED_WAIT_0) {
				return
			}
			select {
			case <-p.stop:
				return
			default:
			}
			continue
		}
		if ov == nil {
			continue
		}
		op := (*iocpOp)(unsafe.Pointer(ov))
		batch := op.batch
		p.release(op)
		batch.opDone(overlappedError(uintptr(op.overlapped.Internal), 0, -1))
	}
}

// close stops the reaper and closes the port; the process-wide port is never closed in production.
func (p *iocpPort) close() {
	p.once.Do(func() {
		close(p.stop)
		_ = windows.PostQueuedCompletionStatus(p.handle, 0, 0, nil)
		<-p.done
		_ = windows.CloseHandle(p.handle)
	})
}

// overlappedError turns an NTSTATUS and byte count into an error.
func overlappedError(internal uintptr, transferred uint32, want int) error {
	if status := windows.NTStatus(internal); status != 0 {
		return status.Errno()
	}
	if want >= 0 && int(transferred) != want {
		return io.ErrShortWrite
	}
	return nil
}

// iocpOp is one in-flight overlapped operation; overlapped is first so a completion maps back by address.
type iocpOp struct {
	overlapped windows.Overlapped
	batch      *iocpBatch
	length     int
}

// iocpBatch tracks one submitted batch until every operation it issued completes.
type iocpBatch struct {
	writer *iocpWriter
	seq    uint64
	syncs  []*os.File
	done   func(error)

	mu      sync.Mutex
	writes  int
	err     error
	settled bool
}

// opDone records one completed operation.
func (b *iocpBatch) opDone(err error) { b.advance(err, 1) }

// skip records n operations the batch will never submit.
func (b *iocpBatch) skip(err error, n int) { b.advance(err, n) }

// advance moves the batch toward completion and settles it once nothing is outstanding.
func (b *iocpBatch) advance(err error, skipped int) {
	b.mu.Lock()
	if err != nil && b.err == nil {
		b.err = err
	}
	b.writes -= skipped
	if b.writes > 0 {
		b.mu.Unlock()
		return
	}
	outcome, syncs := b.err, b.syncs
	b.mu.Unlock()

	if outcome == nil && len(syncs) > 0 {
		for _, f := range syncs {
			if serr := windows.FlushFileBuffers(windows.Handle(f.Fd())); serr != nil {
				outcome = serr
				break
			}
		}
	}
	b.settle(outcome)
}

// settle releases the batch's slot and reports its outcome exactly once.
func (b *iocpBatch) settle(err error) {
	b.mu.Lock()
	if b.settled {
		b.mu.Unlock()
		return
	}
	b.settled = true
	b.mu.Unlock()

	<-b.writer.slots
	if err != nil {
		b.writer.setError(err)
	}
	b.writer.order.settle(b.seq, err, b.done)
}

// iocpWriter writes batches through overlapped I/O completed by a shared port.
type iocpWriter struct {
	port    *iocpPort
	key     uintptr
	files   []*os.File
	syncSet []*os.File
	durable bool
	order   *order
	slots   chan struct{}

	mu       sync.Mutex
	attached map[windows.Handle]bool
	closed   bool
	err      error
}

// newWriter registers one writer with the shared port.
func (p *iocpPort) newWriter(files []*os.File, opts Options) *iocpWriter {
	outstanding := opts.Outstanding
	if outstanding <= 0 {
		outstanding = opts.Workers * 4
	}
	if outstanding <= 0 {
		outstanding = 1
	}
	w := &iocpWriter{
		port:     p,
		files:    files,
		syncSet:  opts.SyncFiles,
		durable:  opts.Durability == DataSync,
		order:    newOrder(),
		slots:    make(chan struct{}, outstanding),
		attached: make(map[windows.Handle]bool),
	}
	w.key = p.register(w)
	return w
}

// Write submits one batch's buffers and, under DataSync, flushes the sync targets once they land.
func (w *iocpWriter) Write(ctx context.Context, batch []Write, done func(error)) error {
	if err := w.currentError(); err != nil {
		w.order.settle(w.order.reserve(), err, done)
		return err
	}
	if err := ctx.Err(); err != nil {
		w.order.settle(w.order.reserve(), err, done)
		return err
	}
	select {
	case w.slots <- struct{}{}:
	default:
		select {
		case w.slots <- struct{}{}:
		case <-ctx.Done():
			w.order.settle(w.order.reserve(), ctx.Err(), done)
			return ctx.Err()
		}
	}

	b := &iocpBatch{writer: w, seq: w.order.reserve(), done: done, writes: len(batch)}
	if w.durable {
		b.syncs = w.syncTargets(batch)
	}
	if len(batch) == 0 {
		b.opDone(nil)
		return nil
	}
	for i, bw := range batch {
		if err := w.issue(b, bw); err != nil {
			b.skip(err, len(batch)-i-1)
			return nil
		}
	}
	return nil
}

// Sync waits for outstanding batches, then flushes every owned file.
func (w *iocpWriter) Sync() error {
	if err := w.currentError(); err != nil {
		return err
	}
	w.order.waitDrained()
	for _, f := range w.files {
		if err := windows.FlushFileBuffers(windows.Handle(f.Fd())); err != nil {
			w.setError(err)
			return err
		}
	}
	return nil
}

// Close waits for outstanding batches and deregisters the writer; the files stay the caller's.
func (w *iocpWriter) Close() error {
	w.order.waitDrained()
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.port.unregister(w.key)
	return nil
}

// issue submits one buffer, completing the batch's operation inline when the write lands synchronously.
func (w *iocpWriter) issue(b *iocpBatch, bw Write) error {
	h, ok := w.attach(bw.F)
	if !ok {
		_, err := bw.F.WriteAt(bw.Buf, bw.Off)
		b.opDone(err)
		return err
	}
	op := &iocpOp{batch: b, length: len(bw.Buf)}
	op.overlapped.Offset = uint32(bw.Off)
	op.overlapped.OffsetHigh = uint32(bw.Off >> 32)
	w.port.track(op)
	var written uint32
	err := windows.WriteFile(h, bw.Buf, &written, &op.overlapped)
	switch {
	case err == nil:
		// A synchronous success is the one case that posts no completion packet
		// (FILE_SKIP_COMPLETION_PORT_ON_SUCCESS), so the kernel is done with the
		// overlapped and it can be unrooted here.
		err = overlappedError(0, written, len(bw.Buf))
		w.port.release(op)
		b.opDone(err)
		return err
	case errors.Is(err, windows.ERROR_IO_PENDING):
		return nil
	default:
		// A synchronous failure still queues a completion packet, so the
		// overlapped stays rooted until the reaper sees it.
		b.opDone(err)
		return err
	}
}

// attach associates a file handle with the port once and reports whether overlapped I/O works on it.
func (w *iocpWriter) attach(f *os.File) (windows.Handle, bool) {
	h := windows.Handle(f.Fd())
	w.mu.Lock()
	defer w.mu.Unlock()
	if ok, seen := w.attached[h]; seen {
		return h, ok
	}
	_, err := windows.CreateIoCompletionPort(h, w.port.handle, w.key, 0)
	if err == nil {
		err = windows.SetFileCompletionNotificationModes(h, windows.FILE_SKIP_COMPLETION_PORT_ON_SUCCESS|windows.FILE_SKIP_SET_EVENT_ON_HANDLE)
	}
	w.attached[h] = err == nil
	return h, err == nil
}

// syncTargets lists the files a batch must flush, falling back to the files written by the batch.
func (w *iocpWriter) syncTargets(batch []Write) []*os.File {
	if len(w.syncSet) > 0 {
		return w.syncSet
	}
	targets := make([]*os.File, 0, len(batch))
	for _, b := range batch {
		if b.F != nil && !slices.Contains(targets, b.F) {
			targets = append(targets, b.F)
		}
	}
	return targets
}

// setError records the first fatal batch error.
func (w *iocpWriter) setError(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
}

// currentError reports the first fatal error, treating a closed writer as finished.
func (w *iocpWriter) currentError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return ErrClosed
	}
	return nil
}
