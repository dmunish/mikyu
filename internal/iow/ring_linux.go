//go:build linux

package iow

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	uringEntries    = 4096 // power of two; bounds outstanding SQEs
	uringSqeSize    = 64
	uringCqeSize    = 16
	uringCqBatch    = 256              // CQEs drained per reaper pass
	uringBrokenPoll = time.Millisecond // idle wait once the ring can no longer be entered

	uringOffSQRing = 0
	uringOffCQRing = 0x8000000
	uringOffSQEs   = 0x10000000

	sqeFlagLink  = 1 << 2 // IOSQE_IO_LINK
	sqeFlagDrain = 1 << 1 // IOSQE_IO_DRAIN

	uringOpWriteByte = 23 // IORING_OP_WRITE
	uringOpFsyncByte = 3  // IORING_OP_FSYNC
	uringOpNopByte   = 0  // IORING_OP_NOP

	uringFsyncDataSync  = 1
	uringEnterGetEvents = 1

	uringMinKernelMajor = 5
	uringMinKernelMinor = 13 // IORING_OP_WRITE appeared in 5.13

	uringSinkIDNone = ^uint32(0) // completion id that matches no registered handle
)

// rawSQE mirrors struct io_uring_sqe; the size assert in newRing pins the ABI.
type rawSQE struct {
	opcode        uint8
	flags         uint8
	ioprio        uint16
	fd            int32
	off           uint64
	addr          uint64
	length        uint32
	protocolFlags uint32
	userData      uint64
	bufIndex      uint16
	personality   uint16
	spliceFDIn    int32
	addr3         uint64
	pad           uint64
}

// rawCQE mirrors struct io_uring_cqe.
type rawCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

type uringSqOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	userAddr                                                        uint64
}

type uringCqOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	userAddr                                                        uint64
}

type uringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        uringSqOffsets
	cqOff        uringCqOffsets
}

type opKind uint8

const (
	opWrite opKind = iota
	opFsync
	opNop
)

// op is one ring submission; fields unused by Kind are ignored. Submit stamps the handle's id into Sink and the completion echoes Sink<<32 | Tag back in userData.
type op struct {
	Kind     opKind
	File     *os.File
	Buf      []byte
	Off      int64
	DataSync bool
	Link     bool
	Drain    bool
	Sink     uint32
	Tag      uint32
}

// ring is one io_uring instance shared by every Writer in the process.
type ring struct {
	fd      int
	entries uint32
	sqRing  []byte
	cqRing  []byte
	sqes    []byte

	sqHead  *uint32
	sqTail  *uint32
	sqMask  uint32
	sqArray *uint32
	cqHead  *uint32
	cqTail  *uint32
	cqMask  uint32
	cqBase  unsafe.Pointer

	gate ringGate

	mu      sync.Mutex
	handles []*ringHandle
	free    []uint32

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// sharedRing opens the process-wide ring on first use; nil means io_uring is unavailable.
var sharedRing = sync.OnceValue(newRing)

// newRingWriter returns a Writer backed by the shared ring, or false when io_uring is unavailable.
func newRingWriter(files []*os.File, opts Options) (Writer, bool) {
	r := sharedRing()
	if r == nil {
		return nil, false
	}
	return newRingWriterOn(r, files, opts), true
}

// newRingWriterOn binds one Writer to an existing ring, which the caller owns.
func newRingWriterOn(r *ring, files []*os.File, opts Options) *ringWriter {
	return &ringWriter{
		files:   files,
		syncSet: opts.SyncFiles,
		durable: opts.Durability == DataSync,
		order:   newOrder(),
		handle:  r.register(),
		slots:   make(chan struct{}, outstandingDepth(opts)),
	}
}

// newRing tries io_uring_setup; on any failure it returns nil and callers fall back to the pool.
func newRing() *ring {
	if os.Getenv("MIKYU_DISABLE_URING") != "" {
		return nil
	}
	if !uringKernelSupported() || unsafe.Sizeof(rawSQE{}) != uringSqeSize {
		return nil
	}
	var params uringParams
	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(uringEntries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil
	}
	r := &ring{
		fd:      int(fd),
		entries: params.sqEntries,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if err := r.mapRings(&params); err != nil {
		_ = unix.Close(int(fd))
		return nil
	}
	for i := uint32(0); i < r.entries; i++ {
		atomic.StoreUint32((*uint32)(unsafe.Add(unsafe.Pointer(r.sqArray), uintptr(i)*4)), i)
	}
	go r.loop()
	return r
}

// mapRings maps the shared SQ ring, CQ ring, and SQE array at their protocol offsets.
func (r *ring) mapRings(p *uringParams) error {
	sq, err := unix.Mmap(r.fd, uringOffSQRing, int(p.sqOff.array)+int(p.sqEntries)*4, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	cq, err := unix.Mmap(r.fd, uringOffCQRing, int(p.cqOff.cqes)+int(p.cqEntries)*uringCqeSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Munmap(sq)
		return err
	}
	sqes, err := unix.Mmap(r.fd, uringOffSQEs, int(p.sqEntries)*uringSqeSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Munmap(sq)
		_ = unix.Munmap(cq)
		return err
	}
	r.sqRing, r.cqRing, r.sqes = sq, cq, sqes
	r.sqHead = u32At(sq, p.sqOff.head)
	r.sqTail = u32At(sq, p.sqOff.tail)
	r.sqArray = u32At(sq, p.sqOff.array)
	r.sqMask = atomic.LoadUint32(u32At(sq, p.sqOff.ringMask))
	r.cqHead = u32At(cq, p.cqOff.head)
	r.cqTail = u32At(cq, p.cqOff.tail)
	r.cqMask = atomic.LoadUint32(u32At(cq, p.cqOff.ringMask))
	r.cqBase = unsafe.Add(unsafe.Pointer(&r.cqRing[0]), uintptr(p.cqOff.cqes))
	return nil
}

func u32At(buf []byte, off uint32) *uint32 {
	return (*uint32)(unsafe.Add(unsafe.Pointer(&buf[0]), uintptr(off)))
}

// uringKernelSupported reports whether the host kernel is new enough for the operations the ring submits.
func uringKernelSupported() bool {
	release, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return false
	}
	fields := strings.SplitN(strings.TrimSpace(string(release)), ".", 3)
	if len(fields) < 2 {
		return false
	}
	major, err := strconv.Atoi(fields[0])
	if err != nil {
		return false
	}
	minor, err := strconv.Atoi(fields[1])
	if err != nil {
		return false
	}
	return major > uringMinKernelMajor || (major == uringMinKernelMajor && minor >= uringMinKernelMinor)
}

// submit publishes ops to the ring. A failed enter retires the ring for good:
// published SQEs may still be unconsumed, and nothing may ever submit them again.
func (r *ring) submit(ops []op) error {
	if len(ops) == 0 {
		return nil
	}
	return r.gate.run(func() error { return r.publish(ops) })
}

// publish writes the SQEs and hands them to the kernel. The gate guarantees a
// single publisher, so a failed enter retires the ring before anyone else
// publishes.
func (r *ring) publish(ops []op) error {
	for i := 0; i < len(ops); {
		tail := atomic.LoadUint32(r.sqTail)
		head := atomic.LoadUint32(r.sqHead)
		free := int(r.entries - (tail - head))
		if free <= 0 {
			if err := r.enter(0, 1, uringEnterGetEvents); err != nil {
				return err
			}
			continue
		}
		n := min(free, len(ops)-i)
		for k := range n {
			slot := (tail + uint32(k)) & r.sqMask
			r.encodeSQE((*rawSQE)(unsafe.Pointer(&r.sqes[uintptr(slot)*uringSqeSize])), ops[i+k])
		}
		atomic.StoreUint32(r.sqTail, tail+uint32(n))
		if err := r.enter(uint32(n), 0, 0); err != nil {
			return err
		}
		i += n
	}
	return nil
}

func (r *ring) encodeSQE(sqe *rawSQE, o op) {
	*sqe = rawSQE{userData: uint64(o.Sink)<<32 | uint64(o.Tag)}
	switch o.Kind {
	case opWrite:
		sqe.opcode = uringOpWriteByte
		sqe.fd = int32(o.File.Fd())
		sqe.off = uint64(o.Off)
		sqe.addr = uint64(uintptr(unsafe.Pointer(unsafe.SliceData(o.Buf))))
		sqe.length = uint32(len(o.Buf))
	case opFsync:
		sqe.opcode = uringOpFsyncByte
		sqe.fd = int32(o.File.Fd())
		if o.DataSync {
			sqe.off = uringFsyncDataSync
		}
	case opNop:
		sqe.opcode = uringOpNopByte
	}
	if o.Link {
		sqe.flags |= sqeFlagLink
	}
	if o.Drain {
		sqe.flags |= sqeFlagDrain
	}
}

func (r *ring) enter(toSubmit, minComplete, flags uint32) error {
	for {
		_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
		if errno == 0 {
			return nil
		}
		if errno == unix.EINTR {
			continue
		}
		return errno
	}
}

// register allocates a sink id and returns the handle bound to it.
func (r *ring) register() *ringHandle {
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
	h := &ringHandle{ring: r, id: id, tags: newOutcomes()}
	r.handles[id] = h
	return h
}

// unregister clears the handle's slot and returns its id to the free list.
func (r *ring) unregister(h *ringHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if int(h.id) >= len(r.handles) || r.handles[h.id] != h {
		return
	}
	r.handles[h.id] = nil
	r.free = append(r.free, h.id)
}

// close closes the ring fd; in-flight completions are lost and every handle with an outstanding tag reports ErrClosed.
func (r *ring) close() error {
	r.stopOnce.Do(func() {
		close(r.stop)
		r.wakeReaper()
		_ = unix.Close(r.fd)
		<-r.done
		_ = unix.Munmap(r.sqRing)
		_ = unix.Munmap(r.cqRing)
		_ = unix.Munmap(r.sqes)
	})
	return nil
}

// wakeReaper submits a completion id that matches no handle so a reaper parked in enter returns and observes the stop signal.
func (r *ring) wakeReaper() {
	_ = r.submit([]op{{Kind: opNop, Sink: uringSinkIDNone}})
}

// loop reaps completions for the life of the ring. It reads the shared CQ ring
// without syscalls and only enters when idle and healthy, so a ring that can no
// longer be entered still delivers everything already in flight.
func (r *ring) loop() {
	defer close(r.done)
	cqes := make([]rawCQE, uringCqBatch)
	for {
		select {
		case <-r.stop:
			return
		default:
		}
		if n := r.drain(cqes); n > 0 {
			for i := range n {
				r.dispatch(cqes[i])
			}
			continue
		}
		if r.gate.isBroken() {
			if !sleepOrStop(r.stop, uringBrokenPoll) {
				return
			}
			continue
		}
		if err := r.enter(0, 1, uringEnterGetEvents); err != nil {
			if r.stopping() {
				return
			}
			r.gate.fail(err)
		}
	}
}

// drain copies up to one batch of ready CQEs out of the shared ring and advances
// the consumer head. It never blocks and needs no syscall.
func (r *ring) drain(dst []rawCQE) int {
	ready := atomic.LoadUint32(r.cqTail) - atomic.LoadUint32(r.cqHead)
	if ready == 0 {
		return 0
	}
	n := min(int(ready), len(dst), int(r.entries))
	head := atomic.LoadUint32(r.cqHead)
	for i := range n {
		dst[i] = *(*rawCQE)(unsafe.Add(r.cqBase, uintptr((head+uint32(i))&r.cqMask)*uringCqeSize))
	}
	atomic.StoreUint32(r.cqHead, head+uint32(n))
	return n
}

// sleepOrStop parks for d and reports whether the ring is still running.
func sleepOrStop(stop <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}

// stopping reports whether close has signalled the reaper.
func (r *ring) stopping() bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

// dispatch routes a completion to its handle; a completion whose sink id is no longer registered is dropped because no one is listening.
func (r *ring) dispatch(cqe rawCQE) {
	sinkID := uint32(cqe.userData >> 32)
	r.mu.Lock()
	var h *ringHandle
	if sinkID < uint32(len(r.handles)) {
		h = r.handles[sinkID]
	}
	r.mu.Unlock()
	if h != nil {
		h.complete(uint32(cqe.userData), int64(cqe.res))
	}
}

// ringHandle tracks one Writer's outstanding tags. A tag submitted with a nil callback is waited on through wait; a tag submitted with a callback is driven by the reaper.
type ringHandle struct {
	ring *ring
	id   uint32
	next atomic.Uint32
	tags *outcomes
}

// nextTag returns a fresh tag for the next submission.
func (h *ringHandle) nextTag() uint32 { return h.next.Add(1) - 1 }

// submit registers the tag and its completion callback before the ring takes the ops, rolls the registration back if submission fails, and never calls done itself: the caller owns completion exactly once.
func (h *ringHandle) submit(ops []op, done func(error)) error {
	if len(ops) == 0 {
		if h.tags.isClosed() {
			return ErrClosed
		}
		return nil
	}
	for i := range ops {
		ops[i].Sink = h.id
	}
	tag := ops[0].Tag
	if err := h.tags.register(tag, int32(len(ops)), done); err != nil {
		return err
	}
	if err := h.ring.submit(ops); err != nil {
		h.tags.rollback(tag, int32(len(ops)))
		return err
	}
	return nil
}

// wait blocks until every completion for tag has arrived, then reports the first error recorded for it.
func (h *ringHandle) wait(tag uint32) error { return h.tags.wait(tag) }

// complete is called by the reaper once per CQE; the tag's last completion fires its callback or wakes its waiter.
func (h *ringHandle) complete(tag uint32, res int64) { h.tags.complete(tag, res) }

// fsync submits a data-sync-only fsync and waits for it.
func (h *ringHandle) fsync(f *os.File) error {
	tag := h.nextTag()
	if err := h.submit([]op{{Kind: opFsync, File: f, DataSync: true, Tag: tag}}, nil); err != nil {
		return err
	}
	return h.wait(tag)
}

// close marks the handle closed, wakes every waiter with ErrClosed, and deregisters it from the ring.
func (h *ringHandle) close() error {
	if h.tags.close() {
		h.ring.unregister(h)
	}
	return nil
}

// ringWriter writes batches by submitting SQEs to a shared io_uring ring.
type ringWriter struct {
	files   []*os.File
	syncSet []*os.File
	durable bool

	ring   *ring
	handle *ringHandle
	order  *order
	slots  chan struct{}

	mu     sync.Mutex
	closed bool
	err    error
}

// Write submits one batch's buffers and the fsync it owes under DataSync.
func (w *ringWriter) Write(ctx context.Context, batch []Write, done func(error)) error {
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

	seq := w.order.reserve()
	tag := w.handle.nextTag()
	ops := make([]op, 0, len(batch)+2)
	for _, b := range batch {
		ops = append(ops, op{Kind: opWrite, File: b.F, Buf: b.Buf, Off: b.Off, Tag: tag})
	}
	if w.durable {
		for _, f := range w.syncTargets(batch) {
			ops = append(ops, op{Kind: opFsync, File: f, DataSync: true, Drain: true, Tag: tag})
		}
	}

	err := w.handle.submit(ops, func(err error) {
		<-w.slots
		if err != nil {
			w.setError(err)
		}
		w.order.settle(seq, err, done)
	})
	if err != nil {
		<-w.slots
		w.order.settle(seq, err, done)
		return err
	}
	if len(ops) == 0 {
		<-w.slots
		w.order.settle(seq, nil, done)
	}
	return nil
}

// Sync waits for outstanding batches, then fsyncs every file the writer owns.
func (w *ringWriter) Sync() error {
	if err := w.currentError(); err != nil {
		return err
	}
	w.order.waitDrained()
	for _, f := range w.files {
		if err := w.handle.fsync(f); err != nil {
			w.setError(err)
			return err
		}
	}
	return nil
}

// Close waits for outstanding batches and deregisters the handle; the files stay the caller's.
func (w *ringWriter) Close() error {
	w.order.waitDrained()
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return w.handle.close()
}

// syncTargets lists the files a batch must fsync, falling back to the files written by the batch.
func (w *ringWriter) syncTargets(batch []Write) []*os.File {
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
func (w *ringWriter) setError(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
}

// currentError reports the first fatal error, treating a closed writer as finished.
func (w *ringWriter) currentError() error {
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
