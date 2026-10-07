//go:build linux

package engine

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	uringEntries = 4096 // power of two; bounds outstanding SQEs
	uringSqeSize = 64
	uringCqeSize = 16
	uringCqBatch = 256 // CQEs drained per reaper pass

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

// rawSQE mirrors struct io_uring_sqe; the size assert in newUringRing pins the ABI.
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

// uringRing is one io_uring instance shared by every queue of a data directory.
type uringRing struct {
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

	submitMu sync.Mutex

	mu      sync.Mutex
	handles []*uringSinkHandle
	free    []uint32

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// newUringRing tries io_uring_setup; on any failure it returns errUringUnsupported and callers treat the ring as absent.
func newUringRing() (*uringRing, error) {
	if os.Getenv("MIKYU_DISABLE_URING") != "" {
		return nil, errUringUnsupported
	}
	if !uringKernelSupported() || unsafe.Sizeof(rawSQE{}) != uringSqeSize {
		return nil, errUringUnsupported
	}
	var params uringParams
	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(uringEntries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, errUringUnsupported
	}
	r := &uringRing{
		fd:      int(fd),
		entries: params.sqEntries,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if err := r.mapRings(&params); err != nil {
		_ = unix.Close(int(fd))
		return nil, errUringUnsupported
	}
	for i := uint32(0); i < r.entries; i++ {
		atomic.StoreUint32((*uint32)(unsafe.Add(unsafe.Pointer(r.sqArray), uintptr(i)*4)), i)
	}
	go r.loop()
	return r, nil
}

// mapRings maps the shared SQ ring, CQ ring, and SQE array at their protocol offsets.
func (r *uringRing) mapRings(p *uringParams) error {
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
	var u unix.Utsname
	if err := unix.Uname(&u); err != nil {
		return false
	}
	release := utsnameString(u.Release[:])
	majorText, minorText, found := strings.Cut(release, ".")
	if !found {
		return false
	}
	major, err := strconv.Atoi(majorText)
	if err != nil {
		return false
	}
	minor, err := strconv.Atoi(minorText)
	if err != nil {
		return false
	}
	return major > uringMinKernelMajor || (major == uringMinKernelMajor && minor >= uringMinKernelMinor)
}

func utsnameString(field []byte) string {
	out := make([]byte, 0, len(field))
	for _, c := range field {
		if c == 0 {
			break
		}
		out = append(out, byte(c))
	}
	return string(out)
}

// Submit writes the SQEs and issues one io_uring_enter per chunk, holding submitMu so concurrent producers never interleave their SQE writes.
func (r *uringRing) Submit(ops []uringOp) error {
	if len(ops) == 0 {
		return nil
	}
	r.submitMu.Lock()
	defer r.submitMu.Unlock()
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
		for k := 0; k < n; k++ {
			slot := (tail + uint32(k)) & r.sqMask
			r.encodeSQE((*rawSQE)(unsafe.Pointer(&r.sqes[uintptr(slot)*uringSqeSize])), ops[i+k])
		}
		atomic.StoreUint32(r.sqTail, tail+uint32(n))
		if err := r.enter(uint32(n), 0, 0); err != nil {			return err
		}
		i += n
	}
	return nil
}

func (r *uringRing) encodeSQE(sqe *rawSQE, op uringOp) {
	*sqe = rawSQE{userData: uint64(op.Sink)<<32 | uint64(op.Tag)}
	switch op.Kind {
	case uringOpWrite:
		sqe.opcode = uringOpWriteByte
		sqe.fd = int32(op.File.Fd())
		sqe.off = uint64(op.Off)
		sqe.addr = uint64(uintptr(unsafe.Pointer(unsafe.SliceData(op.Buf))))
		sqe.length = uint32(len(op.Buf))
	case uringOpFsync:
		sqe.opcode = uringOpFsyncByte
		sqe.fd = int32(op.File.Fd())
		if op.DataSync {
			sqe.off = uringFsyncDataSync
		}
	case uringOpNop:
		sqe.opcode = uringOpNopByte
	}
	if op.Link {
		sqe.flags |= sqeFlagLink
	}
	if op.Drain {
		sqe.flags |= sqeFlagDrain
	}
}

func (r *uringRing) enter(toSubmit, minComplete, flags uint32) error {
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

// Register allocates a sink id and returns the handle bound to it.
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

// Unregister clears the handle's slot and returns its id to the free list.
func (r *uringRing) Unregister(h *uringSinkHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if int(h.id) >= len(r.handles) || r.handles[h.id] != h {
		return
	}
	r.handles[h.id] = nil
	r.free = append(r.free, h.id)
}

// Close closes the ring fd; in-flight completions are lost and every handle with an outstanding tag sees ErrQueueClosed from its Wait.
func (r *uringRing) Close() error {
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

// wakeReaper submits a completion id that matches no handle so a reaper parked
// in enter returns and observes the stop signal.
func (r *uringRing) wakeReaper() {
	_ = r.Submit([]uringOp{{Kind: uringOpNop, Sink: uringSinkIDNone}})
}

func (r *uringRing) loop() {
	defer close(r.done)
	cqes := make([]rawCQE, uringCqBatch)
	for {
		select {
		case <-r.stop:
			return
		default:
		}
		n, err := r.waitCQEs(cqes)
		if err != nil {
			select {
			case <-r.stop:
				return
			default:
				continue
			}
		}
		for i := 0; i < n; i++ {
			r.dispatch(cqes[i])
		}
	}
}

// waitCQEs blocks until at least one CQE is available, copies out up to one
// batch, and advances the consumer head.
func (r *uringRing) waitCQEs(dst []rawCQE) (int, error) {
	for {
		ready := atomic.LoadUint32(r.cqTail) - atomic.LoadUint32(r.cqHead)
		if ready > 0 {
			n := min(int(ready), len(dst), int(r.entries))
			head := atomic.LoadUint32(r.cqHead)
			for i := 0; i < n; i++ {
				dst[i] = *(*rawCQE)(unsafe.Add(r.cqBase, uintptr((head+uint32(i))&r.cqMask)*uringCqeSize))
			}
			atomic.StoreUint32(r.cqHead, head+uint32(n))
			return n, nil
		}
		if err := r.enter(0, 1, uringEnterGetEvents); err != nil {
			return 0, err
		}
	}
}

// dispatch routes a completion to its handle; a completion whose sink id is no
// longer registered is dropped because no one is listening.
func (r *uringRing) dispatch(cqe rawCQE) {
	sinkID := uint32(cqe.userData >> 32)
	tag := uint32(cqe.userData)
	if sinkID >= uint32(len(r.handles)) {
		return
	}
	r.mu.Lock()
	h := r.handles[sinkID]
	r.mu.Unlock()
	if h != nil {
		h.complete(tag, int64(cqe.res))
	}
}

// uringSinkHandle tracks one sink's outstanding tags. Sync sinks wait on cond;
// async sinks install a notify callback and are driven by the reaper.
type uringSinkHandle struct {
	ring *uringRing
	id   uint32
	next atomic.Uint32

	mu     sync.Mutex
	cond   *sync.Cond
	pend   map[uint32]int32
	res    map[uint32]int64
	notify func(tag uint32, err error)
	closed bool
}

// NextTag returns a fresh tag for the next submission.
func (h *uringSinkHandle) NextTag() uint32 { return h.next.Add(1) - 1 }

// Submit registers the tag before the ring takes the ops and rolls the registration back if the submission fails.
func (h *uringSinkHandle) Submit(ops []uringOp) error {
	if len(ops) == 0 {
		return nil
	}
	for i := range ops {
		ops[i].Sink = h.id
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

// Wait blocks until every completion for tag has arrived, then reports the first error recorded for it, if any.
func (h *uringSinkHandle) Wait(tag uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.pend[tag] > 0 && !h.closed {
		h.cond.Wait()
	}
	if h.closed {
		return ErrQueueClosed
	}
	e := h.res[tag]
	delete(h.pend, tag)
	delete(h.res, tag)
	if e != 0 {
		return syscall.Errno(-e)
	}
	return nil
}

// complete is called by the reaper once per CQE; the first error for a tag wins.
func (h *uringSinkHandle) complete(tag uint32, res int64) {
	h.mu.Lock()
	n, ok := h.pend[tag]
	if !ok {
		h.mu.Unlock()
		return
	}
	if n > 0 {
		h.pend[tag] = n - 1
	}
	if res < 0 && h.res[tag] == 0 {
		h.res[tag] = res
	}
	if h.pend[tag] != 0 {
		h.mu.Unlock()
		return
	}
	var err error
	if e := h.res[tag]; e != 0 {
		err = syscall.Errno(-e)
	}
	delete(h.pend, tag)
	delete(h.res, tag)
	notify := h.notify
	h.mu.Unlock()
	if notify != nil {
		notify(tag, err)
		return
	}
	h.cond.Broadcast()
}

// fsync submits a data-sync-only fsync and waits for it.
func (h *uringSinkHandle) fsync(f *os.File) error {
	tag := h.NextTag()
	ops := []uringOp{{Kind: uringOpFsync, File: f, DataSync: true, Tag: tag}}
	if err := h.Submit(ops); err != nil {
		return err
	}
	return h.Wait(tag)
}

// Close marks the handle closed, wakes every waiter with ErrQueueClosed, and deregisters it from the ring.
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

// encodeIndexInto packs consecutive index entries into dst for a single SQE.
func encodeIndexInto(dst []byte, s sinkIndex) {
	for i := range s.Positions {
		var ent [rawIndexEntrySize]byte
		rawIndexEntry{pos: s.Positions[i], len: s.Lengths[i]}.MarshalBinary(&ent)
		copy(dst[i*rawIndexEntrySize:], ent[:])
	}
	if indexWriteHook != nil {
		indexWriteHook(s.Start, len(s.Positions)*rawIndexEntrySize)
	}
}
