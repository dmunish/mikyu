package taskqueue

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

var taskRecordMagic = [8]byte{'M', 'I', 'K', 'Y', 'U', 'T', 'Q', 1}

const (
	recordTask                 byte  = 1
	recordCPStart              byte  = 2
	recordCPChunk              byte  = 3
	recordCPCommit             byte  = 4
	recordDLQ                  byte  = 5
	recordDLQAck               byte  = 6
	checkpointVersion                = 2
	maxCheckpointChunks              = 1 << 20
	maxCheckpointBytes               = 512 << 20
	checkpointChunkHeaderBytes int64 = 8 + 1 + 4 + 4
)

type checkpointCommit struct {
	startOffset    uint64
	totalChunks    uint32
	coveredThrough uint64
}

func encodeTaskRecord(id string, maxAttempts uint32, visibilityTimeout time.Duration, scheduledAt time.Time, payload []byte) []byte {
	var b bytes.Buffer
	writeHeader(&b, recordTask)
	writeString(&b, id)
	writeU32(&b, maxAttempts)
	writeI64(&b, int64(visibilityTimeout))
	writeTime(&b, scheduledAt)
	writeU32(&b, uint32(len(payload)))
	_, _ = b.Write(payload)
	return b.Bytes()
}

func decodeTaskRecord(payload []byte) (id string, maxAttempts uint32, visibilityTimeout time.Duration, scheduledAt time.Time, taskPayload []byte, err error) {
	r, err := newRecordReader(payload, recordTask)
	if err != nil {
		return "", 0, 0, time.Time{}, nil, err
	}
	if id, err = r.readString(); err != nil || id == "" {
		return "", 0, 0, time.Time{}, nil, ErrInvalidTask
	}
	if maxAttempts, err = r.readU32(); err != nil || maxAttempts == 0 {
		return "", 0, 0, time.Time{}, nil, ErrInvalidTask
	}
	var timeout int64
	if timeout, err = r.readI64(); err != nil || timeout <= 0 {
		return "", 0, 0, time.Time{}, nil, ErrInvalidTask
	}
	visibilityTimeout = time.Duration(timeout)
	if scheduledAt, err = r.readTime(); err != nil {
		return "", 0, 0, time.Time{}, nil, ErrInvalidTask
	}
	if taskPayload, err = r.readBytes(); err != nil || !r.empty() {
		return "", 0, 0, time.Time{}, nil, ErrInvalidTask
	}
	return id, maxAttempts, visibilityTimeout, scheduledAt, taskPayload, nil
}

func encodeDeadLetterRecord(task Task) []byte {
	var b bytes.Buffer
	writeHeader(&b, recordDLQ)
	writeU64(&b, task.offset)
	writeString(&b, task.ID)
	writeU32(&b, task.Attempts)
	writeU32(&b, task.MaxAttempts)
	writeTime(&b, task.ScheduledAt)
	writeU32(&b, uint32(len(task.Payload)))
	b.Write(task.Payload)
	return b.Bytes()
}

func decodeDeadLetterRecord(payload []byte) (Task, error) {
	r, err := newRecordReader(payload, recordDLQ)
	if err != nil {
		return Task{}, ErrCheckpointCorrupt
	}
	off, err := r.readU64()
	if err != nil {
		return Task{}, ErrCheckpointCorrupt
	}
	id, err := r.readString()
	if err != nil || id == "" {
		return Task{}, ErrCheckpointCorrupt
	}
	attempts, err := r.readU32()
	if err != nil || attempts == 0 {
		return Task{}, ErrCheckpointCorrupt
	}
	maxAttempts, err := r.readU32()
	if err != nil || maxAttempts == 0 || attempts > maxAttempts {
		return Task{}, ErrCheckpointCorrupt
	}
	scheduledAt, err := r.readTime()
	if err != nil || scheduledAt.IsZero() {
		return Task{}, ErrCheckpointCorrupt
	}
	payload, err = r.readBytes()
	if err != nil || !r.empty() {
		return Task{}, ErrCheckpointCorrupt
	}
	return Task{ID: id, Payload: payload, Attempts: attempts, MaxAttempts: maxAttempts, ScheduledAt: scheduledAt, offset: off}, nil
}

func encodeDeadLetterAck(task Task) []byte {
	var b bytes.Buffer
	writeHeader(&b, recordDLQAck)
	writeU64(&b, task.offset)
	writeString(&b, task.ID)
	writeU32(&b, task.Attempts)
	return b.Bytes()
}

func decodeDeadLetterAck(payload []byte) (string, uint64, uint32, error) {
	r, err := newRecordReader(payload, recordDLQAck)
	if err != nil {
		return "", 0, 0, ErrCheckpointCorrupt
	}
	off, err := r.readU64()
	if err != nil {
		return "", 0, 0, ErrCheckpointCorrupt
	}
	id, err := r.readString()
	if err != nil || id == "" {
		return "", 0, 0, ErrCheckpointCorrupt
	}
	attempts, err := r.readU32()
	if err != nil || !r.empty() {
		return "", 0, 0, ErrCheckpointCorrupt
	}
	return id, off, attempts, nil
}

func isTaskRecord(payload []byte) bool {
	return len(payload) >= len(taskRecordMagic)+1 && bytes.Equal(payload[:len(taskRecordMagic)], taskRecordMagic[:])
}
func recordKind(payload []byte) byte {
	if !isTaskRecord(payload) {
		return 0
	}
	return payload[len(taskRecordMagic)]
}
func writeHeader(b *bytes.Buffer, kind byte) { b.Write(taskRecordMagic[:]); b.WriteByte(kind) }

func encodeCheckpointStart(coveredThrough uint64) []byte {
	var b bytes.Buffer
	writeHeader(&b, recordCPStart)
	writeU64(&b, coveredThrough)
	writeU32(&b, checkpointVersion)
	return b.Bytes()
}
func encodeCheckpointChunk(index uint32, data []byte) []byte {
	var b bytes.Buffer
	writeHeader(&b, recordCPChunk)
	writeU32(&b, index)
	writeU32(&b, uint32(len(data)))
	b.Write(data)
	return b.Bytes()
}
func encodeCheckpointCommit(startOffset uint64, totalChunks uint32, coveredThrough uint64) []byte {
	var b bytes.Buffer
	writeHeader(&b, recordCPCommit)
	writeU64(&b, startOffset)
	writeU32(&b, totalChunks)
	writeU64(&b, coveredThrough)
	writeU32(&b, checkpointVersion)
	return b.Bytes()
}
func decodeCheckpointStart(payload []byte) (uint64, error) {
	r, err := newRecordReader(payload, recordCPStart)
	if err != nil {
		return 0, err
	}
	covered, err := r.readU64()
	if err != nil {
		return 0, ErrCheckpointCorrupt
	}
	version, err := r.readU32()
	if err != nil || version != checkpointVersion || !r.empty() {
		return 0, ErrCheckpointCorrupt
	}
	return covered, nil
}

func decodeCheckpointCommit(payload []byte) (checkpointCommit, error) {
	r, err := newRecordReader(payload, recordCPCommit)
	if err != nil {
		return checkpointCommit{}, err
	}
	start, err := r.readU64()
	if err != nil {
		return checkpointCommit{}, ErrCheckpointCorrupt
	}
	total, err := r.readU32()
	if err != nil || total == 0 || total > maxCheckpointChunks {
		return checkpointCommit{}, ErrCheckpointCorrupt
	}
	covered, err := r.readU64()
	if err != nil {
		return checkpointCommit{}, ErrCheckpointCorrupt
	}
	version, err := r.readU32()
	if err != nil || version != checkpointVersion || !r.empty() {
		return checkpointCommit{}, ErrCheckpointCorrupt
	}
	return checkpointCommit{startOffset: start, totalChunks: total, coveredThrough: covered}, nil
}
func decodeCheckpointChunk(payload []byte) (uint32, []byte, error) {
	r, err := newRecordReader(payload, recordCPChunk)
	if err != nil {
		return 0, nil, err
	}
	idx, err := r.readU32()
	if err != nil {
		return 0, nil, ErrCheckpointCorrupt
	}
	data, err := r.readBytes()
	if err != nil || len(data) > checkpointChunkBytes || !r.empty() {
		return 0, nil, ErrCheckpointCorrupt
	}
	return idx, data, nil
}

func encodeSnapshot(states []*taskState, dlq map[string]Task) []byte {
	statesCopy := append([]*taskState(nil), states...)
	sort.Slice(statesCopy, func(i, j int) bool { return statesCopy[i].task.offset < statesCopy[j].task.offset })
	dead := make([]Task, 0, len(dlq))
	for _, task := range dlq {
		dead = append(dead, task)
	}
	sort.Slice(dead, func(i, j int) bool { return dead[i].offset < dead[j].offset })
	var b bytes.Buffer
	writeU32(&b, uint32(len(statesCopy)))
	for _, state := range statesCopy {
		writeU64(&b, state.task.offset)
		writeString(&b, state.task.ID)
		writeU32(&b, state.task.Attempts)
		writeU32(&b, state.task.MaxAttempts)
		writeI64(&b, int64(state.visibilityTimeout))
		writeTime(&b, state.task.Visibility)
		writeTime(&b, state.task.ScheduledAt)
		writeString(&b, state.task.DeliveryID)
		writeTime(&b, state.nextAt)
		if state.inFlight {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
		writeU32(&b, uint32(len(state.task.Payload)))
		b.Write(state.task.Payload)
	}
	writeU32(&b, uint32(len(dead)))
	for _, task := range dead {
		writeU64(&b, task.offset)
		writeString(&b, task.ID)
		writeU32(&b, task.Attempts)
		writeU32(&b, task.MaxAttempts)
		writeTime(&b, task.ScheduledAt)
		writeU32(&b, uint32(len(task.Payload)))
		b.Write(task.Payload)
	}
	return b.Bytes()
}

func decodeSnapshot(data []byte) ([]taskState, map[string]Task, error) {
	r := newReader(data)
	count, err := r.readU32()
	if err != nil || count > 10_000_000 {
		return nil, nil, ErrCheckpointCorrupt
	}
	states := make([]taskState, 0, count)
	seen := make(map[string]struct{}, count)
	for i := uint32(0); i < count; i++ {
		offset, err := r.readU64()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		id, err := r.readString()
		if err != nil || id == "" {
			return nil, nil, ErrCheckpointCorrupt
		}
		if _, ok := seen[id]; ok {
			return nil, nil, ErrCheckpointCorrupt
		}
		seen[id] = struct{}{}
		attempts, err := r.readU32()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		maxAttempts, err := r.readU32()
		if err != nil || maxAttempts == 0 || attempts > maxAttempts {
			return nil, nil, ErrCheckpointCorrupt
		}
		visibilityTimeout, err := r.readI64()
		if err != nil || visibilityTimeout <= 0 {
			return nil, nil, ErrCheckpointCorrupt
		}
		visibility, err := r.readTime()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		scheduled, err := r.readTime()
		if err != nil || scheduled.IsZero() {
			return nil, nil, ErrCheckpointCorrupt
		}
		deliveryID, err := r.readString()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		nextAt, err := r.readTime()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		flag, err := r.readByte()
		if err != nil || (flag != 0 && flag != 1) {
			return nil, nil, ErrCheckpointCorrupt
		}
		payload, err := r.readBytes()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		if (flag == 1 && visibility.IsZero()) || (flag == 0 && !visibility.IsZero()) {
			return nil, nil, ErrCheckpointCorrupt
		}
		if (flag == 1 && deliveryID == "") || (flag == 0 && deliveryID != "") {
			return nil, nil, ErrCheckpointCorrupt
		}
		states = append(states, taskState{task: Task{ID: id, Payload: payload, Attempts: attempts, MaxAttempts: maxAttempts, Visibility: visibility, ScheduledAt: scheduled, DeliveryID: deliveryID, offset: offset}, nextAt: nextAt, visibilityTimeout: time.Duration(visibilityTimeout), inFlight: flag == 1})
	}

	dlqCount, err := r.readU32()
	if err != nil || dlqCount > 10_000_000 {
		return nil, nil, ErrCheckpointCorrupt
	}
	dlq := make(map[string]Task, dlqCount)
	for i := uint32(0); i < dlqCount; i++ {
		offset, err := r.readU64()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		id, err := r.readString()
		if err != nil || id == "" {
			return nil, nil, ErrCheckpointCorrupt
		}
		if _, ok := seen[id]; ok {
			return nil, nil, ErrCheckpointCorrupt
		}
		if _, ok := dlq[id]; ok {
			return nil, nil, ErrCheckpointCorrupt
		}
		attempts, err := r.readU32()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		maxAttempts, err := r.readU32()
		if err != nil || maxAttempts == 0 || attempts > maxAttempts {
			return nil, nil, ErrCheckpointCorrupt
		}
		scheduled, err := r.readTime()
		if err != nil || scheduled.IsZero() {
			return nil, nil, ErrCheckpointCorrupt
		}
		payload, err := r.readBytes()
		if err != nil {
			return nil, nil, ErrCheckpointCorrupt
		}
		dlq[id] = Task{ID: id, Payload: payload, Attempts: attempts, MaxAttempts: maxAttempts, ScheduledAt: scheduled, offset: offset}
	}
	if !r.empty() {
		return nil, nil, ErrCheckpointCorrupt
	}
	return states, dlq, nil
}

func (q *TaskQueue) checkpointLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The physical queue can be shared with the log driver. Refresh first so
	// foreign records appended since the last task operation are included in
	// the checkpoint's covered range without being interpreted as tasks.
	if err := q.refreshLocked(ctx); err != nil {
		return err
	}
	coveredThrough := q.scanNext
	states := make([]*taskState, 0, len(q.active))
	for _, state := range q.active {
		cp := *state
		cp.task.Payload = bytes.Clone(state.task.Payload)
		states = append(states, &cp)
	}
	snapshot := encodeSnapshot(states, q.dlq)
	startOffset, err := q.storage.Append(ctx, encodeCheckpointStart(coveredThrough))
	if err != nil {
		return err
	}
	storagePayloadLimit := q.storage.MaxAppendPayloadBytes()
	if storagePayloadLimit <= checkpointChunkHeaderBytes {
		return engine.ErrRecordTooLarge
	}

	maxChunkBytes := int64(checkpointChunkBytes)
	if storagePayloadLimit-checkpointChunkHeaderBytes < maxChunkBytes {
		maxChunkBytes = storagePayloadLimit - checkpointChunkHeaderBytes
	}

	var total uint32
	for len(snapshot) > 0 {
		chunkLen := len(snapshot)
		if int64(chunkLen) > maxChunkBytes {
			chunkLen = int(maxChunkBytes)
		}
		if _, err := q.storage.Append(ctx, encodeCheckpointChunk(total, snapshot[:chunkLen])); err != nil {
			return err
		}
		snapshot = snapshot[chunkLen:]
		total++
	}
	if total == 0 {
		if _, err := q.storage.Append(ctx, encodeCheckpointChunk(0, nil)); err != nil {
			return err
		}
		total = 1
	}
	commitOffset, err := q.storage.Append(ctx, encodeCheckpointCommit(startOffset, total, coveredThrough))
	if err != nil {
		return err
	}
	if commitOffset != startOffset+uint64(total)+1 {
		return ErrCheckpointCorrupt
	}
	q.checkpointOffset = startOffset
	q.scanNext = commitOffset + 1
	for id := range q.dlqUncheckpointed {
		delete(q.dlqUncheckpointed, id)
	}
	q.dirty = 0
	return nil
}

func (q *TaskQueue) loadCheckpointLocked(ctx context.Context) (bool, error) {
	last, min := q.storage.LastOffset(), q.storage.MinOffset()
	if last <= min {
		return false, nil
	}
	for off := last; off > min; {
		off--
		msg, err := q.storage.Read(ctx, off)
		if err != nil {
			if err == engine.ErrOffsetTruncated {
				return false, nil
			}
			return false, err
		}
		if recordKind(msg.Payload) != recordCPCommit {
			continue
		}
		commit, err := decodeCheckpointCommit(msg.Payload)
		if err != nil {
			continue
		}
		if commit.startOffset < min || commit.startOffset >= off || commit.startOffset+uint64(commit.totalChunks)+1 != off {
			continue
		}
		if uint64(commit.totalChunks)*checkpointChunkBytes > maxCheckpointBytes {
			continue
		}
		startMsg, err := q.storage.Read(ctx, commit.startOffset)
		if err != nil {
			continue
		}
		coveredThrough, err := decodeCheckpointStart(startMsg.Payload)
		if err != nil || coveredThrough != commit.coveredThrough || coveredThrough != commit.startOffset {
			continue
		}
		chunks := make([][]byte, commit.totalChunks)
		valid := true
		for i := uint32(0); i < commit.totalChunks; i++ {
			chunkMsg, err := q.storage.Read(ctx, commit.startOffset+1+uint64(i))
			if err != nil {
				valid = false
				break
			}
			idx, data, err := decodeCheckpointChunk(chunkMsg.Payload)
			if err != nil || idx != i {
				valid = false
				break
			}
			chunks[i] = data
		}
		if !valid {
			continue
		}
		var snapshot bytes.Buffer
		for _, chunk := range chunks {
			snapshot.Write(chunk)
		}
		if snapshot.Len() > maxCheckpointBytes {
			continue
		}
		states, dlq, err := decodeSnapshot(snapshot.Bytes())
		if err != nil {
			continue
		}
		for id := range q.active {
			delete(q.active, id)
		}
		for id := range q.dlq {
			delete(q.dlq, id)
		}
		for i := range states {
			state := &states[i]
			q.active[state.task.ID] = state
			q.pushScheduleLocked(state)
		}
		for id, task := range dlq {
			q.dlq[id] = task
		}
		clear(q.dlqUncheckpointed)
		q.checkpointOffset = commit.startOffset
		q.scanNext = off + 1
		return true, nil
	}
	return false, nil
}

func (q *TaskQueue) refreshLocked(ctx context.Context) error {
	last := q.storage.LastOffset()
	for q.scanNext < last {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := q.storage.Read(ctx, q.scanNext)
		if err != nil {
			return err
		}
		switch recordKind(msg.Payload) {
		case recordTask:
			id, maxAttempts, visibilityTimeout, scheduledAt, payload, err := decodeTaskRecord(msg.Payload)
			if err != nil {
				return err
			}
			if _, exists := q.active[id]; exists {
				return ErrInvalidTask
			}
			if _, exists := q.dlq[id]; exists {
				return ErrInvalidTask
			}
			state := &taskState{task: Task{ID: id, Payload: bytes.Clone(payload), MaxAttempts: maxAttempts, ScheduledAt: scheduledAt, offset: msg.Offset}, nextAt: scheduledAt, visibilityTimeout: visibilityTimeout}
			q.active[id] = state
			q.pushScheduleLocked(state)
		case recordDLQ:
			task, err := decodeDeadLetterRecord(msg.Payload)
			if err != nil {
				return err
			}
			delete(q.active, task.ID)
			q.dlq[task.ID] = task
		case recordDLQAck:
			id, off, attempts, err := decodeDeadLetterAck(msg.Payload)
			if err != nil {
				return err
			}
			if current, ok := q.dlq[id]; ok && current.offset == off && current.Attempts == attempts {
				delete(q.dlq, id)
			}
		case recordCPStart, recordCPChunk, recordCPCommit:
		default:
			// This storage queue may also be used by the log driver.
			// Unknown record types are owned by another driver and are
			// intentionally ignored here; the retention coordinator keeps
			// the drivers' truncation floors independent.
		}
		q.scanNext++
	}
	return nil
}

func writeU32(b *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	b.Write(tmp[:])
}
func writeU64(b *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	b.Write(tmp[:])
}
func writeI64(b *bytes.Buffer, v int64)     { writeU64(b, uint64(v)) }
func writeString(b *bytes.Buffer, s string) { writeU32(b, uint32(len(s))); b.WriteString(s) }
func writeTime(b *bytes.Buffer, t time.Time) {
	if t.IsZero() {
		b.WriteByte(0)
		return
	}
	b.WriteByte(1)
	writeI64(b, t.UnixNano())
}

type recordReader struct{ r *reader }

func newRecordReader(data []byte, expected byte) (*recordReader, error) {
	if len(data) < len(taskRecordMagic)+1 || !bytes.Equal(data[:len(taskRecordMagic)], taskRecordMagic[:]) || data[len(taskRecordMagic)] != expected {
		return nil, ErrCheckpointCorrupt
	}
	return &recordReader{newReader(data[len(taskRecordMagic)+1:])}, nil
}
func (r *recordReader) readByte() (byte, error)      { return r.r.readByte() }
func (r *recordReader) readU32() (uint32, error)     { return r.r.readU32() }
func (r *recordReader) readU64() (uint64, error)     { return r.r.readU64() }
func (r *recordReader) readI64() (int64, error)      { v, e := r.r.readU64(); return int64(v), e }
func (r *recordReader) readString() (string, error)  { b, e := r.r.readBytes(); return string(b), e }
func (r *recordReader) readBytes() ([]byte, error)   { return r.r.readBytes() }
func (r *recordReader) readTime() (time.Time, error) { return r.r.readTime() }
func (r *recordReader) empty() bool                  { return r.r.empty() }

type reader struct {
	b []byte
	i int
}

func newReader(b []byte) *reader { return &reader{b: b} }
func (r *reader) need(n int) error {
	if n < 0 || r.i+n > len(r.b) {
		return ErrCheckpointCorrupt
	}
	return nil
}
func (r *reader) readString() (string, error) {
	b, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *reader) readI64() (int64, error) {
	v, err := r.readU64()
	return int64(v), err
}

func (r *reader) readByte() (byte, error) {
	if e := r.need(1); e != nil {
		return 0, e
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}
func (r *reader) readU32() (uint32, error) {
	if e := r.need(4); e != nil {
		return 0, e
	}
	v := binary.BigEndian.Uint32(r.b[r.i : r.i+4])
	r.i += 4
	return v, nil
}
func (r *reader) readU64() (uint64, error) {
	if e := r.need(8); e != nil {
		return 0, e
	}
	v := binary.BigEndian.Uint64(r.b[r.i : r.i+8])
	r.i += 8
	return v, nil
}
func (r *reader) readBytes() ([]byte, error) {
	n, e := r.readU32()
	if e != nil {
		return nil, e
	}
	if int64(n) > int64(len(r.b)-r.i) {
		return nil, ErrCheckpointCorrupt
	}
	v := bytes.Clone(r.b[r.i : r.i+int(n)])
	r.i += int(n)
	return v, nil
}
func (r *reader) readTime() (time.Time, error) {
	flag, e := r.readByte()
	if e != nil {
		return time.Time{}, e
	}
	if flag == 0 {
		return time.Time{}, nil
	}
	if flag != 1 {
		return time.Time{}, ErrCheckpointCorrupt
	}
	v, e := r.readU64()
	if e != nil {
		return time.Time{}, e
	}
	return time.Unix(0, int64(v)), nil
}
func (r *reader) empty() bool { return r.i == len(r.b) }
