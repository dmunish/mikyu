package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskWireRoundTrip(t *testing.T) {
	want := Task{
		ID:          "task-1",
		Payload:     []byte{0, 1, 2},
		Attempts:    2,
		MaxAttempts: 5,
		Lease:  time.Unix(123, 456),
		ScheduledAt: time.Unix(100, 0),
		DeliveryID:  "delivery-1",
		Offset:      42,
	}
	var b Buffer
	EncodeTask(&b, want)
	got, err := DecodeTask(NewReader(b.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestTaskWireRejectsMalformedPayload(t *testing.T) {
	var b Buffer
	b.WriteBytes(nil)
	_, err := DecodeTask(NewReader(b.Bytes()))
	assert.ErrorIs(t, err, ErrInvalidTask)
}

func TestTaskWireRejectsDeliveredTaskWithoutDeliveryID(t *testing.T) {
	want := Task{
		ID:          "task-1",
		Payload:     []byte("payload"),
		Attempts:    1,
		MaxAttempts: 2,
		Lease:  time.Unix(1, 0),
		ScheduledAt: time.Unix(1, 0),
		Offset:      7,
	}
	var b Buffer
	EncodeTask(&b, want)
	_, err := DecodeTask(NewReader(b.Bytes()))
	assert.ErrorIs(t, err, ErrInvalidTask)
}

func TestTaskDequeueWireRoundTrip(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second, 1500 * time.Millisecond} {
		var b Buffer
		EncodeTaskDequeue(&b, "tasks", timeout)
		queue, got, err := DecodeTaskDequeue(NewReader(b.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, "tasks", queue)
		assert.Equal(t, timeout, got)
	}

	// Negative budgets clamp to the non-blocking 0, never to a negative wire
	// value (which would decode as a 64-bit millisecond timeout).
	var neg Buffer
	EncodeTaskDequeue(&neg, "tasks", -time.Second)
	_, got, err := DecodeTaskDequeue(NewReader(neg.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), got)
}

func TestTaskRefRoundTrip(t *testing.T) {
	task := Task{
		ID:          "task-1",
		Payload:     []byte("payload"),
		Attempts:    2,
		MaxAttempts: 5,
		Lease:  time.Unix(123, 0),
		ScheduledAt: time.Unix(100, 0),
		DeliveryID:  "delivery-1",
		Offset:      7,
	}

	var b Buffer
	EncodeTaskRef(&b, task)
	id, got, mode, err := DecodeTaskRef(NewReader(b.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, TaskRefModeTask, mode)
	assert.Empty(t, id)
	require.NotNil(t, got)
	assert.Equal(t, task, *got)

	b = Buffer{}
	EncodeTaskRef(&b, &task)
	_, ptr, mode, err := DecodeTaskRef(NewReader(b.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, TaskRefModeTask, mode)
	require.NotNil(t, ptr)
	assert.Equal(t, task, *ptr)

	b = Buffer{}
	EncodeTaskRef(&b, "just-an-id")
	id, got, mode, err = DecodeTaskRef(NewReader(b.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, TaskRefModeTaskID, mode)
	assert.Nil(t, got)
	assert.Equal(t, "just-an-id", id)
}

func TestTaskRefRejectsMalformed(t *testing.T) {
	// No mode byte at all.
	_, _, _, err := DecodeTaskRef(NewReader(nil))
	assert.ErrorIs(t, err, ErrInvalidTask)

	// Unknown mode.
	var b Buffer
	WriteMode(&b, 9)
	_, _, _, err = DecodeTaskRef(NewReader(b.Bytes()))
	assert.ErrorIs(t, err, ErrInvalidTask)

	// Mode 1 with an empty ID.
	b = Buffer{}
	EncodeTaskRef(&b, "")
	_, _, _, err = DecodeTaskRef(NewReader(b.Bytes()))
	assert.ErrorIs(t, err, ErrInvalidTask)
}

func TestTaskDequeueWireRejectsMalformedPayload(t *testing.T) {
	var noTimeout Buffer
	noTimeout.WriteBytes([]byte("tasks"))
	_, _, err := DecodeTaskDequeue(NewReader(noTimeout.Bytes()))
	assert.ErrorIs(t, err, ErrInvalidTask)

	var emptyQueue Buffer
	emptyQueue.WriteBytes(nil)
	emptyQueue.WriteU64(0)
	_, _, err = DecodeTaskDequeue(NewReader(emptyQueue.Bytes()))
	assert.ErrorIs(t, err, ErrInvalidTask)
}

// TestNackModeInheritNoDelay: a Nack frame in inherit mode ends right after the mode byte.
func TestNackModeInheritNoDelay(t *testing.T) {
	var b Buffer
	EncodeTaskRef(&b, "task-1")
	WriteMode(&b, NackModeInherit)

	r := NewReader(b.Bytes())
	id, task, refMode, err := DecodeTaskRef(r)
	require.NoError(t, err)
	assert.Nil(t, task)
	assert.Equal(t, "task-1", id)
	assert.Equal(t, TaskRefModeTaskID, refMode)

	mode, err := ReadMode(r)
	require.NoError(t, err)
	assert.Equal(t, NackModeInherit, mode)
	assert.Equal(t, len(r.b), r.i)
}

// TestNackModeExplicitDelay: a Nack frame in explicit mode carries its delay in nanoseconds.
func TestNackModeExplicitDelay(t *testing.T) {
	var b Buffer
	EncodeTaskRef(&b, "task-1")
	WriteMode(&b, NackModeExplicit)
	b.WriteU64(uint64(int64(2 * time.Second)))

	r := NewReader(b.Bytes())
	_, _, _, err := DecodeTaskRef(r)
	require.NoError(t, err)

	mode, err := ReadMode(r)
	require.NoError(t, err)
	assert.Equal(t, NackModeExplicit, mode)

	delay, err := r.ReadU64()
	require.NoError(t, err)
	assert.Equal(t, uint64(int64(2*time.Second)), delay)
	assert.Equal(t, len(r.b), r.i)
}
