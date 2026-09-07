package protocol

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/taskqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskWireRoundTrip(t *testing.T) {
	want := taskqueue.NewTaskFromWireWithDeliveryID(
		"task-1", []byte{0, 1, 2}, 2, 5,
		time.Unix(123, 456), time.Unix(100, 0), "delivery-1", 42,
	)
	var b Buffer
	EncodeTask(&b, want)
	got, err := DecodeTask(NewReader(b.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.Payload, got.Payload)
	assert.Equal(t, want.Attempts, got.Attempts)
	assert.Equal(t, want.MaxAttempts, got.MaxAttempts)
	assert.Equal(t, want.Visibility, got.Visibility)
	assert.Equal(t, want.ScheduledAt, got.ScheduledAt)
	assert.Equal(t, want.DeliveryID, got.DeliveryID)
	assert.Equal(t, want.OriginalOffset(), got.OriginalOffset())
}

func TestTaskWireRejectsMalformedPayload(t *testing.T) {
	var b Buffer
	b.WriteBytes(nil)
	_, err := DecodeTask(NewReader(b.Bytes()))
	assert.ErrorIs(t, err, taskqueue.ErrInvalidTask)
}

func TestTaskWireRejectsDeliveredTaskWithoutDeliveryID(t *testing.T) {
	want := taskqueue.NewTaskFromWire("task-1", []byte("payload"), 1, 2, time.Unix(1, 0), time.Unix(1, 0), 7)
	var b Buffer
	EncodeTask(&b, want)
	_, err := DecodeTask(NewReader(b.Bytes()))
	assert.ErrorIs(t, err, taskqueue.ErrInvalidTask)
}
