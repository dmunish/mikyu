package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleTask is a delivered task (non-zero Visibility requires a DeliveryID).
func sampleTask() Task {
	return Task{
		ID:          "task-1",
		Payload:     []byte("payload"),
		Attempts:    1,
		MaxAttempts: 5,
		Visibility:  time.Unix(123, 0),
		ScheduledAt: time.Unix(100, 0),
		DeliveryID:  "delivery-1",
		Offset:      42,
	}
}

var opNames = map[byte]string{
	OpCreate: "create", OpDelete: "delete", OpList: "list",
	OpUpdate: "update", OpDescribe: "describe", OpTruncate: "truncate", OpMetrics: "metrics",
	OpAppend: "append", OpAppendBatch: "append_batch", OpRead: "read", OpReadBatch: "read_batch",
	OpLastOffset: "last_offset", OpMinOffset: "min_offset", OpCommit: "commit", OpCommitted: "committed", OpFetch: "fetch",
	OpEnqueue: "enqueue", OpDequeue: "dequeue", OpAck: "ack", OpNack: "nack", OpExtend: "extend", OpArchive: "archive",
	OpDeadLetters: "dead_letters", OpDeadLetterAck: "dead_letter_ack",
}

// TestFrameRoundTripAllOps encodes a representative request payload for every
// opcode in the frozen table and decodes it back field by field. Response
// payloads share the same field layouts (u64 offsets, message records, task
// records), so this covers both directions.
func TestFrameRoundTripAllOps(t *testing.T) {
	payloads := func(op byte) []byte {
		var b Buffer
		switch op {
		case OpCreate:
			b.WriteU8(1) // kind: log
			b.WriteBytes([]byte("payments"))
			b.WriteU16(2)
			b.WriteBytes([]byte(`{"engine":{"fsync":true}}`))
		case OpDelete:
			b.WriteBytes([]byte("payments"))
		case OpList:
		case OpUpdate:
			b.WriteBytes([]byte("payments"))
			b.WriteU16(2)
			b.WriteBytes([]byte(`{"engine":{"batch_size":64}}`))
		case OpDescribe:
			b.WriteBytes([]byte("payments"))
		case OpTruncate:
			b.WriteBytes([]byte("payments"))
			b.WriteU64(7)
		case OpMetrics:
		case OpAppend:
			b.WriteBytes([]byte("events"))
			b.WriteBytes([]byte("hello"))
		case OpAppendBatch:
			b.WriteBytes([]byte("events"))
			b.WriteU32(2)
			b.WriteBytes([]byte("one"))
			b.WriteBytes([]byte("two"))
		case OpRead:
			b.WriteBytes([]byte("events"))
			b.WriteU64(3)
		case OpReadBatch:
			b.WriteBytes([]byte("events"))
			b.WriteU64(3)
			b.WriteU32(10)
		case OpLastOffset, OpMinOffset:
			b.WriteBytes([]byte("events"))
		case OpCommit:
			b.WriteBytes([]byte("events"))
			b.WriteBytes([]byte("consumers"))
			b.WriteU64(9)
		case OpCommitted:
			b.WriteBytes([]byte("events"))
			b.WriteBytes([]byte("consumers"))
		case OpEnqueue:
			b.WriteBytes([]byte("tasks"))
			b.WriteBytes([]byte("work"))
			b.WriteU64(uint64(int64(time.Minute)))
			b.WriteU32(3)
			b.WriteU64(uint64(time.Now().UnixNano()))
		case OpDequeue:
			EncodeTaskDequeue(&b, "tasks", 250*time.Millisecond)
		case OpAck, OpDeadLetterAck:
			b.WriteBytes([]byte("tasks"))
			EncodeTaskRef(&b, sampleTask())
		case OpArchive:
			b.WriteBytes([]byte("tasks"))
			EncodeTaskRef(&b, sampleTask())
		case OpNack:
			b.WriteBytes([]byte("tasks"))
			EncodeTaskRef(&b, "just-an-id")
			WriteMode(&b, NackModeExplicit)
			b.WriteU64(uint64(int64(2 * time.Second)))
		case OpExtend:
			b.WriteBytes([]byte("tasks"))
			EncodeTaskRef(&b, sampleTask())
			b.WriteU64(uint64(int64(90 * time.Second)))
		case OpDeadLetters:
			b.WriteBytes([]byte("tasks"))
		case OpFetch:
			b.WriteBytes([]byte("events"))
			b.WriteU64(0)
			b.WriteU32(10)
			b.WriteU64(uint64(int64(30 * time.Second)))
		default:
			t.Fatalf("no representative payload for op 0x%02X", op)
		}
		return b.Bytes()
	}

	ops := []byte{
		OpCreate, OpDelete, OpList, OpUpdate, OpDescribe, OpTruncate, OpMetrics,
		OpAppend, OpAppendBatch, OpRead, OpReadBatch, OpLastOffset, OpMinOffset, OpCommit, OpCommitted, OpFetch,
		OpEnqueue, OpDequeue, OpAck, OpNack, OpExtend, OpArchive, OpDeadLetters, OpDeadLetterAck,
	}
	for _, op := range ops {
		t.Run(opNames[op], func(t *testing.T) {
			r := NewReader(payloads(op))

			switch op {
			case OpCreate:
				kind, err := r.ReadU8()
				require.NoError(t, err)
				assert.Equal(t, byte(1), kind)
				name, err := r.ReadBytes()
				require.NoError(t, err)
				assert.Equal(t, "payments", string(name))
				version, err := r.ReadU16()
				require.NoError(t, err)
				assert.Equal(t, uint16(2), version)
				data, err := r.ReadBytes()
				require.NoError(t, err)
				assert.JSONEq(t, `{"engine":{"fsync":true}}`, string(data))
			case OpUpdate:
				name, err := r.ReadBytes()
				require.NoError(t, err)
				assert.Equal(t, "payments", string(name))
				version, err := r.ReadU16()
				require.NoError(t, err)
				assert.Equal(t, uint16(2), version)
				data, err := r.ReadBytes()
				require.NoError(t, err)
				assert.JSONEq(t, `{"engine":{"batch_size":64}}`, string(data))
			case OpDelete, OpDescribe, OpLastOffset, OpMinOffset, OpDeadLetters:
				name, err := r.ReadBytes()
				require.NoError(t, err)
				assert.NotEmpty(t, name)
			case OpAppend:
				assert.Equal(t, "events", string(mustReadBytes(t, r)))
				assert.Equal(t, "hello", string(mustReadBytes(t, r)))
			case OpTruncate:
				assert.NotEmpty(t, mustReadBytes(t, r))
				off, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(7), off)
			case OpRead:
				assert.NotEmpty(t, mustReadBytes(t, r))
				off, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(3), off)
			case OpReadBatch:
				assert.NotEmpty(t, mustReadBytes(t, r))
				start, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(3), start)
				count, err := r.ReadU32()
				require.NoError(t, err)
				assert.Equal(t, uint32(10), count)
			case OpCommit:
				assert.NotEmpty(t, mustReadBytes(t, r))
				group, err := r.ReadBytes()
				require.NoError(t, err)
				assert.Equal(t, "consumers", string(group))
				off, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(9), off)
			case OpCommitted:
				assert.NotEmpty(t, mustReadBytes(t, r))
				group, err := r.ReadBytes()
				require.NoError(t, err)
				assert.Equal(t, "consumers", string(group))
			case OpDequeue:
				queue, timeout, err := DecodeTaskDequeue(r)
				require.NoError(t, err)
				assert.Equal(t, "tasks", queue)
				assert.Equal(t, 250*time.Millisecond, timeout)
			case OpAck, OpDeadLetterAck, OpArchive, OpNack, OpExtend:
				assert.NotEmpty(t, mustReadBytes(t, r))
				id, task, mode, err := DecodeTaskRef(r)
				require.NoError(t, err)
				switch op {
				case OpNack:
					assert.Equal(t, TaskRefModeTaskID, mode)
					assert.Equal(t, "just-an-id", id)
					nackMode, err := ReadMode(r)
					require.NoError(t, err)
					assert.Equal(t, NackModeExplicit, nackMode)
					retry, err := r.ReadU64()
					require.NoError(t, err)
					assert.Equal(t, uint64(int64(2*time.Second)), retry)
				default:
					assert.Equal(t, TaskRefModeTask, mode)
					require.NotNil(t, task)
					assert.Equal(t, sampleTask(), *task)
					if op == OpExtend {
						timeout, err := r.ReadU64()
						require.NoError(t, err)
						assert.Equal(t, uint64(int64(90*time.Second)), timeout)
					}
				}
			case OpFetch:
				assert.NotEmpty(t, mustReadBytes(t, r))
				start, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(0), start)
				count, err := r.ReadU32()
				require.NoError(t, err)
				assert.Equal(t, uint32(10), count)
				wait, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(int64(30*time.Second)), wait)
			case OpAppendBatch:
				assert.NotEmpty(t, mustReadBytes(t, r))
				n, err := r.ReadU32()
				require.NoError(t, err)
				assert.Equal(t, uint32(2), n)
				for range n {
					assert.NotEmpty(t, mustReadBytes(t, r))
				}
			case OpEnqueue:
				assert.NotEmpty(t, mustReadBytes(t, r))
				payload, err := r.ReadBytes()
				require.NoError(t, err)
				assert.Equal(t, "work", string(payload))
				vis, err := r.ReadU64()
				require.NoError(t, err)
				assert.Equal(t, uint64(int64(time.Minute)), vis)
				attempts, err := r.ReadU32()
				require.NoError(t, err)
				assert.Equal(t, uint32(3), attempts)
				_, err = r.ReadU64() // scheduledAt
				require.NoError(t, err)
			}
			// Every payload must be fully consumed: a trailing byte means the
			// encoder and decoder disagree on the layout.
			assert.Equal(t, len(r.b), r.i)
		})
	}

	// Nack in inherit mode carries no delay bytes at all.
	t.Run("nack_inherit_mode", func(t *testing.T) {
		var b Buffer
		b.WriteBytes([]byte("tasks"))
		EncodeTaskRef(&b, "just-an-id")
		WriteMode(&b, NackModeInherit)

		r := NewReader(b.Bytes())
		assert.NotEmpty(t, mustReadBytes(t, r))
		id, _, refMode, err := DecodeTaskRef(r)
		require.NoError(t, err)
		assert.Equal(t, TaskRefModeTaskID, refMode)
		assert.Equal(t, "just-an-id", id)
		mode, err := ReadMode(r)
		require.NoError(t, err)
		assert.Equal(t, NackModeInherit, mode)
		assert.Equal(t, len(r.b), r.i, "inherit mode must not write a delay")
	})
}

func mustReadBytes(t *testing.T, r *Reader) []byte {
	t.Helper()
	b, err := r.ReadBytes()
	require.NoError(t, err)
	return b
}