package mikyu

import (
	"context"
	"io"
	"iter"
	"time"
)

// LogQueue is an append-only stream of records.
//
// Producers Append (or AppendBatch) and receive a monotonically
// increasing offset for each record. Consumers Read from any offset in
// [MinOffset, LastOffset), Fetch with a wait to long-poll, Stream to
// catch up on history, and Tail to follow the end of the stream.
//
// All offsets are stable for the life of the data: a record's offset
// never changes, and reads of untruncated history are deterministic.
type LogQueue interface {
	// Name returns the queue's name, as given to OpenLog, CreateLog, or
	// Log.
	Name() string

	// Append appends one record and returns its offset.
	//
	// By default the call returns only after the record is safely on
	// disk (durability sync). A queue configured with DurabilityAsync
	// returns sooner, trading a small loss window on a crash for higher
	// throughput.
	//
	// A payload larger than the queue's max_payload setting fails with
	// ErrMessageTooLarge.
	Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error)

	// AppendBatch appends many records in one call and returns their
	// offsets in order. Batching amortizes the write cost across
	// records and preserves order exactly.
	AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error)

	// Read reads the record at one offset.
	//
	// An offset past the end of the stream, or one that never existed,
	// fails with ErrOffsetNotFound. An offset that retention or
	// Truncate removed fails with ErrOffsetTruncated. An offset that
	// keyed compaction replaced fails with ErrRecordCompacted.
	Read(ctx context.Context, offset uint64) (Record, error)

	// ReadBatch reads up to count records starting at start. It can
	// return fewer than count records when compaction removed records
	// inside the range.
	ReadBatch(ctx context.Context, start uint64, count int) ([]Record, error)

	// Fetch reads records at offset, waiting up to maxWait for data to
	// arrive. With maxWait of zero or less the call is non-blocking:
	// records at once, or ErrNoData.
	//
	// This is the long-poll primitive: pass a generous maxWait and the
	// call parks server-side until records appear or the wait elapses.
	Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Record, error)

	// Stream yields every record from start that exists today, then
	// stops. Use it to catch up on history without waiting for more.
	//
	// The iteration ends cleanly when the queue is caught up. A read
	// error yields a zero Record with the error and stops.
	Stream(ctx context.Context, start uint64) iter.Seq2[Record, error]

	// Tail yields the records from start, then keeps waiting for new
	// ones, until ctx is cancelled or an error occurs. It is Stream
	// plus patience: the follow loop for live consumers.
	Tail(ctx context.Context, start uint64) iter.Seq2[Record, error]

	// LastOffset returns the offset the next appended record will
	// receive. The readable range of the queue is [MinOffset,
	// LastOffset).
	LastOffset(ctx context.Context) (uint64, error)

	// MinOffset returns the oldest readable offset. It stays zero until
	// retention or Truncate removes history.
	MinOffset(ctx context.Context) (uint64, error)

	// Group returns the consumer group handle for name, creating the
	// group on first use. The group's position only reaches disk when
	// it is committed; see Group.
	Group(name string) Group
}

// Group tracks how far one named consumer has read a log queue.
//
// A group is just a position: Mark records progress locally, Commit
// makes it durable, and Committed reads it back. After a crash or a
// restart, the consumer resumes from Committed and replays anything
// past it. Choose the mark-and-commit points to match what your
// processing can tolerate twice: commit early and you might skip work
// that half finished; commit late and you might repeat it.
//
// Groups are cheap handles keyed by name. Two handles for the same name
// see the same position, so treat the name as the unit of coordination.
type Group interface {
	io.Closer

	// Name returns the group's name.
	Name() string

	// Mark records that everything up to and including offset has been
	// processed. Marks move forward only; a lower mark never drags the
	// position back.
	Mark(offset uint64)

	// Commit persists the group's current position. Committing after
	// every record is safe but chatty; most consumers commit in batches
	// or on a timer.
	Commit(ctx context.Context) error

	// Committed returns the last persisted position.
	Committed(ctx context.Context) (uint64, error)

	// Close commits the final position and releases the handle. It is
	// safe to call more than once.
	Close() error
}
