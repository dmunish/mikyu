// Package mikyu is the public SDK for the Mikyu message queue.
//
// # Overview
//
// Mikyu is a durable, embedded-first message queue. You point it at a
// directory and get queues that survive restarts, crashes, and reboots.
// The same API works in two modes:
//
//   - Embedded: the queues live inside your process, the way SQLite does.
//     Open gives you this.
//   - Remote: the queues live in a separate daemon that your process
//     talks to over TCP, a Unix socket, or a Windows named pipe.
//     Dial gives you this.
//
// Both entry points return a Client, so the rest of your code cannot
// tell which mode it is running in. Move from embedded to a daemon and
// back without rewriting anything.
//
// # Queues
//
// There are two kinds of queues, and the kind is part of where a queue
// lives on disk. log/events and task/events are two different queues
// that happen to share a name.
//
// A log queue is an append-only stream. Producers append records,
// consumers read from any offset or follow the tail, and consumer
// groups track their own progress. Reach for one when you want replay,
// streaming, or a durable history.
//
// A task queue is work waiting to be done. A dequeued task comes with a
// visibility lease. If the worker crashes, the lease expires and the
// task is delivered again. Ack when the work is done, or let the
// attempt budget run out and the task lands in a durable dead-letter
// queue. Reach for one when you want at-least-once processing.
//
// # Getting started
//
// Open a directory, open a queue, use it:
//
//	client, err := mikyu.Open("./data")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	events, err := client.OpenLog(ctx, "events")
//	if err != nil {
//		log.Fatal(err)
//	}
//	offset, err := events.Append(ctx, []byte("hello"))
//
// OpenLog finds an existing queue or creates one, which is the call
// most applications want. CreateLog refuses to create over an existing
// queue, which matters when many machines declare the same queue and
// you need to know if two of them disagreed. Every accessor comes in
// both flavors; see QueueAccess for the full picture.
//
// # Configuration
//
// Queue settings form layers, from most specific to least:
//
//	call options, the stored spec, session defaults, built-in defaults
//
// Each layer only fills what the layer above left unset, so options
// describe a sparse patch, never a total. Call options apply when a
// queue is created; some settings are create-only and can never change
// after that (durability, encryption, compaction, the segment and
// payload contracts). Mutable settings change later through
// Client.UpdateQueue. See the Spec type and the With constructors in
// this package.
//
// # Options
//
// The With constructors follow one naming rule, so the prefix tells you
// what a setting applies to:
//
//   - No prefix: a queue-level setting (WithDurability, WithLease).
//   - Task prefix: a per-task value at enqueue time (WithTaskLease).
//   - Key and ScheduledAt: the remaining per-task values.
//   - Record prefix: a per-record value at append time (WithRecordKey).
//   - RetryAfter: the per-nack delay.
//
// Where the same concept exists at two layers, the queue-level option
// is the default and the per-call option overrides it: WithLease sets
// the queue's fallback lease, WithTaskLease sets one task's.
//
// # Errors
//
// Every failure you can program against is a stable error value
// exported from this package. Compare with errors.Is so wrapping is
// respected:
//
//	if errors.Is(err, mikyu.ErrQueueNotFound) { ... }
//
// The Is helpers (IsNotFound, IsStale, and friends) group sentinels
// that callers usually treat the same way.
package mikyu
