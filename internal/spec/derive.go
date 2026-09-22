package spec

import "time"

// RecordOverhead is the per-record cost a payload carries beyond its own length.
const RecordOverhead = 36

// Built-in values Resolve falls back to for every unset field.
const (
	builtinMaxSegment    = int64(64 << 20)
	builtinMaxSegmentAge = Duration(time.Hour)
	builtinMaxPayload    = int64(1 << 20)
	builtinMaxCache      = int64(32 << 20)
	builtinMaxFlushBytes = int64(1 << 20)
	builtinMaxFlushWait  = Duration(10 * time.Millisecond)
	builtinLease         = Duration(30 * time.Second)
	builtinMaxAttempts   = uint32(5)
	builtinMaxInFlight   = uint32(100_000)
)

// Resolve fills every derived field on s that was left nil. It does not validate; call ValidateCreate or ValidateUpdate afterwards.
func Resolve(s Spec) Spec {
	if s.Storage.MaxSegment == nil {
		s.Storage.MaxSegment = new(builtinMaxSegment)
	}
	if s.Storage.MaxSegmentAge == nil {
		s.Storage.MaxSegmentAge = new(builtinMaxSegmentAge)
	}
	if s.Storage.MaxPayload == nil {
		derived := min(builtinMaxPayload, *s.Storage.MaxSegment-RecordOverhead)
		s.Storage.MaxPayload = new(derived)
	}
	if s.Storage.MaxCache == nil {
		derived := min(builtinMaxCache, *s.Storage.MaxSegment*4)
		s.Storage.MaxCache = new(derived)
	}
	if s.Storage.Durability == nil {
		s.Storage.Durability = new(DurabilitySync)
	}
	if s.Storage.Encryption == nil {
		s.Storage.Encryption = new(EncryptionNone)
	}
	if s.Storage.Compaction == nil {
		s.Storage.Compaction = new(CompactionNone)
	}
	if s.Flush.MaxBytes == nil {
		s.Flush.MaxBytes = new(builtinMaxFlushBytes)
	}
	if s.Flush.MaxWait == nil {
		s.Flush.MaxWait = new(builtinMaxFlushWait)
	}
	if s.Retention.MaxAge == nil {
		s.Retention.MaxAge = new(Duration(0))
	}
	if s.Retention.MaxBytes == nil {
		s.Retention.MaxBytes = new(int64(0))
	}
	if s.Task.Lease == nil {
		s.Task.Lease = new(builtinLease)
	}
	if s.Task.MaxAttempts == nil {
		s.Task.MaxAttempts = new(builtinMaxAttempts)
	}
	if s.Task.MaxInFlight == nil {
		s.Task.MaxInFlight = new(builtinMaxInFlight)
	}
	if s.Task.MaxDeadLetters == nil {
		s.Task.MaxDeadLetters = new(*s.Task.MaxInFlight)
	}
	return s
}

// FlushLinger returns the adaptive linger derived from max_flush_wait.
func FlushLinger(s Spec) time.Duration {
	wait := builtinMaxFlushWait
	if s.Flush.MaxWait != nil {
		wait = *s.Flush.MaxWait
	}
	return max(time.Duration(0), wait.Duration()/8)
}
