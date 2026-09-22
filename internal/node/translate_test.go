package node

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
)

// TestEngineConfigFromSpec covers each spec field's pass-through into the engine config.
func TestEngineConfigFromSpec(t *testing.T) {
	var s spec.Spec
	s.Storage.Durability = new(spec.DurabilityAsync)
	s.Storage.Encryption = new(spec.EncryptionOS)
	s.Storage.Compaction = new(spec.CompactionKeys)
	s.Storage.MaxSegment = new(int64(32 << 20))
	s.Storage.MaxSegmentAge = new(spec.Duration(2 * time.Hour))
	s.Storage.MaxPayload = new(int64(4096))
	s.Storage.MaxCache = new(int64(8 << 20))
	s.Flush.MaxBytes = new(int64(128 << 10))
	s.Flush.MaxWait = new(spec.Duration(20 * time.Millisecond))

	cfg := toEngineConfig(spec.Resolve(s))
	assert.Equal(t, spec.DurabilityAsync, cfg.Durability)
	assert.Equal(t, spec.EncryptionOS, cfg.Encryption)
	assert.Equal(t, spec.CompactionKeys, cfg.Compaction)
	assert.Equal(t, int64(32<<20), cfg.MaxSegment)
	assert.Equal(t, 2*time.Hour, cfg.MaxSegmentAge)
	assert.Equal(t, int64(4096), cfg.MaxPayload)
	assert.Equal(t, int64(8<<20), cfg.MaxCache)
	assert.Equal(t, int64(128<<10), cfg.MaxFlushBytes)
	assert.Equal(t, 20*time.Millisecond, cfg.MaxFlushWait)
	assert.Equal(t, 20*time.Millisecond/8, cfg.FlushLinger)
}

// TestEngineConfigFromSparseSpec derives the unset fields.
func TestEngineConfigFromSparseSpec(t *testing.T) {
	cfg := toEngineConfig(spec.Builtin())
	assert.Equal(t, spec.DurabilitySync, cfg.Durability)
	assert.Equal(t, spec.EncryptionNone, cfg.Encryption)
	assert.Equal(t, spec.CompactionNone, cfg.Compaction)
	assert.Equal(t, int64(64<<20), cfg.MaxSegment)
	assert.Equal(t, time.Hour, cfg.MaxSegmentAge)
	assert.Equal(t, int64(1<<20), cfg.MaxPayload)
	assert.Equal(t, int64(32<<20), cfg.MaxCache)
	assert.Equal(t, int64(1<<20), cfg.MaxFlushBytes)
	assert.Equal(t, 10*time.Millisecond, cfg.MaxFlushWait)
	assert.Equal(t, 1250*time.Microsecond, cfg.FlushLinger)
}

// TestTaskDefaultsFromSpec pins the task block translation.
func TestTaskDefaultsFromSpec(t *testing.T) {
	d := taskDefaultsFor(spec.Builtin())
	assert.Equal(t, 30*time.Second, d.Visibility)
	assert.Equal(t, uint32(5), d.MaxAttempts)
	assert.Equal(t, uint32(100_000), d.MaxInFlight)
	assert.Equal(t, uint32(100_000), d.MaxDLQ)
}

// TestPolicyFromSpec pins the retention translation.
func TestPolicyFromSpec(t *testing.T) {
	p := policyFor(spec.Builtin())
	assert.Zero(t, p.Age)
	assert.Zero(t, p.Bytes)
}
