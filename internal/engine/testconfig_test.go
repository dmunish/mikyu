package engine

import (
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// testConfig returns a fully-resolved Config matching spec.Builtin; tests mutate fields from here.
func testConfig() Config {
	return Config{
		Durability:    spec.DurabilitySync,
		Encryption:    spec.EncryptionNone,
		Compaction:    spec.CompactionNone,
		MaxSegment:    64 << 20,
		MaxSegmentAge: time.Hour,
		MaxPayload:    1 << 20,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  10 * time.Millisecond,
		FlushLinger:   1250 * time.Microsecond,
	}
}
