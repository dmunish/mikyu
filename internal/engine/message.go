package engine

import "time"

// Record is the internal engine-level message representation.
type Record struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Group     []byte
	Payload   []byte
}
