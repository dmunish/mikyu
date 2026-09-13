package engine

import "time"

// Message is the internal engine‑level message representation.
type Message struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Payload   []byte
}
