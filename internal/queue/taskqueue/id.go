package taskqueue

import (
	"crypto/rand"
	"encoding/hex"
)

// newTaskID returns a random 32-character hex identifier.
// crypto/rand.Read never fails on Go 1.24+, so no error is returned.
func newTaskID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
