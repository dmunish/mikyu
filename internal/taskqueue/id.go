package taskqueue

import (
	"crypto/rand"
	"encoding/hex"
)

func newTaskID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
