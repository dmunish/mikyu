package queue

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// OffsetStore persists consumer-group progress (group -> next offset to read)
// as an append-only log. Each record is crash-safe via fsync before the
// producing call returns, mirroring the durability contract of Produce.
type OffsetStore struct {
	mu      sync.RWMutex
	file    *os.File
	offsets map[string]uint64
}

const offsetFileName = "offsets.dat"

func OpenOffsetStore(dir string) (*OffsetStore, error) {
	path := filepath.Join(dir, offsetFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &OffsetStore{file: f, offsets: make(map[string]uint64)}
	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// Record format: [uint16 groupLen | groupBytes | uint64 offset]
func (s *OffsetStore) recover() error {
	for {
		var hdr [2]byte
		if _, err := io.ReadFull(s.file, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // graceful: truncated header means clean end-of-file
			}
			return err
		}
		groupLen := binary.BigEndian.Uint16(hdr[:])
		groupBytes := make([]byte, groupLen)
		if _, err := io.ReadFull(s.file, groupBytes); err != nil {
			return nil // torn record: stop replay, ignore tail
		}
		var offBuf [8]byte
		if _, err := io.ReadFull(s.file, offBuf[:]); err != nil {
			return nil // torn record: stop replay
		}
		s.offsets[string(groupBytes)] = binary.BigEndian.Uint64(offBuf[:])
	}
}

func (s *OffsetStore) Commit(group string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.offsets[group] = offset

	gBytes := []byte(group)
	var hdr [10]byte
	binary.BigEndian.PutUint16(hdr[0:2], uint16(len(gBytes)))
	copy(hdr[2:2+len(gBytes)], gBytes)
	binary.BigEndian.PutUint64(hdr[2+len(gBytes):], offset)

	if _, err := s.file.Write(hdr[:2+len(gBytes)+8]); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *OffsetStore) Fetch(group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[group]
}

func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
