package engine

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const managerOffsetFileName = "manager-offsets.dat"

// OffsetStore is a manager-level offset persistence store.
// A single file tracks consumer group offsets across all queues to avoid inode exhaustion.
type OffsetStore struct {
	mu      sync.RWMutex
	file    *os.File
	w       *bufio.Writer
	offsets map[string]uint64
}

func openOffsetStore(dir string) (*OffsetStore, error) {
	path := filepath.Join(dir, managerOffsetFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &OffsetStore{
		file:    f,
		w:       bufio.NewWriter(f),
		offsets: make(map[string]uint64),
	}
	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *OffsetStore) recover() error {
	for {
		var hdr [2]byte
		if _, err := io.ReadFull(s.file, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		keyLen := binary.BigEndian.Uint16(hdr[:])
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(s.file, keyBytes); err != nil {
			return nil // torn record tail
		}
		var offBuf [8]byte
		if _, err := io.ReadFull(s.file, offBuf[:]); err != nil {
			return nil
		}
		s.offsets[string(keyBytes)] = binary.BigEndian.Uint64(offBuf[:])
	}
}

func (s *OffsetStore) Commit(queueName, group string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := queueName + ":" + group
	s.offsets[key] = offset

	keyBytes := []byte(key)
	var hdr [2]byte
	binary.BigEndian.PutUint16(hdr[:], uint16(len(keyBytes)))

	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(keyBytes); err != nil {
		return err
	}
	var offBuf [8]byte
	binary.BigEndian.PutUint64(offBuf[:], offset)
	if _, err := s.w.Write(offBuf[:]); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *OffsetStore) Fetch(queueName, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[queueName+":"+group]
}

func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.w.Flush()
	return s.file.Close()
}
