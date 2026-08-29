package queue

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type OffsetStore struct {
	mu      sync.RWMutex
	path    string
	file    *os.File
	offsets map[string]uint64
}

func OpenOffsetStore(dir string) (*OffsetStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "offsets.dat")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	s := &OffsetStore{
		path:    path,
		file:    f,
		offsets: make(map[string]uint64),
	}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *OffsetStore) recover() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		var lenBuf [2]byte
		if _, err := io.ReadFull(s.file, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		groupLen := binary.BigEndian.Uint16(lenBuf[:])
		groupBytes := make([]byte, groupLen)
		if _, err := io.ReadFull(s.file, groupBytes); err != nil {
			return err
		}
		var offBuf [8]byte
		if _, err := io.ReadFull(s.file, offBuf[:]); err != nil {
			return err
		}
		s.offsets[string(groupBytes)] = binary.BigEndian.Uint64(offBuf[:])
	}
}

func (s *OffsetStore) Fetch(group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[group]
}

func (s *OffsetStore) Commit(group string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev, ok := s.offsets[group]
	if ok && offset <= prev {
		return nil
	}
	s.offsets[group] = offset

	gBytes := []byte(group)
	var prefix [2]byte
	binary.BigEndian.PutUint16(prefix[:], uint16(len(gBytes)))
	if _, err := s.file.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := s.file.Write(gBytes); err != nil {
		return err
	}
	var offBuf [8]byte
	binary.BigEndian.PutUint64(offBuf[:], offset)
	if _, err := s.file.Write(offBuf[:]); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
