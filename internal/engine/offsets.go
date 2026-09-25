package engine

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/dmunish/mikyu/internal/metrics"
)

const (
	managerOffsetFileName   = "manager-offsets.dat"
	managerOffsetTmpName    = "manager-offsets.dat.tmp"
	compactAppendsThreshold = 5000
)

// OffsetStore is a manager-level offset persistence store.
type OffsetStore struct {
	mu                  *metrics.RWLockObserver
	root                *os.Root
	file                *os.File
	w                   *bufio.Writer
	offsets             map[string]uint64
	appendsSinceCompact int
}

func openOffsetStore(dir string) (*OffsetStore, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	f, err := root.OpenFile(managerOffsetFileName, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	s := &OffsetStore{
		mu:      metrics.NewRWLockObserver("engine.offsets", nil),
		root:    root,
		file:    f,
		w:       bufio.NewWriter(f),
		offsets: make(map[string]uint64),
	}
	metrics.RegisterLockObserver(s.mu)
	if err := s.recover(); err != nil {
		_ = f.Close()
		_ = root.Close()
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

// compactLocked atomically rewrites the offset file via a temp file and rename; it runs under s.mu and is the only whole-file writer.
func (s *OffsetStore) compactLocked() error {
	f, err := s.root.OpenFile(managerOffsetTmpName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	for key, off := range s.offsets {
		keyBytes := []byte(key)
		var hdr [2]byte
		binary.BigEndian.PutUint16(hdr[:], uint16(len(keyBytes)))
		if _, err := w.Write(hdr[:]); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			_ = f.Close()
			return err
		}
		var offBuf [8]byte
		binary.BigEndian.PutUint64(offBuf[:], off)
		if _, err := w.Write(offBuf[:]); err != nil {
			_ = f.Close()
			return err
		}
	}

	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}

	// A rename through the root replaces the existing target file.
	if err := s.root.Rename(managerOffsetTmpName, managerOffsetFileName); err != nil {
		return err
	}
	if err := syncDir(s.root); err != nil {
		return err
	}

	newFile, err := s.root.OpenFile(managerOffsetFileName, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.file = newFile
	s.w = bufio.NewWriter(newFile)
	s.appendsSinceCompact = 0
	return nil
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
	if err := s.file.Sync(); err != nil {
		return err
	}

	s.appendsSinceCompact++
	if s.appendsSinceCompact >= compactAppendsThreshold {
		return s.compactLocked()
	}
	return nil
}

// Size returns the offset store file size in bytes.
func (s *OffsetStore) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return 0
	}
	info, err := s.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// Fetch returns the committed offset for a queue/group pair.
func (s *OffsetStore) Fetch(queueName, group string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[queueName+":"+group]
}

func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if s.file != nil {
		if s.w != nil {
			if ferr := s.w.Flush(); ferr != nil {
				err = ferr
			}
		}
		if cerr := s.file.Close(); err == nil {
			err = cerr
		}
		s.file = nil
		s.w = nil
	}
	if s.root != nil {
		if rerr := s.root.Close(); err == nil {
			err = rerr
		}
		s.root = nil
	}
	return err
}