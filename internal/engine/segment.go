package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/edsrzf/mmap-go"
)

type segment struct {
	baseOff   uint64
	logFile   *os.File
	idxFile   *os.File
	pos       int64
	createdAt time.Time
	mmapData  mmap.MMap
	idxMMap   mmap.MMap // Memory-mapped index region for closed historical segment
}

func openSegment(dir string, baseOff uint64) (*segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOff))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.idx", baseOff))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	if info, err := logFile.Stat(); err == nil && info.Size() == 0 {
		_ = logFile.Truncate(64 * 1024 * 1024) // Reserves 64 MiB
	}

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	createdAt := time.Now()
	if info, err := logFile.Stat(); err == nil {
		createdAt = info.ModTime()
	}

	return &segment{
		baseOff:   baseOff,
		logFile:   logFile,
		idxFile:   idxFile,
		createdAt: createdAt,
	}, nil
}

// Map the segment into read-only memory once closed for production zero-copy reads.
func (s *segment) enableMMap() error {
	if s.mmapData == nil {
		m, err := mmap.Map(s.logFile, mmap.RDONLY, 0)
		if err != nil {
			return err
		}
		s.mmapData = m
	}
	if s.idxMMap == nil {
		if info, err := s.idxFile.Stat(); err == nil && info.Size() > 0 {
			m, err := mmap.Map(s.idxFile, mmap.RDONLY, 0)
			if err != nil {
				return err
			}
			s.idxMMap = m
		}
	}
	return nil
}

func (s *segment) readIndexEntry(offset uint64) (pos int64, length int64, err error) {
	idxPos := int64(offset-s.baseOff) * rawIndexEntrySize
	var buf [rawIndexEntrySize]byte

	// Zero-copy read from mapped .idx file if historical, otherwise syscall ReadAt
	if len(s.idxMMap) >= int(idxPos+rawIndexEntrySize) {
		copy(buf[:], s.idxMMap[idxPos:idxPos+rawIndexEntrySize])
	} else if _, err := s.idxFile.ReadAt(buf[:], idxPos); err != nil {
		return 0, 0, err
	}

	entry := UnmarshalRawIndexEntry(buf[:])
	if entry.len < (minRecordSize - recordLengthSize) {
		return 0, 0, ErrOffsetNotFound
	}
	return entry.pos, entry.len, nil
}

func (s *segment) writeIndex(pos, len int64) error {
	var buf [rawIndexEntrySize]byte
	rawIndexEntry{pos: pos, len: len}.MarshalBinary(&buf)
	_, err := s.idxFile.Write(buf[:])
	return err
}

func (s *segment) writeIndexAt(idxPos, pos, len int64) error {
	var buf [rawIndexEntrySize]byte
	rawIndexEntry{pos: pos, len: len}.MarshalBinary(&buf)
	_, err := s.idxFile.WriteAt(buf[:], idxPos)
	return err
}

func (s *segment) recover() ([]rawIndexEntry, error) {
	logInfo, err := s.logFile.Stat()
	if err != nil {
		return nil, err
	}
	idxInfo, err := s.idxFile.Stat()
	if err != nil {
		return nil, err
	}

	if entries, ok := s.tryFastRecovery(logInfo.Size(), idxInfo.Size()); ok {
		return entries, nil
	}

	return s.rebuild()
}

func (s *segment) tryFastRecovery(logSize, idxSize int64) ([]rawIndexEntry, bool) {
	if idxSize == 0 || idxSize%rawIndexEntrySize != 0 {
		return nil, false
	}

	var lastBuf [rawIndexEntrySize]byte
	if _, err := s.idxFile.ReadAt(lastBuf[:], idxSize-rawIndexEntrySize); err != nil {
		return nil, false
	}

	lastEntry := UnmarshalRawIndexEntry(lastBuf[:])
	if lastEntry.pos+lastEntry.len != logSize {
		return nil, false
	}

	buf := make([]byte, idxSize)
	if _, err := s.idxFile.ReadAt(buf, 0); err != nil {
		return nil, false
	}

	entries := make([]rawIndexEntry, idxSize/rawIndexEntrySize)
	for i := range entries {
		entries[i] = UnmarshalRawIndexEntry(buf[i*rawIndexEntrySize : (i+1)*rawIndexEntrySize])
	}
	s.pos = logSize
	return entries, true
}

func (s *segment) rebuild() ([]rawIndexEntry, error) {
	_ = s.idxFile.Truncate(0)

	var entries []rawIndexEntry
	var pos int64

	for {
		var lenBuf [recordLengthSize]byte
		n, _ := s.logFile.ReadAt(lenBuf[:], pos)
		if n < recordLengthSize {
			break
		}

		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		total := int64(recordLengthSize) + int64(bodyLen)

		body := make([]byte, bodyLen)
		n, _ = s.logFile.ReadAt(body, pos+int64(recordLengthSize))
		if int64(n) < int64(bodyLen) {
			break // torn write tail
		}

		if _, _, _, err := decodeRecord(body); err != nil {
			break // checksum mismatch
		}

		entries = append(entries, rawIndexEntry{pos: pos, len: total})
		_ = s.writeIndex(pos, total)
		pos += total
	}

	s.pos = pos
	_ = s.logFile.Truncate(pos)
	_ = s.idxFile.Sync()
	return entries, nil
}

func (s *segment) Close() error {
	var mmapErr error
	if s.mmapData != nil {
		mmapErr = s.mmapData.Unmap()
		s.mmapData = nil
	}

	err1 := s.logFile.Close()
	err2 := s.idxFile.Close()

	if mmapErr != nil {
		return mmapErr
	}
	if err1 != nil {
		return err1
	}
	return err2
}
