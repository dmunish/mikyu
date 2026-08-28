package queue

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type rawIndexEntry struct {
	pos int64
	len int64
}

type segment struct {
	baseOff   uint64
	logFile   *os.File
	idxFile   *os.File
	pos       int64
	createdAt time.Time
}

func openSegment(dir string, baseOff uint64) (*segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOff))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.idx", baseOff))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		logFile.Close()
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

func (s *segment) writeIndex(pos, len int64) error {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(pos))
	binary.BigEndian.PutUint64(buf[8:16], uint64(len))
	_, err := s.idxFile.Write(buf[:])
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

	logSize := logInfo.Size()
	idxSize := idxInfo.Size()

	// Fast Path: Check if valid index exists and matches log file size
	if idxSize > 0 && idxSize%16 == 0 {
		var lastBuf [16]byte
		if _, err := s.idxFile.ReadAt(lastBuf[:], idxSize-16); err == nil {
			lastPos := int64(binary.BigEndian.Uint64(lastBuf[0:8]))
			lastLen := int64(binary.BigEndian.Uint64(lastBuf[8:16]))
			if lastPos+lastLen == logSize {
				buf := make([]byte, idxSize)
				if _, err := s.idxFile.ReadAt(buf, 0); err == nil {
					entries := make([]rawIndexEntry, idxSize/16)
					for i := range entries {
						entries[i] = rawIndexEntry{
							pos: int64(binary.BigEndian.Uint64(buf[i*16 : i*16+8])),
							len: int64(binary.BigEndian.Uint64(buf[i*16+8 : i*16+16])),
						}
					}
					s.pos = logSize
					return entries, nil
				}
			}
		}
	}

	// Fallback: Rebuild index from log
	return s.rebuild()
}

func (s *segment) rebuild() ([]rawIndexEntry, error) {
	_ = s.idxFile.Truncate(0)

	var entries []rawIndexEntry
	var pos int64

	for {
		var lenBuf [4]byte
		n, _ := s.logFile.ReadAt(lenBuf[:], pos)
		if n < 4 {
			break
		}

		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		total := int64(4) + int64(bodyLen)

		body := make([]byte, bodyLen)
		n, _ = s.logFile.ReadAt(body, pos+4)
		if int64(n) < int64(bodyLen) {
			break // torn write tail
		}

		if _, _, err := decodeRecord(body); err != nil {
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
	err1 := s.logFile.Close()
	err2 := s.idxFile.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
