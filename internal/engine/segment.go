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

func openSegment(dir string, baseOff uint64, preallocSize int64) (*segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOff))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.idx", baseOff))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	if info, err := logFile.Stat(); err == nil && info.Size() == 0 && preallocSize > 0 {
		_ = logFile.Truncate(preallocSize)
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
		if info, err := s.logFile.Stat(); err == nil && info.Size() > 0 {
			mf, err := os.Open(s.logFile.Name())
			if err != nil {
				return err
			}
			m, err := mmap.Map(mf, mmap.RDONLY, 0)
			_ = mf.Close()
			if err != nil {
				return err
			}
			s.mmapData = m
		}
	}
	if s.idxMMap == nil {
		if info, err := s.idxFile.Stat(); err == nil && info.Size() > 0 {
			mf, err := os.Open(s.idxFile.Name())
			if err != nil {
				return err
			}
			m, err := mmap.Map(mf, mmap.RDONLY, 0)
			_ = mf.Close()
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
	if lastEntry.pos+lastEntry.len > logSize {
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
	s.pos = lastEntry.pos + lastEntry.len
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
		if bodyLen == 0 {
			break // Stop at unwritten pre-allocated zeroes
		}
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
	// Truncate the log file to remove any torn/written-beyond-last-valid bytes
	if err := s.logFile.Truncate(s.pos); err != nil {
		return nil, err
	}
	if err := s.logFile.Sync(); err != nil {
		return nil, err
	}
	if err := s.idxFile.Sync(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *segment) diskSize() int64 {
	var total int64
	if s.logFile != nil {
		if info, err := s.logFile.Stat(); err == nil {
			total += info.Size()
		}
	}
	if s.idxFile != nil {
		if info, err := s.idxFile.Stat(); err == nil {
			total += info.Size()
		}
	}
	return total
}

func (s *segment) nextOffset() uint64 {
	if len(s.idxMMap) > 0 {
		return s.baseOff + uint64(len(s.idxMMap)/rawIndexEntrySize)
	}
	if s.idxFile != nil {
		if info, err := s.idxFile.Stat(); err == nil && info.Size() > 0 {
			return s.baseOff + uint64(info.Size()/rawIndexEntrySize)
		}
	}
	return s.baseOff
}

func (s *segment) destroy() (int64, error) {
	logPath := s.logFile.Name()
	idxPath := s.idxFile.Name()

	freed := s.diskSize()

	// 1. First unmap memory regions and close file handles
	_ = s.Close()

	// 2. Unlink physical files from disk
	err1 := os.Remove(logPath)
	err2 := os.Remove(idxPath)

	if err1 != nil && !os.IsNotExist(err1) {
		return freed, err1
	}
	if err2 != nil && !os.IsNotExist(err2) {
		return freed, err2
	}
	return freed, nil
}

func (s *segment) Close() error {
	// Trim unused pre-allocated capacity to match actual written bytes
	if s.logFile != nil && s.pos >= 0 {
		_ = s.logFile.Truncate(s.pos)
		_ = s.logFile.Sync()
	}

	var mmapErr error
	if s.mmapData != nil {
		mmapErr = s.mmapData.Unmap()
		s.mmapData = nil
	}
	if s.idxMMap != nil {
		if err := s.idxMMap.Unmap(); err != nil && mmapErr == nil {
			mmapErr = err
		}
		s.idxMMap = nil
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
