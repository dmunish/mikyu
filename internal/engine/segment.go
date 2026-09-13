package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

type segment struct {
	baseOff      uint64
	dir          string // directory containing this segment's files
	logName      string // .log file name, relative to dir
	idxName      string // .idx file name, relative to dir
	logFile      *os.File
	idxFile      *os.File
	pos          int64
	preallocSize int64
	createdAt    time.Time
	mmapData     mmap.MMap
	idxMMap      mmap.MMap // Memory-mapped index region for closed historical segment
}

// openSegment creates or opens the segment files for baseOff inside dir.
// The files are opened through an os.Root scoped to dir, so segment paths
// can never escape the queue directory.
func openSegment(dir string, baseOff uint64, preallocSize int64) (*segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOff)
	idxName := fmt.Sprintf("%020d.idx", baseOff)

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	logFile, err := root.OpenFile(logName, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	if info, err := logFile.Stat(); err == nil && info.Size() == 0 && preallocSize > 0 {
		_ = logFile.Truncate(preallocSize)
	}

	idxFile, err := root.OpenFile(idxName, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	createdAt := time.Now()
	if info, err := logFile.Stat(); err == nil {
		createdAt = info.ModTime()
	}

	return &segment{
		baseOff:      baseOff,
		dir:          dir,
		logName:      logName,
		idxName:      idxName,
		logFile:      logFile,
		idxFile:      idxFile,
		preallocSize: preallocSize,
		createdAt:    createdAt,
	}, nil
}

// Map the segment into read-only memory once closed for production zero-copy reads.
// The extra open handles are obtained through an os.Root scoped to the segment
// directory because the segment's own handles may have been opened relative to
// a root (see openSegment).
func (s *segment) enableMMap() error {
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return err
	}
	defer root.Close()

	if s.mmapData == nil {
		if info, err := s.logFile.Stat(); err == nil && info.Size() > 0 {
			mf, err := root.Open(s.logName)
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
			mf, err := root.Open(s.idxName)
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
	// Restore preallocated capacity if active segment contains fewer bytes than prealloc target
	if s.preallocSize > s.pos {
		_ = s.logFile.Truncate(s.preallocSize)
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

// lastTimestamp returns the timestamp of the segment's newest record. ok is
// false when the segment holds no records or its tail cannot be read.
func (s *segment) lastTimestamp() (time.Time, bool) {
	count := 0
	if len(s.idxMMap) > 0 {
		count = len(s.idxMMap) / rawIndexEntrySize
	} else if s.idxFile != nil {
		if info, err := s.idxFile.Stat(); err == nil {
			count = int(info.Size() / rawIndexEntrySize)
		}
	}
	if count == 0 {
		return time.Time{}, false
	}

	idxPos := int64(count-1) * rawIndexEntrySize
	var buf [rawIndexEntrySize]byte
	if len(s.idxMMap) >= int(idxPos+rawIndexEntrySize) {
		copy(buf[:], s.idxMMap[idxPos:idxPos+rawIndexEntrySize])
	} else if _, err := s.idxFile.ReadAt(buf[:], idxPos); err != nil {
		return time.Time{}, false
	}
	entry := UnmarshalRawIndexEntry(buf[:])
	if entry.len < minRecordSize {
		return time.Time{}, false
	}

	rec := make([]byte, entry.len)
	if len(s.mmapData) >= int(entry.pos+entry.len) {
		copy(rec, s.mmapData[entry.pos:entry.pos+entry.len])
	} else if _, err := s.logFile.ReadAt(rec, entry.pos); err != nil {
		return time.Time{}, false
	}
	_, ts, _, err := decodeRecord(rec[recordLengthSize:])
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func (s *segment) destroy() (int64, error) {
	freed := s.diskSize()

	// 1. First unmap memory regions and close file handles
	_ = s.Close()

	// 2. Unlink physical files through an os.Root scoped to the segment
	// directory so no path can escape it.
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return freed, err
	}
	defer root.Close()

	err1 := root.Remove(s.logName)
	err2 := root.Remove(s.idxName)

	if err1 != nil && !errors.Is(err1, fs.ErrNotExist) {
		return freed, err1
	}
	if err2 != nil && !errors.Is(err2, fs.ErrNotExist) {
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
