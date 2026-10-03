package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
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
	mapMu        sync.RWMutex
	mmapData     mmap.MMap
	idxMMap      mmap.MMap // Memory-mapped index region for closed historical segment
}

// openSegment creates or opens the segment files for baseOff inside dir.
// The file names are generated, so direct path opens cannot escape the queue
// directory; the os.Root is still used for the directory-sync durability site.
func openSegment(dir string, baseOff uint64, preallocSize int64) (*segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOff)
	idxName := fmt.Sprintf("%020d.idx", baseOff)

	logCreated := false
	if _, err := os.Stat(filepath.Join(dir, logName)); errors.Is(err, fs.ErrNotExist) {
		logCreated = true
	}
	logFile, err := openSegmentFile(filepath.Join(dir, logName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	if info, err := logFile.Stat(); err == nil && info.Size() == 0 && preallocSize > 0 {
		if err := logFile.Truncate(preallocSize); err != nil {
			_ = logFile.Close()
			return nil, err
		}
	}

	idxCreated := false
	if _, err := os.Stat(filepath.Join(dir, idxName)); errors.Is(err, fs.ErrNotExist) {
		idxCreated = true
	}
	idxFile, err := openSegmentFile(filepath.Join(dir, idxName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	if logCreated || idxCreated {
		root, err := os.OpenRoot(dir)
		if err != nil {
			_ = logFile.Close()
			_ = idxFile.Close()
			return nil, err
		}
		defer root.Close()
		if err := syncDir(root); err != nil {
			_ = logFile.Close()
			_ = idxFile.Close()
			return nil, err
		}
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

// views returns the segment's current log and index mappings.
func (s *segment) views() (logData, idxData mmap.MMap) {
	s.mapMu.RLock()
	defer s.mapMu.RUnlock()
	return s.mmapData, s.idxMMap
}

// Map the segment into read-only memory once closed for production zero-copy reads.
// The extra open handles are obtained through an os.Root scoped to the segment
// directory because the segment's own handles may have been opened relative to
// a root (see openSegment).
func (s *segment) enableMMap() error {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
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
	_, idxData := s.views()
	if len(idxData) >= int(idxPos+rawIndexEntrySize) {
		copy(buf[:], idxData[idxPos:idxPos+rawIndexEntrySize])
	} else if _, err := s.idxFile.ReadAt(buf[:], idxPos); err != nil {
		return 0, 0, err
	}

	entry := UnmarshalRawIndexEntry(buf[:])
	if entry.len < (minRecordSize - recordLengthSize) {
		return 0, 0, ErrOffsetNotFound
	}
	return entry.pos, entry.len, nil
}

// writeIndex appends an index entry during recovery, before the WAL flusher starts.
func (s *segment) writeIndex(pos, len int64) error {
	var buf [rawIndexEntrySize]byte
	rawIndexEntry{pos: pos, len: len}.MarshalBinary(&buf)
	_, err := s.idxFile.Write(buf[:])
	return err
}

// indexWriteHook, when non-nil, receives every live index write's offset and length so tests can assert batching.
var indexWriteHook func(off int64, n int)

// writeIndexAt writes an index entry; only the WAL flusher calls it on a live segment.
func (s *segment) writeIndexAt(idxPos, pos, len int64) error {
	var buf [rawIndexEntrySize]byte
	rawIndexEntry{pos: pos, len: len}.MarshalBinary(&buf)
	if indexWriteHook != nil {
		indexWriteHook(idxPos, rawIndexEntrySize)
	}
	_, err := s.idxFile.WriteAt(buf[:], idxPos)
	return err
}

// writeIndexBatchAt writes consecutive index entries in one syscall.
func (s *segment) writeIndexBatchAt(idxPos int64, positions, lengths []int64) error {
	if len(positions) == 0 {
		return nil
	}
	buf := getBuf(len(positions) * rawIndexEntrySize)
	for i := range positions {
		var ent [rawIndexEntrySize]byte
		rawIndexEntry{pos: positions[i], len: lengths[i]}.MarshalBinary(&ent)
		copy(buf.b[i*rawIndexEntrySize:], ent[:])
	}
	if indexWriteHook != nil {
		indexWriteHook(idxPos, len(buf.b))
	}
	_, err := s.idxFile.WriteAt(buf.b, idxPos)
	buf.release(maxIndexKeepBytes)
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

		if _, _, _, _, err := decodeRecord(body); err != nil {
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
	_, idxData := s.views()
	if len(idxData) > 0 {
		return s.baseOff + uint64(len(idxData)/rawIndexEntrySize)
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
	count := s.indexCount()
	if count == 0 {
		return time.Time{}, false
	}

	idxPos := int64(count-1) * rawIndexEntrySize
	var buf [rawIndexEntrySize]byte
	_, idxData := s.views()
	if len(idxData) >= int(idxPos+rawIndexEntrySize) {
		copy(buf[:], idxData[idxPos:idxPos+rawIndexEntrySize])
	} else if _, err := s.idxFile.ReadAt(buf[:], idxPos); err != nil {
		return time.Time{}, false
	}
	entry := UnmarshalRawIndexEntry(buf[:])
	if entry.len < minRecordSize {
		return time.Time{}, false
	}

	rec := make([]byte, entry.len)
	logData, _ := s.views()
	if len(logData) >= int(entry.pos+entry.len) {
		copy(rec, logData[entry.pos:entry.pos+entry.len])
	} else if _, err := s.logFile.ReadAt(rec, entry.pos); err != nil {
		return time.Time{}, false
	}
	_, ts, _, _, err := decodeRecord(rec[recordLengthSize:])
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// indexCount returns the number of index slots the segment holds.
func (s *segment) indexCount() int {
	_, idxData := s.views()
	if len(idxData) > 0 {
		return len(idxData) / rawIndexEntrySize
	}
	if s.idxFile != nil {
		if info, err := s.idxFile.Stat(); err == nil {
			return int(info.Size() / rawIndexEntrySize)
		}
	}
	return 0
}

// unmap releases the segment's read mappings so its files can be rewritten.
func (s *segment) unmap() error {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	var err error
	if s.mmapData != nil {
		err = s.mmapData.Unmap()
		s.mmapData = nil
	}
	if s.idxMMap != nil {
		if e := s.idxMMap.Unmap(); e != nil && err == nil {
			err = e
		}
		s.idxMMap = nil
	}
	return err
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
	// Unmap first: Windows refuses to truncate a file with a mapped section open.
	mmapErr := s.unmap()

	var firstErr error
	// Trim unused pre-allocated capacity; a real shrink makes the directory entry worth syncing.
	if s.logFile != nil && s.pos >= 0 {
		if info, err := s.logFile.Stat(); err == nil && info.Size() != s.pos {
			if err := s.logFile.Truncate(s.pos); err != nil {
				firstErr = err
			} else if err := syncData(s.logFile); err != nil {
				firstErr = err
			} else if s.dir != "" {
				if root, err := os.OpenRoot(s.dir); err != nil {
					firstErr = err
				} else {
					if err := syncDir(root); err != nil {
						firstErr = err
					}
					_ = root.Close()
				}
			}
		}
	}

	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.idxFile != nil {
		if err := s.idxFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if mmapErr != nil && firstErr == nil {
		firstErr = mmapErr
	}
	return firstErr
}
