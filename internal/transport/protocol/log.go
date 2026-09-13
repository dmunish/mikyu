package protocol

import "time"

// LogRecord is the wire form of one log message.
type LogRecord struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Payload   []byte
}

// WriteLogRecord appends offset, ID, timestamp, and payload.
func WriteLogRecord(b *Buffer, rec LogRecord) {
	b.WriteU64(rec.Offset)
	b.WriteBytes([]byte(rec.ID))
	b.WriteU64(uint64(rec.Timestamp.UnixNano()))
	b.WriteBytes(rec.Payload)
}

// ReadLogRecord parses one log record.
func ReadLogRecord(r *Reader) (LogRecord, error) {
	off, err := r.ReadU64()
	if err != nil {
		return LogRecord{}, err
	}
	id, err := r.ReadBytes()
	if err != nil {
		return LogRecord{}, err
	}
	ts, err := r.ReadU64()
	if err != nil {
		return LogRecord{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return LogRecord{}, err
	}
	return LogRecord{
		Offset:    off,
		ID:        string(id),
		Timestamp: time.Unix(0, int64(ts)),
		Payload:   payload,
	}, nil
}

// WriteLogRecordList writes a length-prefixed sequence of records.
func WriteLogRecordList(b *Buffer, recs []LogRecord) {
	b.WriteU32(uint32(len(recs)))
	for _, rec := range recs {
		WriteLogRecord(b, rec)
	}
}

// ReadLogRecordList parses a record sequence.
func ReadLogRecordList(r *Reader) ([]LogRecord, error) {
	n, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if n > 65536 {
		return nil, ErrTruncatedBody
	}
	recs := make([]LogRecord, 0, n)
	for range n {
		rec, err := ReadLogRecord(r)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// EncodeFetch writes an OpFetch request.
func EncodeFetch(b *Buffer, queue string, start uint64, maxCount int, maxWait time.Duration) {
	b.WriteBytes([]byte(queue))
	b.WriteU64(start)
	b.WriteU32(uint32(maxCount))
	b.WriteU64(uint64(int64(maxWait)))
}

// DecodeFetch parses an OpFetch request.
func DecodeFetch(r *Reader) (queue string, start uint64, maxCount int, maxWait time.Duration, err error) {
	name, err := r.ReadBytes()
	if err != nil {
		return "", 0, 0, 0, err
	}
	start, err = r.ReadU64()
	if err != nil {
		return "", 0, 0, 0, err
	}
	n, err := r.ReadU32()
	if err != nil {
		return "", 0, 0, 0, err
	}
	wait, err := r.ReadU64()
	if err != nil {
		return "", 0, 0, 0, err
	}
	return string(name), start, int(n), time.Duration(int64(wait)), nil
}
