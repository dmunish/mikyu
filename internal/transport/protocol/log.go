package protocol

import "time"

// Record is the wire form of one log message.
type Record struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Group     []byte
	Payload   []byte
}

// WriteRecord appends offset, ID, timestamp, group, and payload.
func WriteRecord(b *Buffer, rec Record) {
	b.WriteU64(rec.Offset)
	b.WriteBytes([]byte(rec.ID))
	b.WriteU64(uint64(rec.Timestamp.UnixNano()))
	b.WriteBytes(rec.Group)
	b.WriteBytes(rec.Payload)
}

// ReadRecord parses one log record.
func ReadRecord(r *Reader) (Record, error) {
	off, err := r.ReadU64()
	if err != nil {
		return Record{}, err
	}
	id, err := r.ReadBytes()
	if err != nil {
		return Record{}, err
	}
	ts, err := r.ReadU64()
	if err != nil {
		return Record{}, err
	}
	group, err := r.ReadBytes()
	if err != nil {
		return Record{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return Record{}, err
	}
	return Record{
		Offset:    off,
		ID:        string(id),
		Timestamp: time.Unix(0, int64(ts)),
		Group:     group,
		Payload:   payload,
	}, nil
}

// WriteRecordList writes a length-prefixed sequence of records.
func WriteRecordList(b *Buffer, recs []Record) {
	b.WriteU32(uint32(len(recs)))
	for _, rec := range recs {
		WriteRecord(b, rec)
	}
}

// ReadRecordList parses a record sequence.
func ReadRecordList(r *Reader) ([]Record, error) {
	n, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if n > 65536 {
		return nil, ErrTruncatedBody
	}
	recs := make([]Record, 0, n)
	for range n {
		rec, err := ReadRecord(r)
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
