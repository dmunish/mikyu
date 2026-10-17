package protocol

// EncodeLogsSubscribe writes an OpLogs request body: u8 follow.
func EncodeLogsSubscribe(b *Buffer, follow bool) {
	if follow {
		b.WriteU8(1)
		return
	}
	b.WriteU8(0)
}

// DecodeLogsSubscribe reads an OpLogs request body: u8 follow.
func DecodeLogsSubscribe(r *Reader) (bool, error) {
	follow, err := r.ReadU8()
	if err != nil {
		return false, err
	}
	return follow != 0, nil
}
