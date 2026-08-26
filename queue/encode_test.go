package queue

import (
	"bytes"
	"errors"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		payload []byte
	}{
		{name: "simple", id: "0198abcd-0000-7000-8000-000000000000", payload: []byte("hello mikyu")},
		{name: "empty payload", id: "some-id", payload: nil},
		{name: "empty id", id: "", payload: []byte("data")},
		{name: "binary payload", id: "bin", payload: []byte{0x00, 0x01, 0xFF, 0xFE, 0x7F}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := encodeRecord(tt.id, tt.payload)
			gotID, gotPayload, err := decodeRecord(rec[4:]) // skip length prefix
			if err != nil {
				t.Fatalf("decodeRecord: %v", err)
			}
			if gotID != tt.id {
				t.Errorf("id = %q, want %q", gotID, tt.id)
			}
			if !bytes.Equal(gotPayload, tt.payload) {
				t.Errorf("payload = %v, want %v", gotPayload, tt.payload)
			}
		})
	}
}

func TestDecodeRecordCorruptChecksum(t *testing.T) {
	t.Parallel()

	rec := encodeRecord("id", []byte("payload"))
	rec[4+6] ^= 0xFF // flip bits in the body, past the checksum field

	if _, _, err := decodeRecord(rec[4:]); !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("err = %v, want %v", err, ErrCorruptRecord)
	}
}

func TestDecodeRecordTruncated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		buf  []byte
	}{
		{name: "shorter than header", buf: []byte{0x00, 0x01}},
		{name: "exactly header", buf: make([]byte, 6)},
		{name: "declared id longer than buffer", buf: append(make([]byte, 6), 'x')}, // idLen=0? no: set below
	}

	// Make the third case actually declare an oversized ID.
	tests[2].buf[4], tests[2].buf[5] = 0x00, 0x10 // idLen = 16, but only 1 byte follows

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := decodeRecord(tt.buf); !errors.Is(err, ErrCorruptRecord) {
				t.Errorf("err = %v, want %v", err, ErrCorruptRecord)
			}
		})
	}
}
