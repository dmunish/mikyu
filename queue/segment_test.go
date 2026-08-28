package queue

import (
	"testing"
)

func TestSegment_RecoverAndRebuild(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := openSegment(dir, 0)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer s.Close()

	rec1 := encodeRecord("id1", []byte("payload1"))
	rec2 := encodeRecord("id2", []byte("payload2"))

	if _, err := s.logFile.WriteAt(rec1, 0); err != nil {
		t.Fatalf("write rec1: %v", err)
	}
	if _, err := s.logFile.WriteAt(rec2, int64(len(rec1))); err != nil {
		t.Fatalf("write rec2: %v", err)
	}

	entries, err := s.recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}

	if entries[0].pos != 0 || entries[0].len != int64(len(rec1)) {
		t.Errorf("entry 0 mismatch: %+v", entries[0])
	}
	if entries[1].pos != int64(len(rec1)) || entries[1].len != int64(len(rec2)) {
		t.Errorf("entry 1 mismatch: %+v", entries[1])
	}

	// Reopen to verify the fast path (index already built)
	s2, err := openSegment(dir, 0)
	if err != nil {
		t.Fatalf("reopenSegment: %v", err)
	}
	defer s2.Close()

	entries2, err := s2.recover()
	if err != nil {
		t.Fatalf("recover s2: %v", err)
	}
	if len(entries2) != 2 {
		t.Fatalf("reopened got %d entries, want 2", len(entries2))
	}
}
