package queue

import (
	"testing"
)

func TestOffsetStore_CommitAndFetchMultiple(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenManagerOffsetStore(dir)
	if err != nil {
		t.Fatalf("OpenManagerOffsetStore: %v", err)
	}
	defer s.Close()

	if err := s.Commit("orders", "g1", 10); err != nil {
		t.Errorf("Commit: %v", err)
	}
	if err := s.Commit("orders", "g2", 20); err != nil {
		t.Errorf("Commit: %v", err)
	}

	if got := s.Fetch("orders", "g1"); got != 10 {
		t.Errorf("Fetch g1 = %d, want 10", got)
	}
	if got := s.Fetch("orders", "g2"); got != 20 {
		t.Errorf("Fetch g2 = %d, want 20", got)
	}
	if got := s.Fetch("orders", "missing"); got != 0 {
		t.Errorf("Fetch missing = %d, want 0", got)
	}
}
