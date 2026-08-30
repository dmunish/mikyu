package queue

import (
	"testing"
)

func TestOffsetStore_CommitAndFetch(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenManagerOffsetStore(dir)
	if err != nil {
		t.Fatalf("OpenManagerOffsetStore: %v", err)
	}
	defer s.Close()

	if err := s.Commit("orders", "group-a", 42); err != nil {
		t.Errorf("Commit: %v", err)
	}
	if got := s.Fetch("orders", "group-a"); got != 42 {
		t.Errorf("Fetch = %d, want 42", got)
	}
}
