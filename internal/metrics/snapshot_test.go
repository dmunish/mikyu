package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotIterQueues(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{{Name: "charlie"}, {Name: "alpha"}, {Name: "bravo"}}}
	var names []string
	for q := range snap.IterQueues() {
		names = append(names, q.Name)
	}
	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, names)

	q, ok := snap.Queue("bravo")
	require.True(t, ok)
	assert.Equal(t, "bravo", q.Name)
	_, ok = snap.Queue("missing")
	assert.False(t, ok)
}
