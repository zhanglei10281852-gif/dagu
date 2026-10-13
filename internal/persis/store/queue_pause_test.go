// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/persis"
	"github.com/dagucloud/dagu/v2/internal/persis/store"
	"github.com/dagucloud/dagu/v2/internal/persis/testutil"
	"github.com/dagucloud/dagu/v2/internal/queue"
)

func newQueuePauseStore() queue.PauseStore {
	return store.NewQueuePauseStore(testutil.NewMemoryBackend().Collection("queue"))
}

func TestQueuePauseDefaultsToNotPaused(t *testing.T) {
	ctx := context.Background()
	s := newQueuePauseStore()

	state, err := s.Get(ctx, "downstream-a")
	require.NoError(t, err)
	assert.Equal(t, queue.PauseState{QueueName: "downstream-a"}, state)
}

func TestQueuePauseRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newQueuePauseStore()

	state, err := s.SetPause(ctx, "downstream-a", true, "alice", "downstream maintenance")
	require.NoError(t, err)
	assert.True(t, state.Paused)
	assert.Equal(t, int64(1), state.Version)
	assert.Equal(t, "alice", state.PausedBy)
	assert.Equal(t, "downstream maintenance", state.Reason)
	assert.False(t, state.PausedAt.IsZero())

	got, err := s.Get(ctx, "downstream-a")
	require.NoError(t, err)
	assert.True(t, got.Paused)
	assert.Equal(t, int64(1), got.Version)
	assert.Equal(t, "alice", got.PausedBy)
	assert.Equal(t, "downstream maintenance", got.Reason)
}

func TestQueuePauseRepeatedPauseKeepsGeneration(t *testing.T) {
	ctx := context.Background()
	s := newQueuePauseStore()

	first, err := s.SetPause(ctx, "q", true, "alice", "first reason")
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)
	second, err := s.SetPause(ctx, "q", true, "bob", "annotated reason")
	require.NoError(t, err)

	assert.True(t, second.Paused)
	assert.Equal(t, first.Version, second.Version, "repeated pause must not bump the generation")
	assert.Equal(t, first.PausedAt, second.PausedAt, "repeated pause must keep the original timestamp")
	assert.Equal(t, "bob", second.PausedBy)
	assert.Equal(t, "annotated reason", second.Reason)
}

func TestQueuePauseResumeBumpsGenerationAndClearsMetadata(t *testing.T) {
	ctx := context.Background()
	s := newQueuePauseStore()

	paused, err := s.SetPause(ctx, "q", true, "alice", "maintenance")
	require.NoError(t, err)

	resumed, err := s.SetPause(ctx, "q", false, "", "")
	require.NoError(t, err)
	assert.False(t, resumed.Paused)
	assert.Equal(t, paused.Version+1, resumed.Version)
	assert.Empty(t, resumed.PausedBy)
	assert.Empty(t, resumed.Reason)
	assert.True(t, resumed.PausedAt.IsZero())

	// Resuming an unpaused queue is a no-op: same generation, no error.
	again, err := s.SetPause(ctx, "q", false, "bob", "ignored")
	require.NoError(t, err)
	assert.Equal(t, resumed.Version, again.Version)
	assert.False(t, again.Paused)
}

// A pause written by the API process must be observed by the scheduler
// process sharing the same backing storage.
func TestQueuePauseVisibleAcrossInstances(t *testing.T) {
	ctx := context.Background()
	col := testutil.NewMemoryBackend().Collection("queue")
	reader := store.NewQueuePauseStore(col)
	writer := store.NewQueuePauseStore(col)

	_, err := reader.Get(ctx, "q")
	require.NoError(t, err)

	_, err = writer.SetPause(ctx, "q", true, "alice", "maintenance")
	require.NoError(t, err)

	got, err := reader.Get(ctx, "q")
	require.NoError(t, err)
	assert.True(t, got.Paused)
	assert.Equal(t, int64(1), got.Version)

	_, err = writer.SetPause(ctx, "q", false, "", "")
	require.NoError(t, err)
	got, err = reader.Get(ctx, "q")
	require.NoError(t, err)
	assert.False(t, got.Paused)
	assert.Equal(t, int64(2), got.Version)
}

func TestQueuePauseIsIndependentPerQueue(t *testing.T) {
	ctx := context.Background()
	s := newQueuePauseStore()

	_, err := s.SetPause(ctx, "q-a", true, "alice", "maintenance")
	require.NoError(t, err)

	a, err := s.Get(ctx, "q-a")
	require.NoError(t, err)
	b, err := s.Get(ctx, "q-b")
	require.NoError(t, err)
	assert.True(t, a.Paused)
	assert.False(t, b.Paused)

	states, err := s.List(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.Equal(t, "q-a", states[0].QueueName)
	assert.True(t, states[0].Paused)
}

func TestQueuePauseListIncludesResumedQueues(t *testing.T) {
	ctx := context.Background()
	s := newQueuePauseStore()

	_, err := s.SetPause(ctx, "q-a", true, "alice", "x")
	require.NoError(t, err)
	_, err = s.SetPause(ctx, "q-a", false, "", "")
	require.NoError(t, err)

	states, err := s.List(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.False(t, states[0].Paused)
}

// A corrupt pause record is unreadable so dispatch fails closed, but an
// operator action repairs it instead of wedging the queue permanently.
func TestQueuePauseCorruptRecordFailsClosedThenRepairs(t *testing.T) {
	ctx := context.Background()
	col := testutil.NewMemoryBackend().Collection("queue")
	s := store.NewQueuePauseStore(col)

	require.NoError(t, col.Put(ctx, &persis.Record{ID: "q/.queue-pause", Data: []byte("{not json")}))

	_, err := s.Get(ctx, "q")
	require.Error(t, err)
	assert.True(t, errors.Is(err, queue.ErrQueuePauseUnavailable))

	repaired, err := s.SetPause(ctx, "q", false, "", "")
	require.NoError(t, err)
	assert.False(t, repaired.Paused)
	assert.Equal(t, int64(1), repaired.Version)

	got, err := s.Get(ctx, "q")
	require.NoError(t, err)
	assert.False(t, got.Paused)
}

// Pause metadata and queue items share a collection but must not interfere:
// pausing neither lists as an item nor changes item order or visibility.
func TestQueuePauseRecordCoexistsWithQueueItems(t *testing.T) {
	ctx := context.Background()
	col := testutil.NewMemoryBackend().Collection("queue")
	queueStore := store.NewQueueStore(col)
	pauseStore := store.NewQueuePauseStore(col)

	const name = "q"
	refs := []ir.DAGRunRef{
		ir.NewDAGRunRef("dag", "run-1"),
		ir.NewDAGRunRef("dag", "run-2"),
		ir.NewDAGRunRef("dag", "run-3"),
	}
	for _, ref := range refs {
		require.NoError(t, queueStore.Enqueue(ctx, name, queue.QueuePriorityLow, ref))
	}
	_, err := pauseStore.SetPause(ctx, name, true, "alice", "maintenance")
	require.NoError(t, err)

	names, err := queueStore.QueueList(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{name}, names)

	items, err := queueStore.List(ctx, name)
	require.NoError(t, err)
	require.Len(t, items, 3)
	first, err := items[0].Data()
	require.NoError(t, err)
	assert.Equal(t, "run-1", first.ID)
	last, err := items[2].Data()
	require.NoError(t, err)
	assert.Equal(t, "run-3", last.ID)
}

// Pause and resume writes wake the queue watcher even when the backlog does
// not change, so a parked scan resumes promptly across processes.
func TestQueuePauseWritesWakeQueueWatcher(t *testing.T) {
	ctx := context.Background()
	col := testutil.NewMemoryBackend().Collection("queue")
	queueStore := store.NewQueueStore(col)
	pauseStore := store.NewQueuePauseStore(col)

	watcher := queueStore.QueueWatcher(ctx)
	notify, err := watcher.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { watcher.Stop(ctx) })

	_, err = pauseStore.SetPause(ctx, "q", true, "alice", "maintenance")
	require.NoError(t, err)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatal("pause write did not wake the queue watcher")
	}

	_, err = pauseStore.SetPause(ctx, "q", false, "", "")
	require.NoError(t, err)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatal("resume write did not wake the queue watcher")
	}
}
