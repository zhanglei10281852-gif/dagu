// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package scheduler

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dagucloud/dagu/v2/internal/cmn/config"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/persis/file"
	"github.com/dagucloud/dagu/v2/internal/persis/store"
	queuedomain "github.com/dagucloud/dagu/v2/internal/queue"
)

// scriptedPauseStore is a test PauseStore that serves a scripted sequence of
// states to Get, allowing deterministic pause/scan race scenarios.
type scriptedPauseStore struct {
	mu       sync.Mutex
	script   []queuedomain.PauseState
	getCalls int
	getErr   error
	fallback queuedomain.PauseState
	hasFallb bool
	setCalls int
	lastSet  queuedomain.PauseState
}

func (s *scriptedPauseStore) Get(_ context.Context, _ string) (queuedomain.PauseState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	if s.getErr != nil {
		return queuedomain.PauseState{}, s.getErr
	}
	if len(s.script) > 0 {
		next := s.script[0]
		s.script = s.script[1:]
		return next, nil
	}
	return s.fallback, nil
}

func (s *scriptedPauseStore) SetPause(_ context.Context, _ string, paused bool, actor, reason string) (queuedomain.PauseState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCalls++
	s.lastSet = queuedomain.PauseState{Paused: paused, PausedBy: actor, Reason: reason}
	return s.lastSet, nil
}

func (s *scriptedPauseStore) List(_ context.Context) ([]queuedomain.PauseState, error) {
	return nil, nil
}

func (s *scriptedPauseStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalls
}

// useMockDistributedDispatcher switches the fixture processor to a
// distributed executor whose handoff is counted by mockDispatcher, backed by
// a real admission store so successful handoffs return immediately.
func (f *queueFixture) useMockDistributedDispatcher() *mockDispatcher {
	dispatcher := &mockDispatcher{}
	activeStore := store.NewActiveDistributedRunStore(
		file.NewCollection(filepath.Join(f.distributedDir, "active-runs")),
	)
	admissionStore := store.NewDispatchTaskStore(
		file.NewCollection(filepath.Join(f.distributedDir, "dispatch-tasks")),
		store.WithDispatchAdmissionLiveness(f.leaseStore, activeStore),
		store.WithDispatchReservationTTL(time.Minute),
	)
	f.processor.dispatchTaskStore = admissionStore
	f.processor.dispatchAdmissionStore = admissionStore
	f.processor.dagExecutor = NewDAGExecutor(dispatcher, nil, config.ExecutionModeDistributed, "")
	return dispatcher
}

func TestQueueProcessor_PausedQueueStopsNewDispatchAndKeepsBacklog(t *testing.T) {
	queueName := "paused-queue"
	f := newQueueFixture(t).withDAG("pause-dag", 2).withProcessor(config.Queues{
		Enabled: true,
		Config:  []config.QueueConfig{{Name: queueName, MaxActiveRuns: 2}},
	})
	dispatcher := f.useMockDistributedDispatcher()

	f.enqueueToQueue(queueName, "run-1", queuedomain.QueuePriorityHigh)
	f.enqueueToQueue(queueName, "run-2", queuedomain.QueuePriorityHigh)

	_, err := f.pauseStore.SetPause(f.ctx, queueName, true, "alice", "downstream maintenance")
	require.NoError(t, err)

	f.processor.ProcessQueueItems(f.ctx, queueName)
	// A second scan must not slip a dispatch through either.
	f.processor.ProcessQueueItems(f.ctx, queueName)

	assert.Equal(t, int32(0), dispatcher.callCount.Load(), "paused queue must not hand off any run")
	items, err := f.queueStore.List(f.ctx, queueName)
	require.NoError(t, err)
	require.Len(t, items, 2, "backlog must be preserved while paused")
	refs := make([]string, 2)
	for i, item := range items {
		ref, err := item.Data()
		require.NoError(t, err)
		refs[i] = ref.ID
	}
	assert.Equal(t, []string{"run-1", "run-2"}, refs, "priority and FIFO order must be preserved")

	attempt, err := f.dagRunRepository.FindAttempt(f.ctx, ir.NewDAGRunRef("pause-dag", "run-1"))
	require.NoError(t, err)
	status, err := attempt.ReadStatus(f.ctx)
	require.NoError(t, err)
	assert.Equal(t, ir.Queued, status.Status, "queued status must be untouched while paused")
}

func TestQueueProcessor_ResumeDispatchesPreservedOrderWithoutExceedingCapacity(t *testing.T) {
	queueName := "resume-queue"
	f := newQueueFixture(t).withDAG("resume-dag", 1).withProcessor(config.Queues{
		Enabled: true,
		Config:  []config.QueueConfig{{Name: queueName, MaxActiveRuns: 1}},
	})
	dispatcher := f.useMockDistributedDispatcher()

	f.enqueueToQueue(queueName, "run-1", queuedomain.QueuePriorityHigh)
	f.enqueueToQueue(queueName, "run-2", queuedomain.QueuePriorityHigh)

	_, err := f.pauseStore.SetPause(f.ctx, queueName, true, "alice", "maintenance")
	require.NoError(t, err)
	f.processor.ProcessQueueItems(f.ctx, queueName)
	require.Zero(t, dispatcher.callCount.Load())

	_, err = f.pauseStore.SetPause(f.ctx, queueName, false, "", "")
	require.NoError(t, err)
	f.processor.ProcessQueueItems(f.ctx, queueName)

	assert.Equal(t, int32(1), dispatcher.callCount.Load(), "resume must not exceed the queue capacity of one")
	items, err := f.queueStore.List(f.ctx, queueName)
	require.NoError(t, err)
	require.Len(t, items, 1)
	remaining, err := items[0].Data()
	require.NoError(t, err)
	assert.Equal(t, "run-2", remaining.ID, "scan resumes from the head, dispatching the older run first")
}

func TestQueueProcessor_PauseLandingAfterSelectionBlocksFinalHandoff(t *testing.T) {
	testCases := []struct {
		name      string
		observed  queuedomain.PauseState
		atHandoff queuedomain.PauseState
	}{
		{
			name:      "pause lands before handoff",
			observed:  queuedomain.PauseState{Paused: false, Version: 0},
			atHandoff: queuedomain.PauseState{Paused: true, Version: 1},
		},
		{
			name:      "pause and resume land before handoff",
			observed:  queuedomain.PauseState{Paused: false, Version: 0},
			atHandoff: queuedomain.PauseState{Paused: false, Version: 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queueName := "race-queue-" + tc.name
			f := newQueueFixture(t).withDAG("race-dag", 1).withProcessor(config.Queues{
				Enabled: true,
				Config:  []config.QueueConfig{{Name: queueName, MaxActiveRuns: 1}},
			})
			dispatcher := f.useMockDistributedDispatcher()

			f.enqueueToQueue(queueName, "run-1", queuedomain.QueuePriorityHigh)

			scripted := &scriptedPauseStore{script: []queuedomain.PauseState{tc.observed, tc.atHandoff}}
			f.processor.queuePauseStore = scripted

			f.processor.ProcessQueueItems(f.ctx, queueName)

			assert.Equal(t, int32(0), dispatcher.callCount.Load(), "final handoff must not cross a pause generation boundary")
			assert.Equal(t, 2, scripted.calls(), "pause state must be re-read at the handoff, not only at scan entry")
			items, err := f.queueStore.List(f.ctx, queueName)
			require.NoError(t, err)
			require.Len(t, items, 1, "item selected before the pause must stay queued for resume")
		})
	}
}

func TestQueueProcessor_PauseReadFailureWithholdsClaims(t *testing.T) {
	queueName := "pause-unreadable-queue"
	f := newQueueFixture(t).withDAG("unreadable-dag", 1).withProcessor(config.Queues{
		Enabled: true,
		Config:  []config.QueueConfig{{Name: queueName, MaxActiveRuns: 1}},
	})
	dispatcher := f.useMockDistributedDispatcher()
	f.enqueueToQueue(queueName, "run-1", queuedomain.QueuePriorityHigh)

	f.processor.queuePauseStore = &scriptedPauseStore{getErr: errors.New("disk on fire")}

	f.processor.ProcessQueueItems(f.ctx, queueName)
	f.processor.ProcessQueueItems(f.ctx, queueName)

	assert.Zero(t, dispatcher.callCount.Load(), "unreadable pause state must fail closed")
	items, err := f.queueStore.List(f.ctx, queueName)
	require.NoError(t, err)
	require.Len(t, items, 1, "backlog must be preserved when pause state is unreadable")
	assert.Contains(t, f.logs(), "withholding new dispatch claims")
}

func TestQueueProcessor_PausingOneQueueDoesNotStopOtherQueues(t *testing.T) {
	f := newQueueFixture(t).withDAG("shared-dag", 2).withProcessor(config.Queues{
		Enabled: true,
		Config: []config.QueueConfig{
			{Name: "q-running", MaxActiveRuns: 2},
			{Name: "q-paused", MaxActiveRuns: 2},
		},
	})
	dispatcher := f.useMockDistributedDispatcher()

	f.enqueueToQueue("q-running", "run-a", queuedomain.QueuePriorityHigh)
	f.enqueueToQueue("q-paused", "run-b", queuedomain.QueuePriorityHigh)
	_, err := f.pauseStore.SetPause(f.ctx, "q-paused", true, "alice", "maintenance")
	require.NoError(t, err)

	f.processor.ProcessQueueItems(f.ctx, "q-running")
	f.processor.ProcessQueueItems(f.ctx, "q-paused")

	assert.Equal(t, int32(1), dispatcher.callCount.Load(), "only the unpaused queue may dispatch")
	runningItems, err := f.queueStore.List(f.ctx, "q-running")
	require.NoError(t, err)
	assert.Empty(t, runningItems)
	pausedItems, err := f.queueStore.List(f.ctx, "q-paused")
	require.NoError(t, err)
	require.Len(t, pausedItems, 1)
}

func TestQueueDispatcher_QueuePauseAllowsDispatch(t *testing.T) {
	ctx := context.Background()

	t.Run("no pause store configured allows dispatch", func(t *testing.T) {
		d := &queueDispatcher{}
		assert.True(t, d.queuePauseAllowsDispatch(ctx, "q", queuePauseGeneration{available: true}))
	})

	cases := []struct {
		name     string
		state    queuedomain.PauseState
		err      error
		observed queuePauseGeneration
		want     bool
	}{
		{"matching resumed generation", queuedomain.PauseState{Paused: false, Version: 3}, nil, queuePauseGeneration{available: true, version: 3}, true},
		{"currently paused", queuedomain.PauseState{Paused: true, Version: 3}, nil, queuePauseGeneration{available: true, version: 2}, false},
		{"generation moved", queuedomain.PauseState{Paused: false, Version: 4}, nil, queuePauseGeneration{available: true, version: 3}, false},
		{"unreadable state", queuedomain.PauseState{}, errors.New("boom"), queuePauseGeneration{available: true, version: 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &queueDispatcher{queuePauseStore: &scriptedPauseStore{fallback: tc.state, getErr: tc.err, hasFallb: true}}
			assert.Equal(t, tc.want, d.queuePauseAllowsDispatch(ctx, "q", tc.observed))
		})
	}
}
