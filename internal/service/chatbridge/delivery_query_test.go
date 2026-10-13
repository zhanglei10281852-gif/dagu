// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/dirlock"
	"github.com/dagucloud/dagu/v2/internal/eventstore"
	"github.com/dagucloud/dagu/v2/internal/ir"
	filemonitor "github.com/dagucloud/dagu/v2/internal/persis/file/monitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queryTestEvent(runID string, status ir.Status, observedAt time.Time) NotificationEvent {
	return NotificationEvent{
		Key:        runID + ":" + "attempt" + ":" + status.String(),
		Type:       eventstore.TypeDAGRunFailed,
		DAGFile:    "dag-file",
		ObservedAt: observedAt,
		Status: &ir.DAGRunStatus{
			Name:      "briefing",
			Status:    status,
			DAGRunID:  runID,
			AttemptID: "attempt",
		},
	}
}

func TestDeliveryRecordID_RoundTrip(t *testing.T) {
	t.Parallel()

	id := EncodeDeliveryRecordID("route\x00global\x00r1", "run-1:attempt-1:failed")
	destination, key, err := DecodeDeliveryRecordID(id)
	require.NoError(t, err)
	assert.Equal(t, "route\x00global\x00r1", destination)
	assert.Equal(t, "run-1:attempt-1:failed", key)

	for _, bad := range []string{"", "!!!", "bm90LWpzb24="} {
		_, _, err := DecodeDeliveryRecordID(bad)
		assert.Error(t, err, bad)
	}
}

func TestMonitor_ListDeliveryRecordsFiltersAndPaginates(t *testing.T) {
	t.Parallel()

	monitor := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{})
	now := time.Now().UTC()
	state := newNotificationMonitorState()
	state.Bootstrapped = true
	state.Destinations["dest-a"] = &notificationDestinationState{Records: map[string]*notificationDeliveryRecord{}}
	state.Destinations["dest-b"] = &notificationDestinationState{Records: map[string]*notificationDeliveryRecord{}}

	eventA1 := queryTestEvent("a1", ir.Failed, now.Add(-3*time.Second))
	eventA2 := queryTestEvent("a2", ir.Failed, now.Add(-1*time.Second))
	state.Destinations["dest-a"].Records[eventA1.Key] = newPendingDeliveryRecord(eventA1, eventA1.ObservedAt)
	deadAt := now.Add(-2 * time.Second)
	state.Destinations["dest-a"].Records[eventA2.Key] = &notificationDeliveryRecord{
		Status: DeliveryStatusDeadLetter, Event: &eventA2, Generation: 1, Attempts: 3,
		LastErrorCategory: DeliveryErrorClientError, LastErrorSummary: "HTTP 404",
		DeadLetterAt: &deadAt, DeadLetterReason: deadLetterReasonClientError,
		CreatedAt: eventA2.ObservedAt, UpdatedAt: deadAt,
	}
	eventB1 := queryTestEvent("b1", ir.Failed, now.Add(-4*time.Second))
	state.Destinations["dest-b"].Records[eventB1.Key] = newDeliveredRecordFromEvent(eventB1, now.Add(-4*time.Second))
	monitor.state = state

	// No filter: newest first.
	page, err := monitor.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{}, "", 2)
	require.NoError(t, err)
	require.Len(t, page.Records, 2)
	assert.Equal(t, eventA2.Key, page.Records[0].EventKey)
	assert.Equal(t, eventA1.Key, page.Records[1].EventKey)
	assert.NotEmpty(t, page.NextCursor)

	page2, err := monitor.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{}, page.NextCursor, 2)
	require.NoError(t, err)
	require.Len(t, page2.Records, 1)
	assert.Equal(t, eventB1.Key, page2.Records[0].EventKey)
	assert.Empty(t, page2.NextCursor)

	// Status filter.
	deadPage, err := monitor.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{Statuses: []DeliveryStatus{DeliveryStatusDeadLetter}}, "", 10)
	require.NoError(t, err)
	require.Len(t, deadPage.Records, 1)
	assert.Equal(t, DeliveryStatusDeadLetter, deadPage.Records[0].Status)
	assert.Equal(t, "HTTP 404", deadPage.Records[0].LastErrorSummary)
	assert.Equal(t, "briefing", deadPage.Records[0].DAGName)
	assert.Equal(t, "dag-file", deadPage.Records[0].DAGFile)

	// Destination filter.
	bPage, err := monitor.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{Destination: "dest-b"}, "", 10)
	require.NoError(t, err)
	require.Len(t, bPage.Records, 1)
	assert.Equal(t, "dest-b", bPage.Records[0].Destination)
	assert.Equal(t, "b1", bPage.Records[0].DAGRunID)

	// Invalid cursor.
	_, err = monitor.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{}, "!!!", 10)
	assert.Error(t, err)
}

func TestMonitor_ListDeliveryRecordsReadsCommittedStateFile(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	now := time.Now().UTC()
	event := queryTestEvent("a1", ir.Failed, now)
	state := newNotificationMonitorState()
	state.Bootstrapped = true
	state.Destinations["dest-a"] = &notificationDestinationState{
		Records: map[string]*notificationDeliveryRecord{event.Key: newPendingDeliveryRecord(event, now)},
	}
	require.NoError(t, newNotificationStateStore(filemonitor.NewStateStore(stateFile)).Save(context.Background(), state))

	monitor := NewNotificationMonitor(nil, filemonitor.NewStateStore(stateFile), nil,
		&fakeNotificationTransport{destinations: []string{"dest-a"}}, nil, DefaultNotificationMonitorConfig())

	page, err := monitor.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{}, "", 50)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	assert.Equal(t, event.Key, page.Records[0].EventKey)
}

func TestMonitor_GetDeliveryRecord(t *testing.T) {
	t.Parallel()

	monitor := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{})
	now := time.Now().UTC()
	event := queryTestEvent("a1", ir.Failed, now)
	seedPendingRecord(t, monitor, "dest-a", event)

	view, ok, err := monitor.GetDeliveryRecord(context.Background(), "dest-a", event.Key)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, DeliveryStatusPending, view.Status)
	assert.Equal(t, "briefing", view.DAGName)

	_, ok, err = monitor.GetDeliveryRecord(context.Background(), "dest-a", "missing")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestMonitor_RedriveDeadLetterReopensGenerationOnce(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	cfg.UrgentWindow = 10 * time.Millisecond
	cfg.SuccessWindow = 10 * time.Millisecond
	monitor := newLifecycleTestMonitor(cfg, &fakeNotificationTransport{destinations: []string{"dest-a"}})

	now := time.Now().UTC()
	deadEvent := queryTestEvent("a1", ir.Failed, now)
	deliveredEvent := queryTestEvent("a2", ir.Succeeded, now)
	pendingEvent := queryTestEvent("a3", ir.Failed, now)
	monitor.state.Bootstrapped = true
	destState := destinationState(&monitor.state, "dest-a")
	deadRecord := &notificationDeliveryRecord{
		Status: DeliveryStatusDeadLetter, Event: &deadEvent, Generation: 2, Attempts: 5,
		LastErrorCategory: DeliveryErrorClientError, LastErrorSummary: "HTTP 404",
		DeadLetterAt: &now, DeadLetterReason: deadLetterReasonClientError,
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	}
	destState.Records[deadEvent.Key] = deadRecord
	destState.Records[deliveredEvent.Key] = newDeliveredRecordFromEvent(deliveredEvent, now)
	destState.Records[pendingEvent.Key] = newPendingDeliveryRecord(pendingEvent, now)

	refs := []DeliveryRecordRef{
		{Destination: "dest-a", EventKey: deadEvent.Key},
		{Destination: "dest-a", EventKey: deliveredEvent.Key},
		{Destination: "dest-a", EventKey: pendingEvent.Key},
		{Destination: "dest-a", EventKey: "missing"},
	}
	results, err := monitor.RedriveDeliveryRecords(context.Background(), refs)
	require.NoError(t, err)
	require.Len(t, results, 4)
	byKey := map[string]DeliveryRedriveResult{}
	for _, result := range results {
		byKey[result.EventKey] = result
	}
	assert.Equal(t, RedriveOutcomeRedriven, byKey[deadEvent.Key].Outcome)
	assert.Equal(t, 3, byKey[deadEvent.Key].Generation)
	assert.Equal(t, DeliveryStatusPending, byKey[deadEvent.Key].Status)
	assert.Equal(t, RedriveOutcomeDelivered, byKey[deliveredEvent.Key].Outcome)
	assert.Equal(t, RedriveOutcomeAlreadyPending, byKey[pendingEvent.Key].Outcome)
	assert.Equal(t, RedriveOutcomeNotFound, byKey["missing"].Outcome)

	reopened := monitor.state.Destinations["dest-a"].Records[deadEvent.Key]
	assert.Equal(t, DeliveryStatusPending, reopened.Status)
	assert.Zero(t, reopened.Attempts)
	assert.Nil(t, reopened.NextAttemptAt)
	assert.Empty(t, reopened.LastErrorSummary)
	require.NotNil(t, reopened.Event)
	assert.Equal(t, deadEvent.Key, reopened.Event.Key)

	// The reopened record is queued into the batcher exactly once.
	batch := waitForReadyBatch(t, monitor.currentBatcher())
	var keys []string
	for _, batchEvent := range batch.Batch.Events {
		keys = append(keys, batchEvent.Key)
	}
	assert.Equal(t, []string{deadEvent.Key}, keys)

	// A second redrive is idempotent: no new generation, no second enqueue.
	again, err := monitor.RedriveDeliveryRecords(context.Background(), []DeliveryRecordRef{{Destination: "dest-a", EventKey: deadEvent.Key}})
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, RedriveOutcomeAlreadyPending, again[0].Outcome)
	assert.Equal(t, 3, again[0].Generation)
}

func TestMonitor_RedriveConcurrentRequestsBumpGenerationOnce(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	cfg.UrgentWindow = time.Hour // keep the event in the bucket so re-enqueues dedupe
	monitor := newLifecycleTestMonitor(cfg, &fakeNotificationTransport{destinations: []string{"dest-a"}})
	now := time.Now().UTC()
	deadEvent := queryTestEvent("a1", ir.Failed, now)
	monitor.state.Bootstrapped = true
	destState := destinationState(&monitor.state, "dest-a")
	destState.Records[deadEvent.Key] = &notificationDeliveryRecord{
		Status: DeliveryStatusDeadLetter, Event: &deadEvent, Generation: 4,
		DeadLetterAt: &now, CreatedAt: now, UpdatedAt: now,
	}

	const callers = 16
	var wg sync.WaitGroup
	redriven := make(chan DeliveryRedriveResult, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := monitor.RedriveDeliveryRecords(context.Background(), []DeliveryRecordRef{{Destination: "dest-a", EventKey: deadEvent.Key}})
			assert.NoError(t, err)
			require.Len(t, results, 1)
			redriven <- results[0]
		}()
	}
	wg.Wait()
	close(redriven)

	var redrivenCount int
	for result := range redriven {
		require.Equal(t, 5, result.Generation)
		if result.Outcome == RedriveOutcomeRedriven {
			redrivenCount++
		}
	}
	assert.Equal(t, 1, redrivenCount)
	monitor.stateMu.Lock()
	finalRecord := monitor.state.Destinations["dest-a"].Records[deadEvent.Key]
	assert.Equal(t, 5, finalRecord.Generation)
	monitor.stateMu.Unlock()
}

func TestMonitor_RedriveWithoutLeaseReturnsInactive(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	monitor := NewNotificationMonitor(nil, nil, nil,
		&fakeNotificationTransport{destinations: []string{"dest-a"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	// A real lease that is never held makes the fence reject the redrive.
	stateFile := filepath.Join(t.TempDir(), "state.json")
	monitor.lock = filemonitor.NewLease(stateFile, &dirlock.LockOptions{
		StaleThreshold: DefaultNotificationLockStaleThreshold,
		RetryInterval:  DefaultNotificationLockRetryInterval,
	})

	_, err := monitor.RedriveDeliveryRecords(context.Background(), []DeliveryRecordRef{{Destination: "dest-a", EventKey: "k"}})
	assert.ErrorIs(t, err, ErrDeliveryControllerInactive)
}

func TestMonitor_RedriveSaveFailureDoesNotMutateOrEnqueue(t *testing.T) {
	t.Parallel()

	monitor := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{destinations: []string{"dest-a"}})
	monitor.stateStore = newNotificationStateStore(failingBootstrapStateStore{})
	now := time.Now().UTC()
	deadEvent := queryTestEvent("a1", ir.Failed, now)
	monitor.state.Bootstrapped = true
	destState := destinationState(&monitor.state, "dest-a")
	destState.Records[deadEvent.Key] = &notificationDeliveryRecord{
		Status: DeliveryStatusDeadLetter, Event: &deadEvent, Generation: 1,
		DeadLetterAt: &now, CreatedAt: now, UpdatedAt: now,
	}

	_, err := monitor.RedriveDeliveryRecords(context.Background(), []DeliveryRecordRef{{Destination: "dest-a", EventKey: deadEvent.Key}})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrDeliveryControllerInactive))

	record := destState.Records[deadEvent.Key]
	assert.Equal(t, DeliveryStatusDeadLetter, record.Status)
	assert.Equal(t, 1, record.Generation)
	assert.Empty(t, monitor.currentBatcher().TakeReady())
}

func TestMonitorRegistry_ActiveFollowsLeaseOwnership(t *testing.T) {
	t.Parallel()

	registry := NewMonitorRegistry()
	assert.Nil(t, registry.Active())

	monitor1 := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{destinations: []string{"dest-a"}})
	monitor2 := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{destinations: []string{"dest-a"}})

	registry.SetActive(monitor1)
	assert.Same(t, monitor1, registry.Active())

	// A stale owner must not clear the new owner.
	registry.SetActive(monitor2)
	registry.ClearActive(monitor1)
	assert.Same(t, monitor2, registry.Active())

	registry.ClearActive(monitor2)
	assert.Nil(t, registry.Active())

	// Nil-safe for monitors without registry.
	monitor1.SetRegistry(nil)
	monitor1.registerActive()
	monitor1.endNotificationSession()
}
