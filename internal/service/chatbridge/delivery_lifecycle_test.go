// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/ir"
	filemonitor "github.com/dagucloud/dagu/v2/internal/persis/file/monitor"
	"github.com/dagucloud/dagu/v2/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLifecycleTestMonitor(cfg NotificationMonitorConfig, transport NotificationTransport) *NotificationMonitor {
	monitor := NewNotificationMonitor(nil, nil, nil, transport, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	monitor.lock = nil
	monitor.lockDir = ""
	monitor.deliveryJitter = func() float64 { return 0.5 }
	return monitor
}

func lifecycleTestStatus(runID string) *ir.DAGRunStatus {
	return &ir.DAGRunStatus{
		Name:      "briefing",
		Status:    ir.Failed,
		DAGRunID:  runID,
		AttemptID: "attempt-" + runID,
		Error:     "boom",
	}
}

func seedPendingRecord(t *testing.T, monitor *NotificationMonitor, destination string, event NotificationEvent) {
	t.Helper()
	monitor.state.Bootstrapped = true
	destState := destinationState(&monitor.state, destination)
	destState.Records[event.Key] = newPendingDeliveryRecord(event, event.ObservedAt)
}

func retryableOutcome() DeliveryOutcome {
	return DeliveryOutcome{
		Delivered: false,
		Retryable: true,
		Category:  DeliveryErrorServerError,
		Summary:   "HTTP 503: busy",
	}
}

func TestMonitor_RecordsRetryableFailureWithBackoffSequence(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	cfg.MaxDeliveryAttempts = 5
	cfg.DeliveryInitialBackoff = 10 * time.Millisecond
	cfg.DeliveryMaxBackoff = 100 * time.Millisecond
	monitor := newLifecycleTestMonitor(cfg, &fakeNotificationTransport{destinations: []string{"dest-1"}})

	event := testNotificationEvent(lifecycleTestStatus("run-1"))
	seedPendingRecord(t, monitor, "dest-1", event)
	batch := NotificationBatch{Class: NotificationClassUrgent, Events: []NotificationEvent{event}}

	wantWaits := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond}
	for attempt, wantWait := range wantWaits {
		monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", batch, retryableOutcome())
		record := monitor.state.Destinations["dest-1"].Records[event.Key]
		require.Equal(t, attempt+1, record.Attempts)
		assert.Equal(t, DeliveryStatusPending, record.Status)
		assert.Equal(t, DeliveryErrorServerError, record.LastErrorCategory)
		assert.Equal(t, "HTTP 503: busy", record.LastErrorSummary)
		require.NotNil(t, record.LastAttemptAt)
		require.NotNil(t, record.NextAttemptAt)
		wait := record.NextAttemptAt.Sub(*record.LastAttemptAt)
		assert.InDelta(t, wantWait, wait, float64(2*time.Millisecond))
	}

	// Fifth retryable failure exhausts the attempt budget.
	monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", batch, retryableOutcome())
	record := monitor.state.Destinations["dest-1"].Records[event.Key]
	assert.Equal(t, 5, record.Attempts)
	assert.Equal(t, DeliveryStatusDeadLetter, record.Status)
	assert.Equal(t, deadLetterReasonAttemptLimit, record.DeadLetterReason)
	require.NotNil(t, record.DeadLetterAt)
	assert.Nil(t, record.NextAttemptAt)
	assert.NotNil(t, record.Event, "dead-letter retains the event snapshot for redrive")
}

func TestMonitor_RetryAfterOverridesComputedBackoff(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	cfg.DeliveryInitialBackoff = 10 * time.Millisecond
	cfg.DeliveryMaxBackoff = 50 * time.Millisecond
	monitor := newLifecycleTestMonitor(cfg, &fakeNotificationTransport{destinations: []string{"dest-1"}})

	event := testNotificationEvent(lifecycleTestStatus("run-1"))
	seedPendingRecord(t, monitor, "dest-1", event)
	outcome := retryableOutcome()
	outcome.Category = DeliveryErrorRateLimited
	outcome.RetryAfter = 500 * time.Millisecond
	monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", NotificationBatch{Events: []NotificationEvent{event}}, outcome)

	record := monitor.state.Destinations["dest-1"].Records[event.Key]
	require.NotNil(t, record.NextAttemptAt)
	wait := record.NextAttemptAt.Sub(*record.LastAttemptAt)
	assert.GreaterOrEqual(t, wait, 500*time.Millisecond)
	assert.LessOrEqual(t, wait, 550*time.Millisecond)
}

func TestMonitor_PermanentFailureDeadLettersImmediately(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	monitor := newLifecycleTestMonitor(cfg, &fakeNotificationTransport{destinations: []string{"dest-1"}})

	event := testNotificationEvent(lifecycleTestStatus("run-1"))
	seedPendingRecord(t, monitor, "dest-1", event)
	outcome := PermanentDeliveryOutcome(DeliveryErrorClientError, "HTTP 404: missing")
	monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", NotificationBatch{Events: []NotificationEvent{event}}, outcome)

	record := monitor.state.Destinations["dest-1"].Records[event.Key]
	assert.Equal(t, DeliveryStatusDeadLetter, record.Status)
	assert.Equal(t, 1, record.Attempts)
	assert.Equal(t, deadLetterReasonClientError, record.DeadLetterReason)
	assert.Nil(t, record.NextAttemptAt)
}

func TestMonitor_ConfigurationFailureDeadLettersWithConfigurationReason(t *testing.T) {
	t.Parallel()

	monitor := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{destinations: []string{"dest-1"}})
	event := testNotificationEvent(lifecycleTestStatus("run-1"))
	seedPendingRecord(t, monitor, "dest-1", event)
	outcome := PermanentDeliveryOutcome(DeliveryErrorConfiguration, "webhook url targets loopback or private network")
	monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", NotificationBatch{Events: []NotificationEvent{event}}, outcome)

	record := monitor.state.Destinations["dest-1"].Records[event.Key]
	assert.Equal(t, DeliveryStatusDeadLetter, record.Status)
	assert.Equal(t, deadLetterReasonConfiguration, record.DeadLetterReason)
}

func TestMonitor_FailedAttemptPersistsToStateFile(t *testing.T) {
	t.Parallel()

	cfg := DefaultNotificationMonitorConfig()
	cfg.DeliveryInitialBackoff = time.Minute
	stateFile := filepath.Join(t.TempDir(), "state.json")
	monitor := NewNotificationMonitor(nil, filemonitor.NewStateStore(stateFile), nil,
		&fakeNotificationTransport{destinations: []string{"dest-1"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	monitor.lock = nil
	monitor.lockDir = ""
	monitor.deliveryJitter = func() float64 { return 0.5 }

	event := testNotificationEvent(lifecycleTestStatus("run-1"))
	seedPendingRecord(t, monitor, "dest-1", event)
	monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", NotificationBatch{Events: []NotificationEvent{event}}, retryableOutcome())

	result := newNotificationStateStore(filemonitor.NewStateStore(stateFile)).Load(context.Background())
	require.NoError(t, result.Warning)
	record := result.State.Destinations["dest-1"].Records[event.Key]
	require.NotNil(t, record)
	assert.Equal(t, 1, record.Attempts)
	assert.Equal(t, DeliveryStatusPending, record.Status)
	assert.Equal(t, DeliveryErrorServerError, record.LastErrorCategory)
	assert.Equal(t, "HTTP 503: busy", record.LastErrorSummary)
	require.NotNil(t, record.LastAttemptAt)
	require.NotNil(t, record.NextAttemptAt)
}

func TestMonitor_SaveFailureDoesNotRecordAttempt(t *testing.T) {
	t.Parallel()

	monitor := newLifecycleTestMonitor(DefaultNotificationMonitorConfig(), &fakeNotificationTransport{destinations: []string{"dest-1"}})
	monitor.stateStore = newNotificationStateStore(failingBootstrapStateStore{})
	event := testNotificationEvent(lifecycleTestStatus("run-1"))
	seedPendingRecord(t, monitor, "dest-1", event)

	monitor.recordBatchDeliveryFailure(context.Background(), "dest-1", NotificationBatch{Events: []NotificationEvent{event}}, retryableOutcome())

	record := monitor.state.Destinations["dest-1"].Records[event.Key]
	assert.Equal(t, DeliveryStatusPending, record.Status)
	assert.Zero(t, record.Attempts)
	assert.Nil(t, record.LastAttemptAt)
	assert.Nil(t, record.NextAttemptAt)
	assert.Empty(t, record.LastErrorSummary)
}

func TestMonitor_RequeuePendingSkipsNotDueRecords(t *testing.T) {
	t.Parallel()

	cfg := newTestNotificationMonitorConfig()
	monitor := newLifecycleTestMonitor(cfg, &fakeNotificationTransport{destinations: []string{"dest-1"}})

	dueEvent := testNotificationEvent(lifecycleTestStatus("run-due"))
	futureEvent := testNotificationEvent(lifecycleTestStatus("run-future"))
	now := time.Now().UTC()
	seedPendingRecord(t, monitor, "dest-1", dueEvent)
	destState := monitor.state.Destinations["dest-1"]
	futureRecord := newPendingDeliveryRecord(futureEvent, now)
	next := now.Add(time.Hour)
	futureRecord.NextAttemptAt = &next
	destState.Records[futureEvent.Key] = futureRecord

	monitor.requeuePending(context.Background(), []string{"dest-1"})

	ready := waitForReadyBatch(t, monitor.currentBatcher())
	var keys []string
	for _, event := range ready.Batch.Events {
		keys = append(keys, event.Key)
	}
	assert.Contains(t, keys, dueEvent.Key)
	assert.NotContains(t, keys, futureEvent.Key)
}

func TestMonitor_DeadLetteredRecordIsNotRedelivered(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		calls int
	)
	transport := &fakeNotificationTransport{
		destinations: []string{"dest-1"},
		flushWithResultFn: func(_ context.Context, _ string, _ NotificationBatch, _ bool) DeliveryOutcome {
			mu.Lock()
			calls++
			mu.Unlock()
			return PermanentDeliveryOutcome(DeliveryErrorClientError, "HTTP 404")
		},
	}
	cfg := newTestNotificationMonitorConfig()
	cfg.PollInterval = 50 * time.Millisecond
	monitor := newFileBackedMonitor(nil, filepath.Join(t.TempDir(), "state.json"), transport, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	stopMonitor := testutil.StartContextRunner(t, monitor)
	defer stopMonitor()

	require.Eventually(t, func() bool {
		monitor.stateMu.Lock()
		defer monitor.stateMu.Unlock()
		return monitor.state.Bootstrapped
	}, notificationMonitorEventuallyTimeout(time.Second), 10*time.Millisecond)

	require.True(t, monitor.NotifyCompletion(lifecycleTestStatus("run-dl")))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 1
	}, notificationMonitorEventuallyTimeout(time.Second), 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	assert.Equal(t, 1, calls, "dead-lettered records must not be retried")
	mu.Unlock()
}

func TestMonitor_AttemptLimitDeadLettersAfterConfiguredAttempts(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		calls int
	)
	transport := &fakeNotificationTransport{
		destinations: []string{"dest-1"},
		flushWithResultFn: func(_ context.Context, _ string, _ NotificationBatch, _ bool) DeliveryOutcome {
			mu.Lock()
			calls++
			mu.Unlock()
			return retryableOutcome()
		},
	}
	cfg := newTestNotificationMonitorConfig()
	cfg.PollInterval = 50 * time.Millisecond
	cfg.MaxDeliveryAttempts = 2
	cfg.DeliveryInitialBackoff = 10 * time.Millisecond
	cfg.DeliveryMaxBackoff = 20 * time.Millisecond
	monitor := newFileBackedMonitor(nil, filepath.Join(t.TempDir(), "state.json"), transport, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	stopMonitor := testutil.StartContextRunner(t, monitor)
	defer stopMonitor()

	status := lifecycleTestStatus("run-limit")
	require.Eventually(t, func() bool {
		monitor.stateMu.Lock()
		defer monitor.stateMu.Unlock()
		return monitor.state.Bootstrapped
	}, notificationMonitorEventuallyTimeout(time.Second), 10*time.Millisecond)
	require.True(t, monitor.NotifyCompletion(status))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 2
	}, notificationMonitorEventuallyTimeout(2*time.Second), 10*time.Millisecond)

	monitor.stateMu.Lock()
	record := monitor.state.Destinations["dest-1"].Records[NotificationSeenKey(status)]
	monitor.stateMu.Unlock()
	require.NotNil(t, record)
	assert.Equal(t, DeliveryStatusDeadLetter, record.Status)
	assert.Equal(t, 2, record.Attempts)
	assert.Equal(t, deadLetterReasonAttemptLimit, record.DeadLetterReason)
}

func TestMonitor_FailingDestinationDoesNotBlockHealthyOne(t *testing.T) {
	t.Parallel()

	var (
		mu         sync.Mutex
		deliveredB int
		callsA     int
		eventsA    int
	)
	transport := &fakeRoutingNotificationTransport{
		fakeNotificationTransport: &fakeNotificationTransport{
			destinations: []string{"dest-a", "dest-b"},
			flushWithResultFn: func(_ context.Context, destination string, batch NotificationBatch, _ bool) DeliveryOutcome {
				mu.Lock()
				if destination == "dest-a" {
					callsA++
					eventsA += len(batch.Events)
				} else {
					deliveredB += len(batch.Events)
				}
				mu.Unlock()
				if destination == "dest-a" {
					return PermanentDeliveryOutcome(DeliveryErrorClientError, "HTTP 404")
				}
				return DeliveredDeliveryOutcome()
			},
		},
		routeFn: func(event NotificationEvent) []string {
			if event.Status != nil && event.Status.Name == "briefing" {
				return []string{"dest-a"}
			}
			return []string{"dest-b"}
		},
	}
	cfg := newTestNotificationMonitorConfig()
	cfg.PollInterval = 20 * time.Millisecond
	cfg.UrgentWindow = 20 * time.Millisecond
	monitor := newFileBackedMonitor(nil, filepath.Join(t.TempDir(), "state.json"), transport, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	stopMonitor := testutil.StartContextRunner(t, monitor)
	defer stopMonitor()

	require.Eventually(t, func() bool {
		monitor.stateMu.Lock()
		defer monitor.stateMu.Unlock()
		return monitor.state.Bootstrapped
	}, notificationMonitorEventuallyTimeout(time.Second), 10*time.Millisecond)

	for i := 0; i < 5; i++ {
		statusA := lifecycleTestStatus(fmt.Sprintf("a-%d", i))
		statusB := &ir.DAGRunStatus{Name: "other", Status: ir.Failed, DAGRunID: fmt.Sprintf("b-%d", i), AttemptID: fmt.Sprintf("ab-%d", i)}
		require.True(t, monitor.NotifyCompletion(statusA))
		require.True(t, monitor.NotifyCompletion(statusB))
	}

	deliveredFive := false
	for deadline := time.Now().Add(notificationMonitorEventuallyTimeout(5 * time.Second)); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		mu.Lock()
		deliveredFive = deliveredB == 5
		mu.Unlock()
		if deliveredFive {
			break
		}
	}
	require.True(t, deliveredFive)

	require.Eventually(t, func() bool {
		monitor.stateMu.Lock()
		defer monitor.stateMu.Unlock()
		for i := 0; i < 5; i++ {
			record := monitor.state.Destinations["dest-a"].Records[NotificationSeenKey(lifecycleTestStatus(fmt.Sprintf("a-%d", i)))]
			if record == nil || record.Status != DeliveryStatusDeadLetter {
				return false
			}
		}
		return true
	}, notificationMonitorEventuallyTimeout(5*time.Second), 20*time.Millisecond)

	mu.Lock()
	assert.GreaterOrEqual(t, eventsA, 5)
	assert.LessOrEqual(t, callsA, 5)
	mu.Unlock()

	monitor.stateMu.Lock()
	defer monitor.stateMu.Unlock()
	for i := 0; i < 5; i++ {
		record := monitor.state.Destinations["dest-a"].Records[NotificationSeenKey(lifecycleTestStatus(fmt.Sprintf("a-%d", i)))]
		require.NotNil(t, record, "a-%d", i)
		assert.Equal(t, DeliveryStatusDeadLetter, record.Status)
	}
}

func TestMonitor_BackoffScheduleSurvivesRestart(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	now := time.Now().UTC()
	status := lifecycleTestStatus("run-restart")
	event := testNotificationEvent(status)
	nextAttempt := now.Add(400 * time.Millisecond)
	state := newNotificationMonitorState()
	state.Bootstrapped = true
	state.Destinations["dest-1"] = &notificationDestinationState{
		Records: map[string]*notificationDeliveryRecord{
			event.Key: func() *notificationDeliveryRecord {
				record := newPendingDeliveryRecord(event, now)
				record.Attempts = 2
				record.LastErrorCategory = DeliveryErrorServerError
				record.LastErrorSummary = "HTTP 503"
				record.LastAttemptAt = &now
				record.NextAttemptAt = &nextAttempt
				return record
			}(),
		},
	}
	require.NoError(t, newNotificationStateStore(filemonitor.NewStateStore(stateFile)).Save(context.Background(), state))

	var (
		mu    sync.Mutex
		calls []time.Time
	)
	transport := &fakeNotificationTransport{
		destinations: []string{"dest-1"},
		flushFn: func(_ context.Context, _ string, _ NotificationBatch, _ bool) bool {
			mu.Lock()
			calls = append(calls, time.Now())
			mu.Unlock()
			return true
		},
	}
	cfg := newTestNotificationMonitorConfig()
	cfg.PollInterval = 50 * time.Millisecond
	monitor := newFileBackedMonitor(nil, stateFile, transport, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	stopMonitor := testutil.StartContextRunner(t, monitor)
	defer stopMonitor()

	// Fresh monitor session must honor the persisted schedule.
	time.Sleep(250 * time.Millisecond)
	mu.Lock()
	require.Empty(t, calls, "record must not be retried before its persisted next attempt time")
	mu.Unlock()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 1
	}, notificationMonitorEventuallyTimeout(2*time.Second), 20*time.Millisecond)

	require.Eventually(t, func() bool {
		return monitor.IsDelivered("dest-1", status)
	}, notificationMonitorEventuallyTimeout(time.Second), 20*time.Millisecond)
}

func TestPruneTerminalRecords_TTLAndCap(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	oldDelivered := now.Add(-3 * time.Hour)
	freshDelivered := now.Add(-time.Hour)
	oldDead := now.Add(-15 * 24 * time.Hour)
	freshDead := now.Add(-time.Hour)

	destState := &notificationDestinationState{Records: map[string]*notificationDeliveryRecord{
		"delivered-old":   newDeliveredDeliveryRecord("delivered-old", oldDelivered),
		"delivered-fresh": newDeliveredDeliveryRecord("delivered-fresh", freshDelivered),
		"dead-old":        {Status: DeliveryStatusDeadLetter, Event: &NotificationEvent{Key: "dead-old"}, DeadLetterAt: &oldDead, CreatedAt: oldDead, UpdatedAt: oldDead},
		"dead-fresh":      {Status: DeliveryStatusDeadLetter, Event: &NotificationEvent{Key: "dead-fresh"}, DeadLetterAt: &freshDead, CreatedAt: freshDead, UpdatedAt: freshDead},
		"pending-1":       newPendingDeliveryRecord(NotificationEvent{Key: "pending-1", ObservedAt: now}, now),
	}}

	changed := pruneTerminalRecords(destState, now, 2*time.Hour, 14*24*time.Hour, 1000)
	assert.True(t, changed)
	assert.NotContains(t, destState.Records, "delivered-old")
	assert.Contains(t, destState.Records, "delivered-fresh")
	assert.NotContains(t, destState.Records, "dead-old")
	assert.Contains(t, destState.Records, "dead-fresh")
	assert.Contains(t, destState.Records, "pending-1")

	// Cap trims the oldest dead-letter regardless of TTL.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("dead-capped-%d", i)
		at := now.Add(-time.Duration(i+2) * time.Hour)
		destState.Records[key] = &notificationDeliveryRecord{
			Status: DeliveryStatusDeadLetter, Event: &NotificationEvent{Key: key}, DeadLetterAt: &at, CreatedAt: at, UpdatedAt: at,
		}
	}
	pruneTerminalRecords(destState, now, 2*time.Hour, 14*24*time.Hour, 3)
	var deadCount int
	for _, record := range destState.Records {
		if record.Status == DeliveryStatusDeadLetter {
			deadCount++
		}
	}
	assert.Equal(t, 3, deadCount)
	assert.Contains(t, destState.Records, "dead-fresh")
}

func TestTrimPendingNotifications_IgnoresTerminalRecords(t *testing.T) {
	t.Parallel()

	state := newNotificationMonitorState()
	destState := &notificationDestinationState{Records: map[string]*notificationDeliveryRecord{}}
	now := time.Now().UTC()
	// 2 pending, 3 dead-letter, 2 delivered: limit applies only to pending.
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("pending-%d", i)
		event := NotificationEvent{Key: key, Status: &ir.DAGRunStatus{Name: "d", DAGRunID: key, AttemptID: key, Status: ir.Failed}, ObservedAt: now.Add(time.Duration(i) * time.Second)}
		destState.Records[key] = newPendingDeliveryRecord(event, event.ObservedAt)
	}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("dead-%d", i)
		destState.Records[key] = &notificationDeliveryRecord{Status: DeliveryStatusDeadLetter, Event: &NotificationEvent{Key: key}, DeadLetterAt: &now, CreatedAt: now, UpdatedAt: now}
	}
	destState.Records["delivered-1"] = newDeliveredDeliveryRecord("delivered-1", now)
	destState.Records["delivered-2"] = newDeliveredDeliveryRecord("delivered-2", now)
	state.Destinations["dest-1"] = destState

	evicted := trimPendingNotifications(&state, 5)
	assert.Nil(t, evicted)
	require.Len(t, destState.Records, 7)

	evicted = trimPendingNotifications(&state, 1)
	require.Contains(t, evicted, "dest-1")
	assert.Len(t, evicted["dest-1"], 1)
	assert.Len(t, pendingRecordsOf(destState), 1)
	assert.Len(t, destState.Records, 6)
}
