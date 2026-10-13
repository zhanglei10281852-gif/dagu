// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/eventstore"
	"github.com/dagucloud/dagu/v2/internal/ir"
	filemonitor "github.com/dagucloud/dagu/v2/internal/persis/file/monitor"
	"github.com/dagucloud/dagu/v2/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two monitors contend for one lease with a shared registry. The standby
// monitor's controller must read state from disk and route redrives to the
// active owner.
func TestDeliveryController_RoutesRedriveToLeaseOwner(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	now := time.Now().UTC()
	status := &ir.DAGRunStatus{Name: "briefing", Status: ir.Failed, DAGRunID: "run-dl", AttemptID: "attempt-dl"}
	event := NotificationEvent{
		Key:        NotificationSeenKey(status),
		Type:       eventstore.TypeDAGRunFailed,
		Status:     status,
		ObservedAt: now,
	}
	deadRecord := &notificationDeliveryRecord{
		Status: DeliveryStatusDeadLetter, Event: &event, Generation: 1,
		DeadLetterAt: &now, DeadLetterReason: deadLetterReasonClientError,
		CreatedAt: now, UpdatedAt: now,
	}
	state := newNotificationMonitorState()
	state.Bootstrapped = true
	state.Destinations["dest-1"] = &notificationDestinationState{
		Records: map[string]*notificationDeliveryRecord{event.Key: deadRecord},
	}
	require.NoError(t, newNotificationStateStore(filemonitor.NewStateStore(stateFile)).Save(context.Background(), state))

	registry := NewMonitorRegistry()
	cfg := newTestNotificationMonitorConfig()
	owner := newFileBackedMonitor(nil, stateFile,
		&fakeNotificationTransport{destinations: []string{"dest-1"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	owner.SetRegistry(registry)
	standby := newFileBackedMonitor(nil, stateFile,
		&fakeNotificationTransport{destinations: []string{"dest-1"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	standby.SetRegistry(registry)

	// Start the intended owner first and wait for it to hold the lease before
	// launching the standby contender; otherwise lock acquisition order between
	// equal contenders is nondeterministic under load.
	ownerStop := testutil.StartContextRunner(t, owner)
	defer ownerStop()

	require.Eventually(t, func() bool {
		return registry.Active() == owner && owner.ownsNotificationLock()
	}, notificationMonitorEventuallyTimeout(10*time.Second), 10*time.Millisecond)

	standbyStop := testutil.StartContextRunner(t, standby)
	defer standbyStop()

	// The standby monitor's controller reads the dead-letter record from disk.
	standbyController := NewDeliveryController(standby, registry)
	page, err := standbyController.ListDeliveryRecords(context.Background(), DeliveryRecordFilter{
		Statuses: []DeliveryStatus{DeliveryStatusDeadLetter},
	}, "", 10)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	assert.Equal(t, event.Key, page.Records[0].EventKey)

	// Redrive through the standby controller is executed by the owner.
	results, err := standbyController.RedriveDeliveryRecords(context.Background(), []DeliveryRecordRef{{
		Destination: "dest-1", EventKey: event.Key,
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, RedriveOutcomeRedriven, results[0].Outcome)

	reloaded := newNotificationStateStore(filemonitor.NewStateStore(stateFile)).Load(context.Background())
	require.NoError(t, reloaded.Warning)
	reopened := reloaded.State.Destinations["dest-1"].Records[event.Key]
	require.NotNil(t, reopened)
	assert.Equal(t, DeliveryStatusPending, reopened.Status)
	assert.Equal(t, 2, reopened.Generation)
}

func TestDeliveryController_NoActiveOwnerReturnsInactive(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	local := newFileBackedMonitor(nil, stateFile,
		&fakeNotificationTransport{destinations: []string{"dest-1"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)), newTestNotificationMonitorConfig())
	registry := NewMonitorRegistry()
	controller := NewDeliveryController(local, registry)

	_, err := controller.RedriveDeliveryRecords(context.Background(), []DeliveryRecordRef{{Destination: "dest-1", EventKey: "k"}})
	assert.ErrorIs(t, err, ErrDeliveryControllerInactive)
}
