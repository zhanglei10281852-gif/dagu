// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/eventstore"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pendingRecordsOf(destState *notificationDestinationState) map[string]*notificationDeliveryRecord {
	pending := make(map[string]*notificationDeliveryRecord)
	if destState == nil {
		return pending
	}
	for key, record := range destState.Records {
		if record.isPending() {
			pending[key] = record
		}
	}
	return pending
}

// marshalV1State encodes a destination state using only the legacy v1 tables.
func marshalV1State(t *testing.T, pending map[string]NotificationEvent, delivered map[string]time.Time) []byte {
	t.Helper()
	raw := struct {
		Version      int                                      `json:"version"`
		Bootstrapped bool                                     `json:"bootstrapped"`
		SourceCursor eventstore.DAGRunCursor                  `json:"source_cursor"`
		Destinations map[string]*notificationDestinationState `json:"destinations"`
	}{
		Version:      1,
		Bootstrapped: true,
		SourceCursor: eventstore.DAGRunCursor{CommittedOffsets: map[string]int64{"events": 7}},
		Destinations: map[string]*notificationDestinationState{
			"dest-1": {
				Pending:   pending,
				Delivered: delivered,
			},
		},
	}
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	return data
}

func TestDecodeNotificationMonitorState_MigratesV1Losslessly(t *testing.T) {
	t.Parallel()

	observedAt := time.Now().UTC().Add(-time.Minute)
	deliveredAt := time.Now().UTC().Add(-2 * time.Minute)
	status := &ir.DAGRunStatus{
		Name:      "briefing",
		Status:    ir.Failed,
		DAGRunID:  "run-migrate",
		AttemptID: "attempt-migrate",
		Error:     "boom",
		Labels:    []string{"workspace=ops"},
	}
	pendingEvent := NotificationEvent{
		Key:        NotificationSeenKey(status),
		Type:       eventstore.TypeDAGRunFailed,
		Status:     status,
		DAGFile:    "briefing-file",
		ObservedAt: observedAt,
	}
	data := marshalV1State(t,
		map[string]NotificationEvent{pendingEvent.Key: pendingEvent},
		map[string]time.Time{"legacy-delivered-key": deliveredAt},
	)

	state, err := decodeNotificationMonitorState(data)
	require.NoError(t, err)

	assert.True(t, state.Bootstrapped)
	assert.Equal(t, int64(7), state.SourceCursor.CommittedOffsets["events"])
	destState := state.Destinations["dest-1"]
	require.NotNil(t, destState)
	assert.Nil(t, destState.Pending)
	assert.Nil(t, destState.Delivered)
	require.Len(t, destState.Records, 2)

	pending := destState.Records[pendingEvent.Key]
	require.NotNil(t, pending)
	assert.Equal(t, DeliveryStatusPending, pending.Status)
	require.NotNil(t, pending.Event)
	assert.Equal(t, "run-migrate", pending.Event.Status.DAGRunID)
	assert.Equal(t, "briefing-file", pending.Event.DAGFile)
	assert.Equal(t, []string{"workspace=ops"}, pending.Event.Status.Labels)
	assert.Equal(t, observedAt, pending.Event.ObservedAt)
	assert.Zero(t, pending.Attempts)
	assert.Nil(t, pending.NextAttemptAt)

	delivered := destState.Records["legacy-delivered-key"]
	require.NotNil(t, delivered)
	assert.Equal(t, DeliveryStatusDelivered, delivered.Status)
	require.NotNil(t, delivered.DeliveredAt)
	assert.Equal(t, deliveredAt, *delivered.DeliveredAt)
	require.NotNil(t, delivered.Summary)
	assert.Equal(t, "legacy-delivered-key", delivered.Summary.Key)
	assert.Nil(t, delivered.Event)

	// A v2 round trip must serialize only records and stay lossless.
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), `"pending":`)
	assert.NotContains(t, string(encoded), `"delivered":`)

	roundTrip, err := decodeNotificationMonitorState(encoded)
	require.NoError(t, err)
	assert.Len(t, roundTrip.Destinations["dest-1"].Records, 2)
}

func TestDecodeNotificationMonitorState_MissingVersionIsAccepted(t *testing.T) {
	t.Parallel()

	status := &ir.DAGRunStatus{Name: "briefing", Status: ir.Failed, DAGRunID: "r1", AttemptID: "a1"}
	event := NotificationEvent{Key: NotificationSeenKey(status), Type: eventstore.TypeDAGRunFailed, Status: status}
	data := marshalV1State(t, map[string]NotificationEvent{event.Key: event}, nil)
	// Strip the version field to simulate the oldest on-disk files.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	delete(raw, "version")
	data, err := json.Marshal(raw)
	require.NoError(t, err)

	state, err := decodeNotificationMonitorState(data)
	require.NoError(t, err)
	require.Contains(t, state.Destinations, "dest-1")
	assert.Len(t, pendingRecordsOf(state.Destinations["dest-1"]), 1)
}

func TestCloneNotificationMonitorState_DeepCopiesRecords(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	next := now.Add(time.Minute)
	status := &ir.DAGRunStatus{Name: "briefing", Status: ir.Failed, DAGRunID: "r1", AttemptID: "a1"}
	event := NotificationEvent{
		Key:        NotificationSeenKey(status),
		Type:       eventstore.TypeDAGRunFailed,
		Status:     status,
		ObservedAt: now,
	}
	original := newNotificationMonitorState()
	original.Destinations["dest-1"] = &notificationDestinationState{
		Records: map[string]*notificationDeliveryRecord{
			event.Key: {
				Status:             DeliveryStatusPending,
				Event:              &event,
				Summary:            compactDeliveryEventSummary(event),
				Attempts:           3,
				Generation:         2,
				LastErrorCategory:  DeliveryErrorServerError,
				LastErrorSummary:   "HTTP 503",
				LastAttemptAt:      &now,
				NextAttemptAt:      &next,
				CreatedAt:          now,
				UpdatedAt:          now,
			},
		},
	}

	clone := cloneNotificationMonitorState(original)
	clonedRecord := clone.Destinations["dest-1"].Records[event.Key]
	clonedRecord.Attempts = 99
	clonedRecord.Event.Status.Name = "changed"
	clonedRecord.Summary.DAGName = "changed-summary"
	*clonedRecord.NextAttemptAt = now.Add(time.Hour)

	sourceRecord := original.Destinations["dest-1"].Records[event.Key]
	assert.Equal(t, 3, sourceRecord.Attempts)
	assert.Equal(t, "briefing", sourceRecord.Event.Status.Name)
	assert.Equal(t, "briefing", sourceRecord.Summary.DAGName)
	assert.Equal(t, next, *sourceRecord.NextAttemptAt)
}

func TestSaveBumpsV1StateToV2(t *testing.T) {
	t.Parallel()

	deliveredAt := time.Now().UTC()
	destination := &notificationDestinationState{
		Delivered: map[string]time.Time{"legacy-delivered-key": deliveredAt},
	}
	state := newNotificationMonitorState()
	state.Version = 1
	state.Bootstrapped = true
	state.Destinations["dest-1"] = destination

	data, err := json.Marshal(state)
	require.NoError(t, err)
	store := newNotificationStateStore(&memoryStateStoreForTest{data: data})
	require.NoError(t, store.Save(context.Background(), state))

	result := store.Load(context.Background())
	require.NoError(t, result.Warning)
	assert.Equal(t, notificationMonitorStateVersion, result.State.Version)
	migrated := result.State.Destinations["dest-1"].Records["legacy-delivered-key"]
	require.NotNil(t, migrated)
	assert.Equal(t, DeliveryStatusDelivered, migrated.Status)
}

type memoryStateStoreForTest struct {
	data []byte
}

func (s *memoryStateStoreForTest) Load(context.Context) ([]byte, bool, error) {
	if len(s.data) == 0 {
		return nil, false, nil
	}
	return s.data, true, nil
}

func (s *memoryStateStoreForTest) Save(_ context.Context, data []byte) error {
	s.data = append([]byte(nil), data...)
	return nil
}

func (s *memoryStateStoreForTest) Quarantine(context.Context) (string, error) {
	return "quarantined", nil
}
