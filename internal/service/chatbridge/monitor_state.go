// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dagucloud/dagu/v2/internal/eventstore"
)

const notificationMonitorStateVersion = 2

type notificationStateLoadResult struct {
	State           notificationMonitorState
	Missing         bool
	Recovered       bool
	QuarantinedPath string
	Warning         error
}

type notificationMonitorState struct {
	Version      int                                      `json:"version"`
	Bootstrapped bool                                     `json:"bootstrapped,omitempty"`
	SourceCursor eventstore.DAGRunCursor                  `json:"source_cursor"`
	Destinations map[string]*notificationDestinationState `json:"destinations,omitempty"`
}

type notificationDestinationState struct {
	// Pending and Delivered are the version-1 tables. They are accepted on
	// decode (and migrated into Records) but are no longer written by the
	// current monitor.
	Pending   map[string]NotificationEvent `json:"pending,omitempty"`
	Delivered map[string]time.Time         `json:"delivered,omitempty"`

	// Records is the version-2 per-event delivery lifecycle table.
	Records map[string]*notificationDeliveryRecord `json:"records,omitempty"`
}

func newNotificationMonitorState() notificationMonitorState {
	return notificationMonitorState{
		Version:      notificationMonitorStateVersion,
		SourceCursor: eventstore.DAGRunCursor{CommittedOffsets: make(map[string]int64)},
		Destinations: make(map[string]*notificationDestinationState),
	}
}

func (s *notificationMonitorState) normalize() {
	if s.Version == 0 {
		s.Version = notificationMonitorStateVersion
	}
	s.SourceCursor = s.SourceCursor.Normalize()
	if s.Destinations == nil {
		s.Destinations = make(map[string]*notificationDestinationState)
	}
	for key, destination := range s.Destinations {
		if destination == nil {
			delete(s.Destinations, key)
			continue
		}
		destination.migrateLegacyTablesLocked()
		if destination.Records == nil {
			destination.Records = make(map[string]*notificationDeliveryRecord)
		}
	}
}

// migrateLegacyTablesLocked converts version-1 pending/delivered tables into
// lifecycle records without losing information. Pending events become
// immediately deliverable pending records; delivered timestamps become
// delivered records. When both tables reference the same key, the pending
// snapshot is authoritative because that event still has work to do.
func (d *notificationDestinationState) migrateLegacyTablesLocked() {
	if len(d.Pending) == 0 && len(d.Delivered) == 0 {
		return
	}
	if d.Records == nil {
		d.Records = make(map[string]*notificationDeliveryRecord, len(d.Pending)+len(d.Delivered))
	}
	now := time.Now().UTC()
	for key, deliveredAt := range d.Delivered {
		if _, exists := d.Records[key]; exists {
			continue
		}
		d.Records[key] = newDeliveredDeliveryRecord(key, deliveredAt.UTC())
	}
	for key, event := range d.Pending {
		observedAt := event.ObservedAt
		if observedAt.IsZero() {
			observedAt = now
		}
		d.Records[key] = newPendingDeliveryRecord(event, observedAt)
	}
	d.Pending = nil
	d.Delivered = nil
}

type notificationStateStore struct {
	store StateStore
}

func newNotificationStateStore(store StateStore) *notificationStateStore {
	if store == nil {
		return nil
	}
	return &notificationStateStore{store: store}
}

func (s *notificationStateStore) Load(ctx context.Context) notificationStateLoadResult {
	result := notificationStateLoadResult{State: newNotificationMonitorState()}
	if s == nil {
		return result
	}

	data, found, err := s.store.Load(ctx)
	if err != nil {
		result.Recovered = true
		result.QuarantinedPath, result.Warning = s.recoverUnreadableState(ctx, fmt.Errorf("read notification state: %w", err))
		return result
	}
	if !found {
		result.Missing = true
		return result
	}

	state, err := decodeNotificationMonitorState(data)
	if err != nil {
		result.Recovered = true
		result.QuarantinedPath, result.Warning = s.recoverUnreadableState(ctx, err)
		return result
	}
	result.State = state
	return result
}

func (s *notificationStateStore) IsBootstrapped(ctx context.Context) bool {
	if s == nil {
		return false
	}

	data, found, err := s.store.Load(ctx)
	if err != nil || !found {
		return false
	}

	state, err := decodeNotificationMonitorState(data)
	return err == nil && state.Bootstrapped
}

func (s *notificationStateStore) Save(ctx context.Context, state notificationMonitorState) error {
	if s == nil {
		return nil
	}
	state.normalize()
	state.Version = notificationMonitorStateVersion

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal notification state: %w", err)
	}

	return s.store.Save(ctx, data)
}

func (s *notificationStateStore) recoverUnreadableState(ctx context.Context, err error) (string, error) {
	quarantinedPath, quarantineErr := s.store.Quarantine(ctx)
	if quarantineErr != nil {
		return "", fmt.Errorf("%w (quarantine failed: %v)", err, quarantineErr)
	}
	return quarantinedPath, err
}

func decodeNotificationMonitorState(data []byte) (notificationMonitorState, error) {
	state := newNotificationMonitorState()
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode notification state: %w", err)
	}
	switch state.Version {
	case 0, 1, notificationMonitorStateVersion:
	default:
		return state, fmt.Errorf("unsupported notification state version %d", state.Version)
	}

	state.normalize()
	state.Version = notificationMonitorStateVersion
	return state, nil
}
