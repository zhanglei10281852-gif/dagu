// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"time"
)

// ErrDeliveryControllerInactive indicates that no monitor in this process
// owns the delivery lease, so a mutating management call cannot be served.
var ErrDeliveryControllerInactive = errors.New("notification delivery controller is not active on this process")

// Delivery list pagination limits.
const (
	DefaultDeliveryRecordsPageSize = 50
	MaxDeliveryRecordsPageSize     = 200
)

// DeliveryRecordView is the management-facing projection of one record.
type DeliveryRecordView struct {
	Destination string
	EventKey    string
	DAGName     string
	DAGRunID    string
	AttemptID   string
	EventType   string
	RunStatus   uint8
	DAGFile     string
	ObservedAt  time.Time

	Status     DeliveryStatus
	Attempts   int
	Generation int

	LastErrorCategory DeliveryErrorCategory
	LastErrorSummary  string
	LastAttemptAt     *time.Time
	NextAttemptAt     *time.Time

	DeliveredAt      *time.Time
	DeadLetterAt     *time.Time
	DeadLetterReason string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// DeliveryRecordFilter selects records for a list query.
type DeliveryRecordFilter struct {
	Statuses    []DeliveryStatus
	Destination string
}

// DeliveryRecordPage is one page of delivery records.
type DeliveryRecordPage struct {
	Records    []DeliveryRecordView
	NextCursor string
}

// DeliveryRecordRef identifies one record by destination and event key.
type DeliveryRecordRef struct {
	Destination string
	EventKey    string
}

// Redrive outcomes reported per requested record.
const (
	RedriveOutcomeRedriven       = "redriven"
	RedriveOutcomeAlreadyPending = "already_pending"
	RedriveOutcomeDelivered      = "delivered"
	RedriveOutcomeNotFound       = "not_found"
)

// DeliveryRedriveResult is the stable per-record outcome of a redrive call.
type DeliveryRedriveResult struct {
	RecordID    string
	Destination string
	EventKey    string
	Outcome     string
	Status      DeliveryStatus
	Generation  int
}

type deliveryRecordCursor struct {
	UpdatedAtNano int64  `json:"u"`
	Destination   string `json:"d"`
	EventKey      string `json:"k"`
}

// EncodeDeliveryRecordID packs a destination/event-key pair into an opaque
// URL-safe identifier.
func EncodeDeliveryRecordID(destination, eventKey string) string {
	raw, _ := json.Marshal(deliveryRecordCursor{
		Destination: destination,
		EventKey:    eventKey,
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeDeliveryRecordID reverses EncodeDeliveryRecordID.
func DecodeDeliveryRecordID(recordID string) (string, string, error) {
	if recordID == "" {
		return "", "", errors.New("delivery record id is empty")
	}
	raw, err := base64.RawURLEncoding.DecodeString(recordID)
	if err != nil {
		return "", "", errors.New("delivery record id is malformed")
	}
	var cursor deliveryRecordCursor
	if err := json.Unmarshal(raw, &cursor); err != nil || cursor.Destination == "" || cursor.EventKey == "" {
		return "", "", errors.New("delivery record id is malformed")
	}
	return cursor.Destination, cursor.EventKey, nil
}

func encodeDeliveryPageCursor(view DeliveryRecordView) string {
	raw, _ := json.Marshal(deliveryRecordCursor{
		UpdatedAtNano: view.UpdatedAt.UnixNano(),
		Destination:   view.Destination,
		EventKey:      view.EventKey,
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeDeliveryPageCursor(cursor string) (*deliveryRecordCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, errors.New("pagination cursor is malformed")
	}
	var decoded deliveryRecordCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, errors.New("pagination cursor is malformed")
	}
	return &decoded, nil
}

func (m *NotificationMonitor) loadCommittedStateLocked(ctx context.Context) notificationMonitorState {
	if m.stateStore != nil {
		result := m.stateStore.Load(ctx)
		if !result.Missing {
			return result.State
		}
	}
	return cloneNotificationMonitorState(m.state)
}

// ListDeliveryRecords reads the latest committed state and returns a filtered,
// keyset-paginated page ordered by updatedAt descending (destination/key as a
// stable tie-break).
func (m *NotificationMonitor) ListDeliveryRecords(ctx context.Context, filter DeliveryRecordFilter, cursor string, limit int) (DeliveryRecordPage, error) {
	if limit <= 0 {
		limit = DefaultDeliveryRecordsPageSize
	}
	if limit > MaxDeliveryRecordsPageSize {
		limit = MaxDeliveryRecordsPageSize
	}
	pageCursor, err := decodeDeliveryPageCursor(cursor)
	if err != nil {
		return DeliveryRecordPage{}, err
	}
	statuses := make(map[DeliveryStatus]struct{}, len(filter.Statuses))
	for _, status := range filter.Statuses {
		statuses[status] = struct{}{}
	}

	m.stateMu.Lock()
	state := m.loadCommittedStateLocked(ctx)
	m.stateMu.Unlock()

	views := make([]DeliveryRecordView, 0)
	for destination, destState := range state.Destinations {
		if destState == nil || (filter.Destination != "" && filter.Destination != destination) {
			continue
		}
		for key, record := range destState.Records {
			if record == nil {
				continue
			}
			if _, ok := statuses[record.Status]; len(statuses) > 0 && !ok {
				continue
			}
			views = append(views, deliveryRecordView(destination, key, record))
		}
	}

	slices.SortFunc(views, func(a, b DeliveryRecordView) int {
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			if a.UpdatedAt.After(b.UpdatedAt) {
				return -1
			}
			return 1
		}
		if a.Destination != b.Destination {
			if a.Destination < b.Destination {
				return -1
			}
			return 1
		}
		if a.EventKey < b.EventKey {
			return -1
		}
		if a.EventKey > b.EventKey {
			return 1
		}
		return 0
	})

	if pageCursor != nil {
		filtered := views[:0]
		for _, view := range views {
			if recordAfterCursor(view, *pageCursor) {
				filtered = append(filtered, view)
			}
		}
		views = filtered
	}

	page := DeliveryRecordPage{Records: []DeliveryRecordView{}}
	if len(views) > limit {
		page.Records = append(page.Records, views[:limit]...)
		page.NextCursor = encodeDeliveryPageCursor(views[limit-1])
	} else {
		page.Records = append(page.Records, views...)
	}
	return page, nil
}

// recordAfterCursor reports whether view belongs to the page following cursor
// under updatedAt-desc + destination/key-asc ordering.
func recordAfterCursor(view DeliveryRecordView, cursor deliveryRecordCursor) bool {
	cursorTime := time.Unix(0, cursor.UpdatedAtNano)
	if !view.UpdatedAt.Equal(cursorTime) {
		return view.UpdatedAt.Before(cursorTime)
	}
	if view.Destination != cursor.Destination {
		return view.Destination > cursor.Destination
	}
	return view.EventKey > cursor.EventKey
}

// GetDeliveryRecord returns a single record by destination and event key.
func (m *NotificationMonitor) GetDeliveryRecord(ctx context.Context, destination, eventKey string) (DeliveryRecordView, bool, error) {
	m.stateMu.Lock()
	state := m.loadCommittedStateLocked(ctx)
	m.stateMu.Unlock()

	destState, ok := state.Destinations[destination]
	if !ok || destState == nil {
		return DeliveryRecordView{}, false, nil
	}
	record, ok := destState.Records[eventKey]
	if !ok || record == nil {
		return DeliveryRecordView{}, false, nil
	}
	return deliveryRecordView(destination, eventKey, record), true, nil
}

func deliveryRecordView(destination, key string, record *notificationDeliveryRecord) DeliveryRecordView {
	view := DeliveryRecordView{
		Destination:        destination,
		EventKey:           key,
		Status:             record.Status,
		Attempts:           record.Attempts,
		Generation:         record.Generation,
		LastErrorCategory:  record.LastErrorCategory,
		LastErrorSummary:   record.LastErrorSummary,
		LastAttemptAt:      cloneTimePtr(record.LastAttemptAt),
		NextAttemptAt:      cloneTimePtr(record.NextAttemptAt),
		DeliveredAt:        cloneTimePtr(record.DeliveredAt),
		DeadLetterAt:       cloneTimePtr(record.DeadLetterAt),
		DeadLetterReason:   record.DeadLetterReason,
		CreatedAt:          record.CreatedAt,
		UpdatedAt:          record.UpdatedAt,
	}
	if record.Summary != nil {
		view.EventKey = record.Summary.Key
		if view.EventKey == "" {
			view.EventKey = key
		}
		view.DAGName = record.Summary.DAGName
		view.DAGRunID = record.Summary.DAGRunID
		view.AttemptID = record.Summary.AttemptID
		view.EventType = string(record.Summary.Type)
		view.RunStatus = record.Summary.RunStatus
		view.DAGFile = record.Summary.DAGFile
		view.ObservedAt = record.Summary.ObservedAt
	}
	if record.Event != nil {
		view.EventKey = record.Event.Key
		view.ObservedAt = record.Event.ObservedAt
		view.DAGFile = record.Event.DAGFile
		view.EventType = string(record.Event.Type)
		if record.Event.Status != nil {
			view.DAGName = record.Event.Status.Name
			view.DAGRunID = record.Event.Status.DAGRunID
			view.AttemptID = record.Event.Status.AttemptID
			view.RunStatus = uint8(record.Event.Status.Status)
		}
	}
	if view.EventKey == "" {
		view.EventKey = key
	}
	return view
}

// RedriveDeliveryRecords reopens dead-lettered records as a fresh pending
// generation. It executes under the lease fence and persists the new
// generation before enqueueing events into the batcher.
func (m *NotificationMonitor) RedriveDeliveryRecords(ctx context.Context, refs []DeliveryRecordRef) ([]DeliveryRedriveResult, error) {
	if !m.ensureNotificationLockOwnership("Notification lock is not held; cannot redrive delivery records") {
		return nil, ErrDeliveryControllerInactive
	}

	now := time.Now().UTC()
	results := make([]DeliveryRedriveResult, 0, len(refs))
	var toEnqueue []queuedNotification

	m.stateMu.Lock()
	persisted := m.applyStateTransitionLocked(ctx, func(candidate *notificationMonitorState) bool {
		changed := false
		for _, ref := range refs {
			if ref.Destination == "" || ref.EventKey == "" {
				continue
			}
			result := DeliveryRedriveResult{
				RecordID:    EncodeDeliveryRecordID(ref.Destination, ref.EventKey),
				Destination: ref.Destination,
				EventKey:    ref.EventKey,
			}
			destState, ok := candidate.Destinations[ref.Destination]
			if !ok || destState == nil {
				result.Outcome = RedriveOutcomeNotFound
				results = append(results, result)
				continue
			}
			record, ok := destState.Records[ref.EventKey]
			if !ok || record == nil {
				result.Outcome = RedriveOutcomeNotFound
				results = append(results, result)
				continue
			}

			result.Status = record.Status
			result.Generation = record.Generation
			switch record.Status {
			case DeliveryStatusDeadLetter:
				event, ok := record.notificationEvent()
				if !ok {
					result.Outcome = RedriveOutcomeNotFound
					results = append(results, result)
					continue
				}
				reopened := resetRecordToPending(record, event, now)
				destState.Records[ref.EventKey] = reopened
				result.Outcome = RedriveOutcomeRedriven
				result.Status = DeliveryStatusPending
				result.Generation = reopened.Generation
				toEnqueue = append(toEnqueue, queuedNotification{destination: ref.Destination, event: event})
				changed = true
			case DeliveryStatusPending:
				result.Outcome = RedriveOutcomeAlreadyPending
			case DeliveryStatusDelivered:
				result.Outcome = RedriveOutcomeDelivered
			default:
				result.Outcome = RedriveOutcomeNotFound
			}
			results = append(results, result)
		}
		return changed
	})
	m.stateMu.Unlock()

	if !persisted {
		return nil, errors.New("failed to persist notification redrive state")
	}

	for _, item := range toEnqueue {
		m.enqueueBatch(item.destination, item.event)
	}
	return results, nil
}

// DeliveryController serves management operations: reads are served from the
// local monitor's committed state (valid even while this process is the
// standby lease holder), while mutating redrives are routed to whichever
// monitor currently owns the lease.
type DeliveryController struct {
	local    *NotificationMonitor
	registry *MonitorRegistry
}

// NewDeliveryController wires a local monitor and the shared active-monitor
// registry.
func NewDeliveryController(local *NotificationMonitor, registry *MonitorRegistry) *DeliveryController {
	return &DeliveryController{local: local, registry: registry}
}

// ListDeliveryRecords delegates to the local monitor's state file.
func (c *DeliveryController) ListDeliveryRecords(ctx context.Context, filter DeliveryRecordFilter, cursor string, limit int) (DeliveryRecordPage, error) {
	return c.local.ListDeliveryRecords(ctx, filter, cursor, limit)
}

// GetDeliveryRecord delegates to the local monitor's state file.
func (c *DeliveryController) GetDeliveryRecord(ctx context.Context, destination, eventKey string) (DeliveryRecordView, bool, error) {
	return c.local.GetDeliveryRecord(ctx, destination, eventKey)
}

// RedriveDeliveryRecords routes to the lease owner. It returns
// ErrDeliveryControllerInactive when no monitor in this process owns the
// lease.
func (c *DeliveryController) RedriveDeliveryRecords(ctx context.Context, refs []DeliveryRecordRef) ([]DeliveryRedriveResult, error) {
	active := c.local
	if c.registry != nil {
		active = c.registry.Active()
	}
	if active == nil {
		return nil, ErrDeliveryControllerInactive
	}
	return active.RedriveDeliveryRecords(ctx, refs)
}
