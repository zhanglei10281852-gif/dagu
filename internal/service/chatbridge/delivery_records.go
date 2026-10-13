// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"time"

	"github.com/dagucloud/dagu/v2/internal/eventstore"
)

// DeliveryStatus is the lifecycle state of one event delivery record.
type DeliveryStatus string

const (
	DeliveryStatusPending    DeliveryStatus = "pending"
	DeliveryStatusDelivered  DeliveryStatus = "delivered"
	DeliveryStatusDeadLetter DeliveryStatus = "dead_letter"
)

// DeliveryErrorCategory classifies the most recent delivery failure.
type DeliveryErrorCategory string

const (
	DeliveryErrorRateLimited   DeliveryErrorCategory = "rate_limited"
	DeliveryErrorServerError   DeliveryErrorCategory = "server_error"
	DeliveryErrorNetwork       DeliveryErrorCategory = "network"
	DeliveryErrorClientError   DeliveryErrorCategory = "client_error"
	DeliveryErrorConfiguration DeliveryErrorCategory = "configuration"
	DeliveryErrorUnknown       DeliveryErrorCategory = "unknown"
)

// Dead-letter reasons.
const (
	deadLetterReasonClientError  = "client_error"
	deadLetterReasonAttemptLimit = "attempt_limit"
	deadLetterReasonConfiguration = "configuration"
)

// deliveryEventSummary is the compact event information retained after an
// event reaches a terminal state, when the full status snapshot is no longer
// needed for rendering or redrive.
type deliveryEventSummary struct {
	Key        string               `json:"key,omitempty"`
	Type       eventstore.EventType `json:"type,omitempty"`
	DAGName    string               `json:"dag_name,omitempty"`
	RunStatus  uint8                `json:"run_status,omitempty"`
	DAGRunID   string               `json:"dag_run_id,omitempty"`
	AttemptID  string               `json:"attempt_id,omitempty"`
	DAGFile    string               `json:"dag_file,omitempty"`
	ObservedAt time.Time            `json:"observed_at,omitempty"`
}

// notificationDeliveryRecord tracks the delivery lifecycle of one event for
// one destination. The full event snapshot is retained while the record is
// pending or dead-lettered (required for batch rendering and manual redrive)
// and released once delivered.
type notificationDeliveryRecord struct {
	Status  DeliveryStatus      `json:"status"`
	Event   *NotificationEvent  `json:"event,omitempty"`
	Summary *deliveryEventSummary `json:"summary,omitempty"`

	Attempts   int `json:"attempts,omitempty"`
	Generation int `json:"generation,omitempty"`

	LastErrorCategory DeliveryErrorCategory `json:"last_error_category,omitempty"`
	LastErrorSummary  string                `json:"last_error_summary,omitempty"`
	LastAttemptAt     *time.Time            `json:"last_attempt_at,omitempty"`
	NextAttemptAt     *time.Time            `json:"next_attempt_at,omitempty"`

	DeliveredAt      *time.Time `json:"delivered_at,omitempty"`
	DeadLetterAt     *time.Time `json:"dead_letter_at,omitempty"`
	DeadLetterReason string     `json:"dead_letter_reason,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func newPendingDeliveryRecord(event NotificationEvent, now time.Time) *notificationDeliveryRecord {
	event = cloneNotificationEvent(event)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	observedAt := event.ObservedAt
	if observedAt.IsZero() {
		observedAt = now
	}
	return &notificationDeliveryRecord{
		Status:     DeliveryStatusPending,
		Event:      &event,
		Generation: 1,
		CreatedAt:  observedAt,
		UpdatedAt:  now,
	}
}

func newDeliveredDeliveryRecord(key string, deliveredAt time.Time) *notificationDeliveryRecord {
	if deliveredAt.IsZero() {
		deliveredAt = time.Now().UTC()
	}
	return &notificationDeliveryRecord{
		Status:      DeliveryStatusDelivered,
		Summary:     &deliveryEventSummary{Key: key},
		DeliveredAt: &deliveredAt,
		CreatedAt:   deliveredAt,
		UpdatedAt:   deliveredAt,
	}
}

func (r *notificationDeliveryRecord) eventKey() string {
	if r == nil {
		return ""
	}
	if r.Event != nil && r.Event.Key != "" {
		return r.Event.Key
	}
	if r.Summary != nil {
		return r.Summary.Key
	}
	return ""
}

func (r *notificationDeliveryRecord) observedAt() time.Time {
	if r == nil {
		return time.Time{}
	}
	if r.Event != nil && !r.Event.ObservedAt.IsZero() {
		return r.Event.ObservedAt
	}
	if r.Summary != nil {
		return r.Summary.ObservedAt
	}
	return r.CreatedAt
}

// notificationEvent returns the full pending/dead-letter event snapshot, or
// false for terminal delivered records that no longer carry one.
func (r *notificationDeliveryRecord) notificationEvent() (NotificationEvent, bool) {
	if r != nil && r.Event != nil {
		return *r.Event, true
	}
	return NotificationEvent{}, false
}

func (r *notificationDeliveryRecord) isPending() bool {
	return r != nil && r.Status == DeliveryStatusPending && r.Event != nil
}

// dueNotificationEvent returns the pending event when it is eligible for
// (re)delivery at now: pending records without a scheduled time are always
// due, while records waiting on a backoff schedule are due at or after it.
func (r *notificationDeliveryRecord) dueNotificationEvent(now time.Time) (NotificationEvent, bool) {
	if !r.isPending() {
		return NotificationEvent{}, false
	}
	if r.NextAttemptAt != nil && r.NextAttemptAt.After(now) {
		return NotificationEvent{}, false
	}
	if r.Event.Status == nil || r.Event.Key == "" {
		return NotificationEvent{}, false
	}
	return *r.Event, true
}

// newDeliveredRecordFromEvent acknowledges an event that has no live lifecycle
// record, retaining only the compact event summary.
func newDeliveredRecordFromEvent(event NotificationEvent, now time.Time) *notificationDeliveryRecord {
	createdAt := event.ObservedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	return &notificationDeliveryRecord{
		Status:      DeliveryStatusDelivered,
		Summary:     compactDeliveryEventSummary(event),
		Generation:  1,
		DeliveredAt: &now,
		CreatedAt:   createdAt,
		UpdatedAt:   now,
	}
}

// deliveredRecordFromPending converts an acknowledged pending record into a
// delivered one, preserving lifecycle metadata while releasing the potentially
// large status snapshot.
func deliveredRecordFromPending(record *notificationDeliveryRecord, event NotificationEvent, now time.Time) *notificationDeliveryRecord {
	createdAt := record.CreatedAt
	if createdAt.IsZero() {
		createdAt = event.ObservedAt
	}
	return &notificationDeliveryRecord{
		Status:          DeliveryStatusDelivered,
		Summary:         compactDeliveryEventSummary(event),
		Attempts:        record.Attempts,
		Generation:      record.Generation,
		DeliveredAt:     &now,
		CreatedAt:       createdAt,
		UpdatedAt:       now,
	}
}

// compactSummary extracts display-only event information before the full
// status snapshot is released.
func compactDeliveryEventSummary(event NotificationEvent) *deliveryEventSummary {
	summary := &deliveryEventSummary{
		Key:        event.Key,
		Type:       event.Type,
		DAGFile:    event.DAGFile,
		ObservedAt: event.ObservedAt,
	}
	if event.Status != nil {
		summary.DAGName = event.Status.Name
		summary.RunStatus = uint8(event.Status.Status)
		summary.DAGRunID = event.Status.DAGRunID
		summary.AttemptID = event.Status.AttemptID
	}
	return summary
}

func cloneDeliveryRecord(record *notificationDeliveryRecord) *notificationDeliveryRecord {
	if record == nil {
		return nil
	}
	clone := *record
	if record.Event != nil {
		event := cloneNotificationEvent(*record.Event)
		clone.Event = &event
	}
	if record.Summary != nil {
		summary := *record.Summary
		clone.Summary = &summary
	}
	clone.LastAttemptAt = cloneTimePtr(record.LastAttemptAt)
	clone.NextAttemptAt = cloneTimePtr(record.NextAttemptAt)
	clone.DeliveredAt = cloneTimePtr(record.DeliveredAt)
	clone.DeadLetterAt = cloneTimePtr(record.DeadLetterAt)
	return &clone
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
