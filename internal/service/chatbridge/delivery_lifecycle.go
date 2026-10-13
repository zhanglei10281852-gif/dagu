// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"time"
)

// deliveryRand jitters persistent backoff intervals. The global source is
// concurrency-safe and auto-seeded.
var deliveryRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// deliveryBackoff computes the wait before the next automatic attempt after
// attempts failed attempts: initial * 2^(attempts-1), capped at max, with
// +-20% jitter to avoid synchronized retries.
func (m *NotificationMonitor) deliveryBackoff(attempts int) time.Duration {
	initial := m.cfg.DeliveryInitialBackoff
	maxBackoff := m.cfg.DeliveryMaxBackoff
	if initial <= 0 {
		initial = DefaultNotificationDeliveryInitialBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = DefaultNotificationDeliveryMaxBackoff
	}
	interval := float64(initial) * math.Pow(2, float64(max(0, attempts-1)))
	if interval > float64(maxBackoff) {
		interval = float64(maxBackoff)
	}
	factor := 1.0
	if m.deliveryJitter != nil {
		factor = 0.8 + 0.4*m.deliveryJitter()
	}
	wait := time.Duration(interval * factor)
	if wait <= 0 {
		wait = time.Second
	}
	return wait
}

// recordBatchDeliveryFailure accounts one delivery attempt for every live
// pending record represented by the batch, scheduling persistent backoff or
// moving the record to the dead-letter. The transition is durable before it is
// visible in memory, mirroring every other monitor state change.
func (m *NotificationMonitor) recordBatchDeliveryFailure(ctx context.Context, destination string, batch NotificationBatch, outcome DeliveryOutcome) {
	now := time.Now().UTC()
	summary := outcome.Summary
	if summary == "" {
		summary = fallbackDeliveryFailureSummary
	}
	category := outcome.Category
	if category == "" {
		category = DeliveryErrorUnknown
	}

	m.stateMu.Lock()
	m.applyStateTransitionLocked(ctx, func(candidate *notificationMonitorState) bool {
		destState := destinationState(candidate, destination)
		changed := false
		for _, event := range batch.Events {
			if event.Key == "" {
				continue
			}
			record, ok := destState.Records[event.Key]
			if !ok || record == nil || record.Status != DeliveryStatusPending {
				continue
			}

			record.Attempts++
			record.LastAttemptAt = &now
			record.LastErrorCategory = category
			record.LastErrorSummary = summary
			record.UpdatedAt = now
			record.NextAttemptAt = nil
			changed = true

			if !outcome.Retryable {
				moveRecordToDeadLetter(record, now, deadLetterReasonForCategory(category))
				continue
			}
			if m.cfg.MaxDeliveryAttempts > 0 && record.Attempts >= m.cfg.MaxDeliveryAttempts {
				moveRecordToDeadLetter(record, now, deadLetterReasonAttemptLimit)
				continue
			}

			wait := m.deliveryBackoff(record.Attempts)
			if outcome.RetryAfter > wait {
				wait = outcome.RetryAfter
			}
			nextAttemptAt := now.Add(wait)
			record.NextAttemptAt = &nextAttemptAt
		}
		// Keep terminal tables bounded immediately when new records land in
		// them rather than waiting for the next eviction tick.
		if pruneTerminalRecords(destState, now, m.cfg.SeenTTL, m.cfg.DeadLetterTTL, m.cfg.DeadLetterLimit) {
			changed = true
		}
		return changed
	})
	m.stateMu.Unlock()
}

// moveRecordToDeadLetter stops automatic delivery and retains the full event
// snapshot for later manual redrive.
func moveRecordToDeadLetter(record *notificationDeliveryRecord, now time.Time, reason string) {
	record.Status = DeliveryStatusDeadLetter
	record.DeadLetterAt = &now
	record.DeadLetterReason = reason
	record.NextAttemptAt = nil
	record.UpdatedAt = now
}

func deadLetterReasonForCategory(category DeliveryErrorCategory) string {
	if category == DeliveryErrorConfiguration {
		return deadLetterReasonConfiguration
	}
	return deadLetterReasonClientError
}

// resetRecordToPending reopens a terminal record as a fresh pending generation,
// clearing attempt/failure state while preserving identity and event content.
// It is used for manual redrive and for a re-observed event whose prior record
// has dead-lettered.
func resetRecordToPending(record *notificationDeliveryRecord, event NotificationEvent, now time.Time) *notificationDeliveryRecord {
	generation := record.Generation
	if generation < 1 {
		generation = 1
	}
	createdAt := record.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	snapshot := cloneNotificationEvent(event)
	return &notificationDeliveryRecord{
		Status:     DeliveryStatusPending,
		Event:      &snapshot,
		Generation: generation + 1,
		CreatedAt:  createdAt,
		UpdatedAt:  now,
	}
}

// pruneTerminalRecords removes expired delivered/dead-letter records and trims
// dead-letter records beyond the per-destination cap (oldest first). It returns
// whether anything changed.
func pruneTerminalRecords(destState *notificationDestinationState, now time.Time, deliveredTTL, deadLetterTTL time.Duration, deadLetterLimit int) bool {
	if destState == nil {
		return false
	}
	changed := false
	for key, record := range destState.Records {
		if record == nil {
			delete(destState.Records, key)
			changed = true
			continue
		}
		switch record.Status {
		case DeliveryStatusDelivered:
			deliveredAt := record.DeliveredAt
			if deliveredAt == nil {
				deliveredAt = &record.UpdatedAt
			}
			if deliveredTTL > 0 && deliveredAt.Before(now.Add(-deliveredTTL)) {
				delete(destState.Records, key)
				changed = true
			}
		case DeliveryStatusDeadLetter:
			deadAt := record.DeadLetterAt
			if deadAt == nil {
				deadAt = &record.UpdatedAt
			}
			if deadLetterTTL > 0 && deadAt.Before(now.Add(-deadLetterTTL)) {
				delete(destState.Records, key)
				changed = true
			}
		}
	}

	if deadLetterLimit <= 0 {
		return changed
	}

	type deadCandidate struct {
		key  string
		when time.Time
	}
	candidates := make([]deadCandidate, 0)
	for key, record := range destState.Records {
		if record == nil || record.Status != DeliveryStatusDeadLetter {
			continue
		}
		when := record.DeadLetterAt
		if when == nil {
			when = &record.UpdatedAt
		}
		candidates = append(candidates, deadCandidate{key: key, when: when.UTC()})
	}
	if len(candidates) <= deadLetterLimit {
		return changed
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].when.Equal(candidates[j].when) {
			return candidates[i].when.Before(candidates[j].when)
		}
		return candidates[i].key < candidates[j].key
	})
	for _, candidate := range candidates[:len(candidates)-deadLetterLimit] {
		delete(destState.Records, candidate.key)
		changed = true
	}
	return changed
}

// evictTerminalRecords persists TTL/cap pruning across all destinations.
func (m *NotificationMonitor) evictTerminalRecords(ctx context.Context, now time.Time) {
	m.stateMu.Lock()
	m.applyStateTransitionLocked(ctx, func(candidate *notificationMonitorState) bool {
		changed := false
		for key, destination := range candidate.Destinations {
			if destination == nil {
				delete(candidate.Destinations, key)
				changed = true
				continue
			}
			if pruneTerminalRecords(destination, now, m.cfg.SeenTTL, m.cfg.DeadLetterTTL, m.cfg.DeadLetterLimit) {
				changed = true
			}
			if len(destination.Records) == 0 {
				delete(candidate.Destinations, key)
				changed = true
			}
		}
		return changed
	})
	m.stateMu.Unlock()
}
