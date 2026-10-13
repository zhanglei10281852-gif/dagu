// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dagucloud/dagu/v2/internal/persis"
	"github.com/dagucloud/dagu/v2/internal/queue"
)

const queuePauseRecordSuffix = ".queue-pause"

type queuePauseRecord struct {
	Version  int64     `json:"version"`
	Paused   bool      `json:"paused"`
	PausedAt time.Time `json:"pausedAt,omitempty"`
	PausedBy string    `json:"pausedBy,omitempty"`
	Reason   string    `json:"reason,omitempty"`
}

type queuePauseCacheEntry struct {
	record queuePauseRecord
	token  string
}

var _ queue.PauseStore = (*QueuePauseStore)(nil)

// QueuePauseStore persists one pause record per queue inside the queue
// collection. Records are addressed as "{queueName}/.queue-pause" and never
// collide with queued items, which use an "item_" prefix.
//
// Reads are served from a per-queue cache validated against the backing
// record's version token, so a pause written by another process is observed
// without decoding the record on every scan.
type QueuePauseStore struct {
	col persis.Collection

	mu    sync.Mutex
	cache map[string]*queuePauseCacheEntry
}

// NewQueuePauseStore creates a queue pause store backed by col.
func NewQueuePauseStore(col persis.Collection) *QueuePauseStore {
	return &QueuePauseStore{
		col:   col,
		cache: make(map[string]*queuePauseCacheEntry),
	}
}

// Get returns the pause state of the named queue. A missing record reports the
// zero value (not paused). A corrupt or unreadable record reports an error so
// dispatch paths fail closed.
func (s *QueuePauseStore) Get(ctx context.Context, queueName string) (queue.PauseState, error) {
	if queueName == "" {
		return queue.PauseState{}, fmt.Errorf("queue pause store: queue name is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.loadLocked(ctx, queueName)
	if err != nil {
		return queue.PauseState{}, err
	}
	return pauseStateFromRecord(queueName, record), nil
}

// SetPause pauses or resumes the named queue. Repeating an already-applied
// state is idempotent and does not bump the generation.
func (s *QueuePauseStore) SetPause(
	ctx context.Context,
	queueName string,
	paused bool,
	actor, reason string,
) (queue.PauseState, error) {
	if queueName == "" {
		return queue.PauseState{}, fmt.Errorf("queue pause store: queue name is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result queue.PauseState
	err := retryConflict(ctx, func(ctx context.Context) error {
		persisted, decoded, corrupt, err := s.readRecordForWrite(ctx, queueName)
		if err != nil {
			return err
		}

		var next queuePauseRecord
		switch {
		case corrupt:
			// Repair a damaged record with the operator-requested state so a
			// corrupt pause flag can never wedge the queue permanently.
			next = queuePauseRecord{Version: 1, Paused: paused}
			if paused {
				next.PausedAt = time.Now().UTC()
				next.PausedBy = actor
				next.Reason = reason
			}
		default:
			found := persisted != nil
			current := queuePauseRecord{}
			if found {
				current = decoded
			}
			var changed bool
			next, changed = nextQueuePauseRecord(current, found, paused, actor, reason)
			if !changed {
				// Idempotent no-op: keep the persisted generation untouched.
				result = pauseStateFromRecord(queueName, next)
				return nil
			}
		}

		if corrupt {
			if err := s.col.Put(ctx, s.toPersistRecord(queueName, &next)); err != nil {
				return err
			}
		} else if err := createOrSwap(ctx, s.col, persisted, s.toPersistRecord(queueName, &next)); err != nil {
			return err
		}

		s.cacheLocked(ctx, queueName, next)
		result = pauseStateFromRecord(queueName, next)
		return nil
	})
	if err != nil {
		// The write failed; drop any cached guess so the next read reloads the
		// preserved on-disk state.
		s.deleteCacheLocked(queueName)
		return queue.PauseState{}, fmt.Errorf("queue pause store: set %q: %w", queueName, err)
	}
	return result, nil
}

// List returns pause state for every queue with a pause record.
func (s *QueuePauseStore) List(ctx context.Context) ([]queue.PauseState, error) {
	ids, err := s.pauseRecordIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("queue pause store: list: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]queue.PauseState, 0, len(ids))
	for _, id := range ids {
		queueName, ok := queueNameFromPauseRecordID(id)
		if !ok {
			continue
		}
		record, err := s.loadLocked(ctx, queueName)
		if err != nil {
			return nil, err
		}
		result = append(result, pauseStateFromRecord(queueName, record))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].QueueName < result[j].QueueName })
	return result, nil
}

// loadLocked returns the current record, or the zero value when absent. It
// must be called with s.mu held.
func (s *QueuePauseStore) loadLocked(ctx context.Context, queueName string) (queuePauseRecord, error) {
	if cached, ok := s.cachedLocked(ctx, queueName); ok {
		return cached, nil
	}

	var record queuePauseRecord
	found, err := NewSingleRecord[queuePauseRecord](s.col, queuePauseRecordID(queueName)).Load(ctx, &record)
	if err != nil {
		if errors.Is(err, ErrCorrupt) {
			return queuePauseRecord{}, fmt.Errorf("%w: queue %q: %w", queue.ErrQueuePauseUnavailable, queueName, err)
		}
		return queuePauseRecord{}, fmt.Errorf("queue %q: %w", queueName, err)
	}
	if !found {
		record = queuePauseRecord{}
	}
	s.cacheLocked(ctx, queueName, record)
	return record, nil
}

// readRecordForWrite loads the raw current record, distinguishing a missing
// record from a corrupt one so callers can force-replace damaged state.
func (s *QueuePauseStore) readRecordForWrite(ctx context.Context, queueName string) (persisted *persis.Record, decoded queuePauseRecord, corrupt bool, err error) {
	rec, getErr := s.col.Get(ctx, queuePauseRecordID(queueName))
	if errors.Is(getErr, persis.ErrNotFound) {
		return nil, queuePauseRecord{}, false, nil
	}
	if getErr != nil {
		return nil, queuePauseRecord{}, false, getErr
	}
	if decodeErr := persis.Decode(rec, &decoded); decodeErr != nil {
		return rec, queuePauseRecord{}, true, nil
	}
	return rec, decoded, false, nil
}

func (s *QueuePauseStore) cachedLocked(ctx context.Context, queueName string) (queuePauseRecord, bool) {
	cached, ok := s.cache[queueName]
	if !ok || cached == nil {
		return queuePauseRecord{}, false
	}
	token, supported, err := collectionRecordVersion(ctx, s.col, queuePauseRecordID(queueName))
	if errors.Is(err, persis.ErrNotFound) {
		token = ""
	}
	if err != nil || !supported || token != cached.token {
		if err != nil {
			s.deleteCacheLocked(queueName)
		}
		return queuePauseRecord{}, false
	}
	return cached.record, true
}

func (s *QueuePauseStore) cacheLocked(ctx context.Context, queueName string, record queuePauseRecord) {
	token, supported, err := collectionRecordVersion(ctx, s.col, queuePauseRecordID(queueName))
	if errors.Is(err, persis.ErrNotFound) {
		token = ""
	}
	if err != nil || !supported {
		s.deleteCacheLocked(queueName)
		return
	}
	s.cache[queueName] = &queuePauseCacheEntry{record: record, token: token}
}

func (s *QueuePauseStore) deleteCacheLocked(queueName string) {
	delete(s.cache, queueName)
}

func (s *QueuePauseStore) toPersistRecord(queueName string, record *queuePauseRecord) *persis.Record {
	data, err := persis.Encode(record)
	if err != nil {
		// queuePauseRecord has only JSON-safe fields; an encode failure is a
		// programming error, not a runtime condition.
		panic(fmt.Sprintf("queue pause store: encode: %v", err))
	}
	now := time.Now().UTC()
	return &persis.Record{
		ID:        queuePauseRecordID(queueName),
		Data:      data,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (s *QueuePauseStore) pauseRecordIDs(ctx context.Context) ([]string, error) {
	if col, ok := s.col.(recordIDsCollection); ok {
		return col.RecordIDs(ctx, "")
	}
	recs, err := listAll(ctx, s.col, persis.ListQuery{})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(recs))
	for _, rec := range recs {
		ids = append(ids, rec.ID)
	}
	return ids, nil
}

// nextQueuePauseRecord computes the record to persist for an idempotent state
// change. changed is false when the request repeats the current state and
// needs no write beyond an optional metadata refresh.
func nextQueuePauseRecord(
	current queuePauseRecord,
	currentFound bool,
	paused bool,
	actor, reason string,
) (queuePauseRecord, bool) {
	if paused {
		if current.Paused {
			// Keep the original generation and timestamp; only annotate who
			// and why so repeated pauses do not disturb parked scans.
			next := current
			next.PausedBy = actor
			next.Reason = reason
			if next.Version == 0 {
				next.Version = 1
			}
			return next, next != current
		}
		return queuePauseRecord{
			Version:  current.Version + 1,
			Paused:   true,
			PausedAt: time.Now().UTC(),
			PausedBy: actor,
			Reason:   reason,
		}, true
	}

	if !currentFound || !current.Paused {
		// Resuming a queue that is not paused: nothing to write.
		return current, false
	}
	return queuePauseRecord{
		Version: current.Version + 1,
		Paused:  false,
	}, true
}

func pauseStateFromRecord(queueName string, record queuePauseRecord) queue.PauseState {
	return queue.PauseState{
		QueueName: queueName,
		Paused:    record.Paused,
		Version:   record.Version,
		PausedAt:  record.PausedAt,
		PausedBy:  record.PausedBy,
		Reason:    record.Reason,
	}
}

func queuePauseRecordID(name string) string {
	return queuePrefix(name) + queuePauseRecordSuffix
}

func queueNameFromPauseRecordID(id string) (string, bool) {
	queueName, suffix, ok := splitQueueRecordID(id)
	if !ok || suffix != queuePauseRecordSuffix {
		return "", false
	}
	if strings.ContainsAny(queueName, "/") {
		return "", false
	}
	return queueName, true
}
