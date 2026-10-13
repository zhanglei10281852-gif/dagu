// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package queue

import (
	"context"
	"errors"
	"time"
)

// ErrQueuePauseUnavailable reports that durable queue pause state could not be
// read. Dispatch paths treat it as fail-closed: a queue whose pause state is
// unknown must not claim new runs.
var ErrQueuePauseUnavailable = errors.New("queue pause state unavailable")

// PauseState is the durable pause state of a single queue.
//
// A zero value means the queue has never been paused. Version is a monotonic
// generation counter bumped on every actual pause or resume transition.
// Dispatch scans carry the observed generation so a pause that lands after
// item selection is still detected at the final handoff.
type PauseState struct {
	// QueueName is populated by List; it is empty for single-queue Get calls.
	QueueName string
	Paused    bool
	Version   int64
	PausedAt  time.Time
	PausedBy  string
	Reason    string
}

// PauseStore persists per-queue pause state. Implementations must be safe for
// concurrent use and make writes visible to other processes sharing the
// backing storage. Get and List return independent snapshots that callers may
// mutate.
type PauseStore interface {
	// Get returns the pause state of the named queue. A queue that has never
	// been paused returns the zero value and no error. A read failure must be
	// reported so callers can fail closed rather than dispatch blindly.
	Get(ctx context.Context, queueName string) (PauseState, error)
	// SetPause pauses or resumes the named queue. It is idempotent: repeating
	// the same state does not bump Version. The returned state reflects the
	// persisted value. A write failure preserves the previous state and
	// returns an error.
	SetPause(ctx context.Context, queueName string, paused bool, actor, reason string) (PauseState, error)
	// List returns the pause state of every queue that has a pause record,
	// including queues that are currently resumed. Queues that were never
	// paused are absent.
	List(ctx context.Context) ([]PauseState, error)
}
