// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package dagrun

import (
	"errors"
)

// Errors related to dag-run management
var (
	ErrDAGRunIDNotFound    = errors.New("dag-run ID not found")
	ErrDAGRunIDEmpty       = errors.New("dag-run ID is empty")
	ErrDAGRunAlreadyExists = errors.New("dag-run already exists")
	ErrDAGRunActive        = errors.New("dag-run is active")
	ErrNoStatusData        = errors.New("no status data")
	ErrCorruptedStatusData = errors.New("corrupted status data")

	// ErrDAGRunRetentionLocked indicates a finished root run carries an
	// explicit retention hold and must not be removed.
	ErrDAGRunRetentionLocked = errors.New("dag-run is retention locked")
	// ErrDAGRunRetentionUnreadable indicates the retention state could not be
	// read; callers must treat such a run as non-removable.
	ErrDAGRunRetentionUnreadable = errors.New("dag-run retention state is unreadable")
	// ErrDAGRunNotRoot indicates retention was requested for a child run,
	// while only root runs may be retention objects.
	ErrDAGRunNotRoot = errors.New("dag-run is not a root run")
)
