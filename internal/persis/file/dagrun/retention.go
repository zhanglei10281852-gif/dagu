// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package dagrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/fileutil"
	"github.com/dagucloud/dagu/v2/internal/cmn/logger"
	"github.com/dagucloud/dagu/v2/internal/cmn/logger/tag"
	domaindagrun "github.com/dagucloud/dagu/v2/internal/dagrun"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/persis"
)

// retentionFileName is the per-root-run evidence retention marker. It lives
// inside the root run directory, so it shares the run's ownership boundary and
// is removed together with the run once the hold is released and policy
// deletes it. It is a file, so attempt/sub-directory discovery ignores it.
const retentionFileName = "retention.json"

// readRetentionLock returns the hold recorded on the run. A missing marker
// yields (nil, nil). Any other failure, including a malformed marker, yields
// dagrun.ErrDAGRunRetentionUnreadable so callers fail closed and keep the run.
func (dr DAGRun) readRetentionLock() (*persis.RetentionLock, error) {
	path := filepath.Join(dr.baseDir, retentionFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read retention state for dag-run %s: %w: %w",
			dr.dagRunID, domaindagrun.ErrDAGRunRetentionUnreadable, err)
	}

	var lock persis.RetentionLock
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, fmt.Errorf("parse retention state for dag-run %s: %w: %w",
			dr.dagRunID, domaindagrun.ErrDAGRunRetentionUnreadable, err)
	}
	if lock.LockedAt.IsZero() {
		return nil, fmt.Errorf("retention state for dag-run %s is missing locked_at: %w",
			dr.dagRunID, domaindagrun.ErrDAGRunRetentionUnreadable)
	}
	return &lock, nil
}

// writeRetentionLock durably replaces the run's retention marker.
func (dr DAGRun) writeRetentionLock(lock *persis.RetentionLock) error {
	data, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		return fmt.Errorf("encode retention state for dag-run %s: %w", dr.dagRunID, err)
	}
	path := filepath.Join(dr.baseDir, retentionFileName)
	if err := fileutil.WriteFileAtomic(path, data, 0640); err != nil {
		return fmt.Errorf("write retention state for dag-run %s: %w", dr.dagRunID, err)
	}
	return nil
}

// removeRetentionLock durably removes the run's retention marker. A missing
// marker is not an error, so repeated releases are idempotent.
func (dr DAGRun) removeRetentionLock() error {
	err := fileutil.RemoveFileDurable(filepath.Join(dr.baseDir, retentionFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove retention state for dag-run %s: %w", dr.dagRunID, err)
	}
	return nil
}

// GetRetentionLock implements persis.DAGRunStore.
func (store *Store) GetRetentionLock(ctx context.Context, root ir.DAGRunRef) (*persis.RetentionLock, error) {
	if root.ID == "" {
		return nil, domaindagrun.ErrDAGRunIDEmpty
	}
	dataRoot := store.dataRoot(root.Name)
	run, err := dataRoot.FindByDAGRunID(ctx, root.ID)
	if err != nil {
		return nil, err
	}
	return run.readRetentionLock()
}

// SetRetentionLock implements persis.DAGRunStore. Only finished root runs can
// be held. The check and the marker write share the DataRoot lock with run
// removal, so a concurrent deletion can never pass its gates after the hold
// became durable. Repeated sets return the existing hold unchanged.
func (store *Store) SetRetentionLock(ctx context.Context, req persis.DAGRunSetRetentionLockRequest) (*persis.RetentionLock, error) {
	if req.DAGRun.ID == "" {
		return nil, domaindagrun.ErrDAGRunIDEmpty
	}

	root := store.dataRoot(req.DAGRun.Name)
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := root.Lock(lockCtx); err != nil {
		return nil, fmt.Errorf("failed to acquire lock for dag-run %s: %w", req.DAGRun.ID, err)
	}
	defer func() {
		if err := root.Unlock(); err != nil {
			logger.Error(ctx, "Failed to unlock dag-run", tag.RunID(req.DAGRun.ID), tag.Error(err))
		}
	}()

	run, err := root.FindByDAGRunID(ctx, req.DAGRun.ID)
	if err != nil {
		return nil, err
	}

	attempt, err := run.LatestAttempt(ctx, store.cache)
	if err != nil {
		return nil, err
	}
	status, err := attempt.ReadStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read dag-run %s status: %w", req.DAGRun.ID, err)
	}
	if status == nil {
		return nil, fmt.Errorf("failed to read dag-run %s status: %w", req.DAGRun.ID, domaindagrun.ErrNoStatusData)
	}
	if status.Status.IsActive() || status.Status == ir.NotStarted {
		return nil, fmt.Errorf("%w: %s", domaindagrun.ErrDAGRunActive, status.Status.String())
	}
	// Child runs are reached only through their root run; top-level run
	// directories must never carry a child status. Refuse rather than create
	// a second retention object for them.
	if status.Parent.ID != "" {
		return nil, fmt.Errorf("%w: dag-run %s is a child of %s/%s",
			domaindagrun.ErrDAGRunNotRoot, req.DAGRun.ID, status.Parent.Name, status.Parent.ID)
	}

	existing, err := run.readRetentionLock()
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// Idempotent replay: leave the original operator/time/reason intact.
		return existing, nil
	}

	lock := &persis.RetentionLock{
		LockedAt: time.Now().UTC(),
		LockedBy: req.LockedBy,
		Reason:   req.Reason,
	}
	if err := run.writeRetentionLock(lock); err != nil {
		return nil, err
	}
	return lock, nil
}

// ReleaseRetentionLock implements persis.DAGRunStore. It takes the same
// DataRoot lock as set and remove, and succeeds when no marker is present.
func (store *Store) ReleaseRetentionLock(ctx context.Context, rootRef ir.DAGRunRef) error {
	if rootRef.ID == "" {
		return domaindagrun.ErrDAGRunIDEmpty
	}

	root := store.dataRoot(rootRef.Name)
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := root.Lock(lockCtx); err != nil {
		return fmt.Errorf("failed to acquire lock for dag-run %s: %w", rootRef.ID, err)
	}
	defer func() {
		if err := root.Unlock(); err != nil {
			logger.Error(ctx, "Failed to unlock dag-run", tag.RunID(rootRef.ID), tag.Error(err))
		}
	}()

	run, err := root.FindByDAGRunID(ctx, rootRef.ID)
	if err != nil {
		return err
	}
	return run.removeRetentionLock()
}

// assertRetentionGate fails closed when the run carries a retention hold or
// its retention state cannot be read. The caller must hold the DataRoot lock.
func assertRetentionGate(run *DAGRun) error {
	lock, err := run.readRetentionLock()
	if err != nil {
		return err
	}
	if lock != nil {
		return fmt.Errorf("%w: dag-run %s retained by %q since %s",
			domaindagrun.ErrDAGRunRetentionLocked, run.dagRunID, lock.LockedBy,
			lock.LockedAt.Format(time.RFC3339))
	}
	return nil
}
