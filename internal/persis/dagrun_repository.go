// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package persis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/logger"
	"github.com/dagucloud/dagu/v2/internal/dagrun"
	"github.com/dagucloud/dagu/v2/internal/ir"
)

// DAGRunRepository provides application-level access to persisted DAG runs.
type DAGRunRepository struct {
	store             DAGRunStore
	workDirs          dagrun.WorkDirStore
	latestStatusToday bool
	location          *time.Location
	now               func() time.Time
	removalEnqueuer   DAGRunRemovalEnqueuer
}

// NewDAGRunRepository creates a repository backed by store.
func NewDAGRunRepository(store DAGRunStore, workDirs dagrun.WorkDirStore, options DAGRunRepositoryOptions) *DAGRunRepository {
	location := options.Location
	if location == nil {
		location = time.Local
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	if workDirs == nil {
		workDirs = noopWorkDirStore{}
	}
	return &DAGRunRepository{
		store:             store,
		workDirs:          workDirs,
		latestStatusToday: options.LatestStatusToday,
		location:          location,
		now:               now,
		removalEnqueuer:   options.RemovalEnqueuer,
	}
}

// CreateAttempt creates an attempt within a DAG run.
func (r *DAGRunRepository) CreateAttempt(
	ctx context.Context,
	dag *ir.DAG,
	timestamp time.Time,
	dagRunID string,
	options DAGRunCreateAttemptOptions,
) (dagrun.Attempt, error) {
	if dagRunID == "" {
		return nil, dagrun.ErrDAGRunIDEmpty
	}
	if !options.RootDAGRun.Zero() && options.RootDAGRun.ID == "" {
		return nil, dagrun.ErrDAGRunIDEmpty
	}
	attempt, err := r.store.CreateAttempt(ctx, DAGRunCreateAttemptRequest{
		DAG:        dag,
		RootDAGRun: options.RootDAGRun,
		Timestamp:  timestamp,
		DAGRunID:   dagRunID,
		AttemptID:  options.AttemptID,
		Retry:      options.Retry,
	})
	if err != nil {
		return nil, err
	}
	return newEventingAttempt(attempt, dag), nil
}

// RecentStatuses returns the newest readable status for recent DAG runs.
func (r *DAGRunRepository) RecentStatuses(ctx context.Context, name string, limit int) ([]ir.DAGRunStatus, error) {
	if limit <= 0 {
		logger.Warn(ctx, "Non-positive recent DAG-run limit, using default of 10", slog.Int("limit", limit))
		limit = 10
	}
	return r.store.RecentStatuses(ctx, name, limit)
}

// LatestAttempt returns the newest visible attempt for a DAG.
func (r *DAGRunRepository) LatestAttempt(
	ctx context.Context,
	name string,
	options DAGRunLatestAttemptOptions,
) (dagrun.Attempt, error) {
	query := DAGRunLatestAttemptQuery{Name: name}
	if r.latestStatusToday && !options.AllHistory {
		query.NotBefore = NewUTC(r.startOfDay())
	}
	attempt, err := r.store.LatestAttempt(ctx, query)
	if err != nil {
		return nil, err
	}
	return newEventingAttempt(attempt, nil), nil
}

// ListStatuses returns statuses in canonical list order.
func (r *DAGRunRepository) ListStatuses(ctx context.Context, options DAGRunListOptions) ([]*ir.DAGRunStatus, error) {
	page, err := r.store.QueryStatuses(ctx, r.statusQuery(options))
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

// ListStatusesPage returns one forward-only page in canonical list order.
func (r *DAGRunRepository) ListStatusesPage(ctx context.Context, options DAGRunListOptions) (DAGRunStatusPage, error) {
	return r.store.QueryStatuses(ctx, r.statusQuery(options))
}

func (r *DAGRunRepository) statusQuery(options DAGRunListOptions) DAGRunStatusQuery {
	query := DAGRunStatusQuery{
		DAGRunID:        options.DAGRunID,
		Name:            options.Name,
		ExactName:       options.ExactName,
		From:            options.From,
		To:              options.To,
		Statuses:        options.Statuses,
		Limit:           options.Limit,
		Cursor:          options.Cursor,
		Labels:          options.Labels,
		WorkspaceFilter: options.WorkspaceFilter,
	}
	if !options.AllHistory && query.From.IsZero() && query.To.IsZero() {
		query.From = NewUTC(r.startOfDay())
	}
	if options.Unbounded {
		query.Limit = 0
	} else if query.Limit <= 0 || query.Limit > 1000 {
		query.Limit = 1000
	}
	return query
}

func (r *DAGRunRepository) startOfDay() time.Time {
	now := r.now().In(r.location)
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, r.location)
}

// CompareAndSwapLatestAttemptStatus atomically updates a matching latest attempt.
func (r *DAGRunRepository) CompareAndSwapLatestAttemptStatus(
	ctx context.Context,
	dagRun ir.DAGRunRef,
	expectedAttemptID string,
	expectedStatus ir.Status,
	mutate func(*ir.DAGRunStatus) error,
	options DAGRunCompareAndSwapOptions,
) (*ir.DAGRunStatus, bool, error) {
	if dagRun.ID == "" {
		return nil, false, dagrun.ErrDAGRunIDEmpty
	}
	root := options.RootDAGRun
	if root.Zero() {
		root = dagRun
	}
	isSubDAG := root.ID != "" && (root.ID != dagRun.ID || root.Name != dagRun.Name)
	if isSubDAG && root.Name == "" {
		return nil, false, fmt.Errorf("missing root dag-run name for sub dag-run %s", dagRun.ID)
	}
	if root.Name == "" {
		root.Name = dagRun.Name
	}
	if root.ID == "" {
		return nil, false, dagrun.ErrDAGRunIDEmpty
	}

	status, swapped, err := r.store.CompareAndSwapLatestAttemptStatus(ctx, DAGRunCompareAndSwapStatusRequest{
		DAGRun:             dagRun,
		RootDAGRun:         root,
		ExpectedAttemptID:  expectedAttemptID,
		ExpectedAttemptKey: options.ExpectedAttemptKey,
		ExpectedStatus:     expectedStatus,
		Mutate: func(status *ir.DAGRunStatus) error {
			if err := mutate(status); err != nil {
				return err
			}
			ir.NormalizeDAGRunConditions(status)
			return nil
		},
	})
	if err != nil || !swapped {
		return status, swapped, err
	}
	r.emitStatusEventAfterSwap(ctx, root, dagRun, expectedStatus, status)
	return status, true, nil
}

// FindAttempt finds the latest visible attempt for a DAG run.
func (r *DAGRunRepository) FindAttempt(ctx context.Context, ref ir.DAGRunRef) (dagrun.Attempt, error) {
	if ref.ID == "" {
		return nil, dagrun.ErrDAGRunIDEmpty
	}
	attempt, err := r.store.FindAttempt(ctx, ref)
	if err != nil {
		return nil, err
	}
	return newEventingAttempt(attempt, nil), nil
}

// FindSubAttempt finds the latest visible attempt for a child DAG run.
func (r *DAGRunRepository) FindSubAttempt(ctx context.Context, root ir.DAGRunRef, childRunID string) (dagrun.Attempt, error) {
	if root.ID == "" {
		return nil, dagrun.ErrDAGRunIDEmpty
	}
	attempt, err := r.store.FindSubAttempt(ctx, root, childRunID)
	if err != nil {
		return nil, err
	}
	return newEventingAttempt(attempt, nil), nil
}

// RemoveOldDAGRuns removes final DAG runs outside the configured retention policy.
// Explicitly retention-locked runs are reported in DAGRunRemovalResult.Retained
// and never removed, enqueued for asynchronous cleanup, or stripped of their
// working directories.
func (r *DAGRunRepository) RemoveOldDAGRuns(
	ctx context.Context,
	name string,
	retentionDays int,
	options DAGRunRetentionOptions,
) (DAGRunRemovalResult, error) {
	request := DAGRunRetentionRequest{Name: name, DryRun: options.DryRun}
	if options.RetentionRuns != nil {
		if *options.RetentionRuns <= 0 {
			logger.Warn(ctx, "Non-positive retentionRuns, no DAG runs will be removed",
				slog.Int("retention-runs", *options.RetentionRuns))
			return DAGRunRemovalResult{}, nil
		}
		request.KeepRuns = *options.RetentionRuns
		return r.removeOldDAGRuns(ctx, request)
	}
	if options.OlderThan != nil {
		request.OlderThan = NewUTC(*options.OlderThan)
		return r.removeOldDAGRuns(ctx, request)
	}
	if retentionDays < 0 {
		logger.Warn(ctx, "Negative retentionDays, no DAG runs will be removed",
			slog.Int("retention-days", retentionDays))
		return DAGRunRemovalResult{}, nil
	}
	request.OlderThan = NewUTC(r.now().AddDate(0, 0, -retentionDays))
	return r.removeOldDAGRuns(ctx, request)
}

// removeOldDAGRuns first classifies candidates with a dry-run selection and
// then removes each run through the same locked gate as a single delete, so a
// retention hold landing between scan and delete is honored and asynchronous
// cleanup cannot be scheduled ahead of it.
func (r *DAGRunRepository) removeOldDAGRuns(ctx context.Context, request DAGRunRetentionRequest) (DAGRunRemovalResult, error) {
	preview := request
	preview.DryRun = true
	selection, err := r.store.RemoveOldDAGRuns(ctx, preview)
	if err != nil {
		return DAGRunRemovalResult{}, err
	}

	result := DAGRunRemovalResult{
		Retained: dagRunRefIDs(selection.Retained),
	}
	if request.DryRun {
		result.Removed = dagRunRefIDs(selection.Removable)
		return result, nil
	}

	var (
		removedRefs []ir.DAGRunRef
		removeErrs  []error
	)
	for _, ref := range selection.Removable {
		err := r.removeDAGRunStore(ctx, ref, DAGRunRemoveOptions{RejectActive: true})
		switch {
		case err == nil:
			removedRefs = append(removedRefs, ref)
		case errors.Is(err, dagrun.ErrDAGRunRetentionLocked):
			// The hold landed after the unlocked preview; classify it now.
			result.Retained = append(result.Retained, ref.ID)
		case errors.Is(err, dagrun.ErrDAGRunIDNotFound), errors.Is(err, dagrun.ErrDAGRunActive):
			// The run disappeared or restarted between scan and removal.
			continue
		default:
			removeErrs = append(removeErrs, err)
		}
	}

	if err := r.removeWorkDirs(ctx, removedRefs); err != nil {
		removeErrs = append(removeErrs, err)
	}
	result.Removed = dagRunRefIDs(removedRefs)
	return result, errors.Join(removeErrs...)
}

func (r *DAGRunRepository) removeWorkDirs(ctx context.Context, refs []ir.DAGRunRef) error {
	var errs []error
	for _, ref := range refs {
		workDirRef, normalizeErr := normalizeWorkDirRef(dagrun.WorkDirRef{DAGRun: ref})
		if normalizeErr != nil {
			errs = append(errs, normalizeErr)
			continue
		}
		if removeErr := r.workDirs.Remove(ctx, workDirRef); removeErr != nil {
			errs = append(errs, fmt.Errorf("remove work directory for dag-run %s: %w", ref.ID, removeErr))
		}
	}
	return errors.Join(errs...)
}

func dagRunRefIDs(refs []ir.DAGRunRef) []string {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.ID)
	}
	return ids
}

// RemoveDAGRun removes a DAG run and all of its attempts.
func (r *DAGRunRepository) RemoveDAGRun(ctx context.Context, ref ir.DAGRunRef, options DAGRunRemoveOptions) error {
	if ref.ID == "" {
		return dagrun.ErrDAGRunIDEmpty
	}
	if err := r.removeDAGRunStore(ctx, ref, options); err != nil {
		if !errors.Is(err, dagrun.ErrDAGRunIDNotFound) {
			return err
		}
		// Preserve the historical tolerance for already-missing runs while
		// still removing a materialized work directory.
		if err := r.removeWorkDir(ctx, ref); err != nil {
			return err
		}
		return nil
	}
	return r.removeWorkDir(ctx, ref)
}

// removeWorkDir removes the materialized per-run working directory.
func (r *DAGRunRepository) removeWorkDir(ctx context.Context, ref ir.DAGRunRef) error {
	workDirRef, err := normalizeWorkDirRef(dagrun.WorkDirRef{DAGRun: ref})
	if err != nil {
		return err
	}
	if removeErr := r.workDirs.Remove(ctx, workDirRef); removeErr != nil {
		return fmt.Errorf("remove work directory for dag-run %s: %w", ref.ID, removeErr)
	}
	return nil
}

// removeDAGRunStore performs the locked store removal. Agent-session cleanup
// jobs are scheduled from the before-commit hook, which the file store invokes
// inside the root ownership lock after every gate passed: a concurrent
// retention set can neither lose to this delete nor be followed by cleanup
// scheduled before the lock.
func (r *DAGRunRepository) removeDAGRunStore(ctx context.Context, ref ir.DAGRunRef, options DAGRunRemoveOptions) error {
	req := DAGRunRemoveRequest{
		DAGRun:       ref,
		RejectActive: options.RejectActive,
	}
	if r.removalEnqueuer != nil {
		req.BeforeCommit = func(hookCtx context.Context) error {
			return r.enqueueDAGRunRemovals(hookCtx, []ir.DAGRunRef{ref})
		}
	}
	return r.store.RemoveDAGRun(ctx, req)
}

// GetRetentionLock returns the explicit retention hold on a finished root DAG
// run, or (nil, nil) when the run is not held.
func (r *DAGRunRepository) GetRetentionLock(ctx context.Context, ref ir.DAGRunRef) (*RetentionLock, error) {
	if ref.ID == "" {
		return nil, dagrun.ErrDAGRunIDEmpty
	}
	return r.store.GetRetentionLock(ctx, ref)
}

// SetRetentionLock places an idempotent retention hold on a finished root DAG
// run. Repeated calls return the existing hold unchanged. Only terminal root
// runs can be held; the store rejects active and child runs.
func (r *DAGRunRepository) SetRetentionLock(ctx context.Context, ref ir.DAGRunRef, input DAGRunRetentionLockInput) (*RetentionLock, error) {
	if ref.ID == "" {
		return nil, dagrun.ErrDAGRunIDEmpty
	}
	return r.store.SetRetentionLock(ctx, DAGRunSetRetentionLockRequest{
		DAGRun:   ref,
		LockedBy: strings.TrimSpace(input.LockedBy),
		Reason:   strings.TrimSpace(input.Reason),
	})
}

// ReleaseRetentionLock removes a retention hold. Releasing an unheld run
// succeeds, so the run returns to its DAG's normal history policy.
func (r *DAGRunRepository) ReleaseRetentionLock(ctx context.Context, ref ir.DAGRunRef) error {
	if ref.ID == "" {
		return dagrun.ErrDAGRunIDEmpty
	}
	return r.store.ReleaseRetentionLock(ctx, ref)
}

// PruneArtifacts removes artifact directories and index records that no
// surviving DAG run points to.
func (r *DAGRunRepository) PruneArtifacts(ctx context.Context, request ArtifactPruneRequest) (*ArtifactPruneResult, error) {
	return r.store.PruneArtifacts(ctx, request)
}

func (r *DAGRunRepository) enqueueDAGRunRemovals(ctx context.Context, refs []ir.DAGRunRef) error {
	for _, ref := range refs {
		resources, err := r.agentSessionResources(ctx, ref)
		if err != nil {
			if errors.Is(err, dagrun.ErrDAGRunIDNotFound) || errors.Is(err, dagrun.ErrNoStatusData) {
				continue
			}
			return fmt.Errorf("collect agent sessions for dag-run %s: %w", ref.ID, err)
		}
		if err := r.removalEnqueuer.EnqueueDAGRunRemoval(ctx, ref, resources); err != nil {
			return fmt.Errorf("enqueue agent session cleanup for dag-run %s: %w", ref.ID, err)
		}
	}
	return nil
}

func (r *DAGRunRepository) agentSessionResources(ctx context.Context, root ir.DAGRunRef) ([]ir.AgentSessionResource, error) {
	attempt, err := r.store.FindAttempt(ctx, root)
	if err != nil {
		return nil, err
	}
	status, err := attempt.ReadStatus(ctx)
	if err != nil {
		return nil, err
	}
	resources := ir.MergeAgentSessionResources(status.AgentSessions, status.Nodes)
	seen := map[string]bool{root.ID: true}
	queue := childDAGRunIDs(status.Nodes)
	for len(queue) > 0 {
		childID := queue[0]
		queue = queue[1:]
		if childID == "" || seen[childID] {
			continue
		}
		seen[childID] = true
		childAttempt, err := r.store.FindSubAttempt(ctx, root, childID)
		if err != nil {
			if errors.Is(err, dagrun.ErrDAGRunIDNotFound) || errors.Is(err, dagrun.ErrNoStatusData) {
				continue
			}
			return nil, err
		}
		childStatus, err := childAttempt.ReadStatus(ctx)
		if err != nil {
			if errors.Is(err, dagrun.ErrNoStatusData) {
				continue
			}
			return nil, err
		}
		resources = ir.MergeAgentSessionResources(resources, childStatus.Nodes)
		resources = append(resources, childStatus.AgentSessions...)
		resources = ir.MergeAgentSessionResources(resources, nil)
		queue = append(queue, childDAGRunIDs(childStatus.Nodes)...)
	}
	return resources, nil
}

func childDAGRunIDs(nodes []*ir.Node) []string {
	var ids []string
	for _, node := range nodes {
		if node == nil {
			continue
		}
		for _, child := range node.SubRuns {
			ids = append(ids, child.DAGRunID)
		}
		for _, child := range node.SubRunsRepeated {
			ids = append(ids, child.DAGRunID)
		}
	}
	return ids
}

// OpenLog reads a regular log file up to its size when opened.
// The caller must close the reader. Missing files return an fs.ErrNotExist error.
func (r *DAGRunRepository) OpenLog(ctx context.Context, path string) (io.ReadCloser, error) {
	return r.store.OpenLog(ctx, path)
}
