// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package persis

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/dagucloud/dagu/v2/internal/dagrun"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/workspace"
)

var ErrInvalidDAGRunQueryCursor = errors.New("dagrun: invalid query cursor")

// DAGRunStore persists DAG runs and their attempts as one consistency boundary.
type DAGRunStore interface {
	// OpenLog reads a regular log file up to its size when opened. The caller must close it.
	OpenLog(ctx context.Context, path string) (io.ReadCloser, error)
	CreateAttempt(ctx context.Context, req DAGRunCreateAttemptRequest) (dagrun.Attempt, error)
	RecentStatuses(ctx context.Context, name string, limit int) ([]ir.DAGRunStatus, error)
	LatestAttempt(ctx context.Context, query DAGRunLatestAttemptQuery) (dagrun.Attempt, error)
	QueryStatuses(ctx context.Context, query DAGRunStatusQuery) (DAGRunStatusPage, error)
	CompareAndSwapLatestAttemptStatus(ctx context.Context, req DAGRunCompareAndSwapStatusRequest) (*ir.DAGRunStatus, bool, error)
	FindAttempt(ctx context.Context, ref ir.DAGRunRef) (dagrun.Attempt, error)
	FindSubAttempt(ctx context.Context, root ir.DAGRunRef, childRunID string) (dagrun.Attempt, error)
	RemoveOldDAGRuns(ctx context.Context, req DAGRunRetentionRequest) (DAGRunRetentionSelection, error)
	RemoveDAGRun(ctx context.Context, req DAGRunRemoveRequest) error
	// GetRetentionLock reads the explicit retention hold on a finished root
	// DAG run. A nil lock with a nil error means the run is not held.
	GetRetentionLock(ctx context.Context, root ir.DAGRunRef) (*RetentionLock, error)
	// SetRetentionLock places a retention hold on a finished root DAG run.
	// Repeated sets are idempotent: an existing valid hold is returned
	// unchanged. The write shares the run's root ownership boundary with
	// retention selection and removal.
	SetRetentionLock(ctx context.Context, req DAGRunSetRetentionLockRequest) (*RetentionLock, error)
	// ReleaseRetentionLock removes a retention hold. Releasing a run without
	// a hold succeeds, so repeated releases are idempotent.
	ReleaseRetentionLock(ctx context.Context, root ir.DAGRunRef) error
	// PruneArtifacts removes artifact directories and index records that no
	// surviving DAG run points to.
	PruneArtifacts(ctx context.Context, req ArtifactPruneRequest) (*ArtifactPruneResult, error)
}

// DAGRunStatusQuery contains normalized backend filters for listing runs.
// Limit is zero for an unbounded query and positive otherwise.
type DAGRunStatusQuery struct {
	DAGRunID        string
	Name            string
	ExactName       string
	From            TimeInUTC
	To              TimeInUTC
	Statuses        []ir.Status
	Limit           int
	Cursor          string
	Labels          []string
	WorkspaceFilter *workspace.WorkspaceFilter
}

// DAGRunStatusPage is one forward-only page of DAG-run statuses.
type DAGRunStatusPage struct {
	Items      []*ir.DAGRunStatus
	NextCursor string
}

// DAGRunCreateAttemptRequest identifies the run and attempt to create.
// A zero RootDAGRun creates a root run; a nonzero value creates its child.
type DAGRunCreateAttemptRequest struct {
	DAG        *ir.DAG
	RootDAGRun ir.DAGRunRef
	Timestamp  time.Time
	DAGRunID   string
	AttemptID  string
	Retry      bool
}

// DAGRunLatestAttemptQuery selects the newest visible attempt for a DAG.
type DAGRunLatestAttemptQuery struct {
	Name      string
	NotBefore TimeInUTC
}

// DAGRunCompareAndSwapStatusRequest describes an atomic latest-attempt status update.
type DAGRunCompareAndSwapStatusRequest struct {
	DAGRun             ir.DAGRunRef
	RootDAGRun         ir.DAGRunRef
	ExpectedAttemptID  string
	ExpectedAttemptKey string
	ExpectedStatus     ir.Status
	Mutate             func(*ir.DAGRunStatus) error
}

// DAGRunRetentionRequest describes normalized DAG-run cleanup policy.
type DAGRunRetentionRequest struct {
	Name      string
	OlderThan TimeInUTC
	KeepRuns  int
	DryRun    bool
}

// DAGRunRetentionSelection classifies runs selected by a retention sweep.
type DAGRunRetentionSelection struct {
	// Removable are runs the sweep would remove (dry run) or attempted to
	// remove (actual sweep).
	Removable []ir.DAGRunRef
	// Retained are runs skipped because they carry an explicit retention hold.
	Retained []ir.DAGRunRef
}

// DAGRunRemoveRequest identifies a DAG run to remove.
type DAGRunRemoveRequest struct {
	DAGRun       ir.DAGRunRef
	RejectActive bool
	// BeforeCommit, when non-nil, runs while the root ownership lock is held,
	// after the run passed the active/retention gates and immediately before
	// its files are removed. A failed callback aborts the removal and leaves
	// the run untouched. It exists so side effects such as enqueueing agent
	// session cleanup cannot be scheduled for a run that a concurrent
	// retention hold just protected.
	BeforeCommit func(ctx context.Context) error
}

// RetentionLock is an evidence-preservation hold placed on one finished root
// DAG run. It is independent of the DAG's history retention policy and moves
// with the run directory, so it survives process restarts.
type RetentionLock struct {
	// LockedAt is when the hold was first placed (UTC).
	LockedAt time.Time `json:"locked_at"`
	// LockedBy identifies the operator that placed the hold.
	LockedBy string `json:"locked_by"`
	// Reason is the optional justification recorded with the hold.
	Reason string `json:"reason,omitempty"`
}

// DAGRunSetRetentionLockRequest places a retention hold on a root run.
type DAGRunSetRetentionLockRequest struct {
	DAGRun   ir.DAGRunRef
	LockedBy string
	Reason   string
}

// ArtifactPruneRequest describes a sweep for orphaned artifact directories.
type ArtifactPruneRequest struct {
	// Root is the artifact tree to sweep; empty uses the store's configured
	// artifact root.
	Root string
	// ProtectedDirs are directories the sweep must never reach. A root that
	// equals or contains one of them, or the store's run history, is refused.
	ProtectedDirs []string
	// OlderThan bounds removal to entries created before it. Stores clamp it
	// to a minimum age, because a run's artifact directory exists before its
	// record does.
	OlderThan TimeInUTC
	// DryRun reports what would be removed without removing it.
	DryRun bool
}

// ArtifactPruneResult reports the paths a sweep removed, or would remove in a
// dry run.
type ArtifactPruneResult struct {
	// Dirs are run artifact directories.
	Dirs []string
	// Records are artifact index sidecars whose run is gone.
	Records []string
}

// DAGRunRepositoryOptions configures application-level DAG-run behavior.
type DAGRunRepositoryOptions struct {
	LatestStatusToday bool
	Location          *time.Location
	Now               func() time.Time
	RemovalEnqueuer   DAGRunRemovalEnqueuer
}

// DAGRunRemovalEnqueuer durably records resources before their DAG-run history is removed.
type DAGRunRemovalEnqueuer interface {
	EnqueueDAGRunRemoval(ctx context.Context, root ir.DAGRunRef, resources []ir.AgentSessionResource) error
}

// DAGRunCreateAttemptOptions configures creation of a run attempt.
type DAGRunCreateAttemptOptions struct {
	// RootDAGRun identifies the root when creating a child attempt.
	RootDAGRun ir.DAGRunRef
	// Retry indicates that the attempt retries an existing DAG run.
	Retry bool
	// AttemptID uses a caller-assigned attempt identifier when non-empty.
	AttemptID string
}

// DAGRunLatestAttemptOptions configures a latest-attempt lookup.
type DAGRunLatestAttemptOptions struct {
	// AllHistory disables the repository's optional today-only lookup window.
	AllHistory bool
}

// DAGRunListOptions configures status listing.
type DAGRunListOptions struct {
	DAGRunID        string
	Name            string
	ExactName       string
	From            TimeInUTC
	To              TimeInUTC
	Statuses        []ir.Status
	Limit           int
	Cursor          string
	Labels          []string
	WorkspaceFilter *workspace.WorkspaceFilter
	// AllHistory disables the implicit recent-time window when no range is set.
	AllHistory bool
	// Unbounded disables the repository's result limit.
	Unbounded bool
}

// DAGRunCompareAndSwapOptions configures an atomic latest-attempt status update.
type DAGRunCompareAndSwapOptions struct {
	// RootDAGRun identifies the root containing a child attempt.
	RootDAGRun ir.DAGRunRef
	// ExpectedAttemptKey rejects an update when the persisted key differs.
	ExpectedAttemptKey string
}

// DAGRunRetentionOptions configures retention cleanup.
type DAGRunRetentionOptions struct {
	DryRun bool
	// RetentionRuns takes precedence over OlderThan and retention days when set.
	RetentionRuns *int
	// OlderThan takes precedence over retention days when RetentionRuns is unset.
	OlderThan *time.Time
}

// DAGRunRemoveOptions configures DAG-run removal.
type DAGRunRemoveOptions struct {
	// RejectActive refuses to remove an active DAG run.
	RejectActive bool
}

// DAGRunRemovalResult reports the outcome of a retention sweep.
type DAGRunRemovalResult struct {
	// Removed are the root run IDs that were removed (or would be removed in
	// a dry run).
	Removed []string
	// Retained are the root run IDs skipped because of an explicit retention
	// hold. In an actual sweep this also includes runs that were selected but
	// gained a hold before their locked removal ran.
	Retained []string
}

// DAGRunRetentionLockInput carries operator metadata for a retention hold.
type DAGRunRetentionLockInput struct {
	// LockedBy identifies the operator placing the hold.
	LockedBy string
	// Reason is an optional justification.
	Reason string
}

// TimeInUTC wraps a time normalized to UTC.
type TimeInUTC struct{ time.Time }

// NewUTC returns t normalized to UTC.
func NewUTC(t time.Time) TimeInUTC {
	return TimeInUTC{Time: t.UTC()}
}

type dagRunListReadBatchContextKey struct{}

var nextDAGRunListReadBatchID atomic.Uint64

// DAGRunListReadBatch identifies list reads that may share the same storage work.
type DAGRunListReadBatch struct {
	id uint64
}

// NewDAGRunListReadBatch creates a new list-read batch.
func NewDAGRunListReadBatch() *DAGRunListReadBatch {
	return &DAGRunListReadBatch{id: nextDAGRunListReadBatchID.Add(1)}
}

// WithDAGRunListReadBatch associates a list-read batch with a context.
func WithDAGRunListReadBatch(ctx context.Context, batch *DAGRunListReadBatch) context.Context {
	return context.WithValue(ctx, dagRunListReadBatchContextKey{}, batch)
}

// DAGRunListReadBatchID returns the list-read batch ID associated with ctx.
func DAGRunListReadBatchID(ctx context.Context) (uint64, bool) {
	batch, ok := ctx.Value(dagRunListReadBatchContextKey{}).(*DAGRunListReadBatch)
	if !ok {
		return 0, false
	}
	return batch.id, true
}
