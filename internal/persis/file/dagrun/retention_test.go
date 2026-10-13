// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package dagrun

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	domaindagrun "github.com/dagucloud/dagu/v2/internal/dagrun"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/persis"
)

// A hold placed on a finished root run persists and is returned unchanged by
// an idempotent replay.
func TestRetentionLockSetGetIsIdempotent(t *testing.T) {
	th := setupTestRepository(t)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	th.CreateAttempt(t, ts, "finished-run", ir.Succeeded)
	ref := ir.NewDAGRunRef("test_DAG", "finished-run")

	lock, err := th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{
		LockedBy: "alice",
		Reason:   "incident INC-42 evidence",
	})
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "alice", lock.LockedBy)
	assert.Equal(t, "incident INC-42 evidence", lock.Reason)
	assert.False(t, lock.LockedAt.IsZero())

	// The marker is a regular file inside the root run directory.
	dataRoot := NewDataRoot(th.TmpDir, "test_DAG")
	run, err := dataRoot.FindByDAGRunID(th.Context, "finished-run")
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(run.baseDir, retentionFileName))

	// A fresh store (simulating a process restart) reads the same hold.
	restarted := persis.NewDAGRunRepository(
		NewStore(th.TmpDir, WithArtifactDir(filepath.Join(th.TmpDir, "artifacts"))),
		NewWorkDirStore(filepath.Join(th.TmpDir, ".dag-run-work"), th.TmpDir),
		persis.DAGRunRepositoryOptions{},
	)
	loaded, err := restarted.GetRetentionLock(th.Context, ref)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, *lock, *loaded)

	// Repeated set is a no-op replay: operator/time/reason stay intact.
	replay, err := th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{
		LockedBy: "bob",
		Reason:   "different reason",
	})
	require.NoError(t, err)
	assert.Equal(t, *lock, *replay)
}

func TestRetentionLockRejectsActiveAndMissingRuns(t *testing.T) {
	th := setupTestRepository(t)
	th.CreateAttempt(t, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "active-run", ir.Running)

	_, err := th.Repository.SetRetentionLock(th.Context, ir.NewDAGRunRef("test_DAG", "active-run"),
		persis.DAGRunRetentionLockInput{LockedBy: "alice"})
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunActive)

	_, err = th.Repository.SetRetentionLock(th.Context, ir.NewDAGRunRef("test_DAG", "missing-run"),
		persis.DAGRunRetentionLockInput{LockedBy: "alice"})
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunIDNotFound)

	// Get on an unheld run reports no hold.
	hold, err := th.Repository.GetRetentionLock(th.Context, ir.NewDAGRunRef("test_DAG", "active-run"))
	require.NoError(t, err)
	assert.Nil(t, hold)
}

func TestRetentionLockReleaseIsIdempotent(t *testing.T) {
	th := setupTestRepository(t)
	th.CreateAttempt(t, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "finished-run", ir.Aborted)
	ref := ir.NewDAGRunRef("test_DAG", "finished-run")

	_, err := th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{LockedBy: "alice"})
	require.NoError(t, err)

	require.NoError(t, th.Repository.ReleaseRetentionLock(th.Context, ref))
	hold, err := th.Repository.GetRetentionLock(th.Context, ref)
	require.NoError(t, err)
	assert.Nil(t, hold)

	// Releasing an unheld run and doing it again both succeed.
	require.NoError(t, th.Repository.ReleaseRetentionLock(th.Context, ref))
	require.NoError(t, th.Repository.ReleaseRetentionLock(th.Context, ref))

	// Releasing a missing run is a not-found error rather than a silent no-op.
	err = th.Repository.ReleaseRetentionLock(th.Context, ir.NewDAGRunRef("test_DAG", "missing-run"))
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunIDNotFound)
}

// A held run cannot be removed, its files stay in place, and the before-commit
// hook never runs.
func TestRetentionLockBlocksExplicitRemoval(t *testing.T) {
	th := setupTestRepository(t)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	th.CreateAttempt(t, ts, "held-run", ir.Failed)
	ref := ir.NewDAGRunRef("test_DAG", "held-run")

	_, err := th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{LockedBy: "alice"})
	require.NoError(t, err)

	hookRan := false
	err = th.Backend.RemoveDAGRun(th.Context, persis.DAGRunRemoveRequest{
		DAGRun:       ref,
		RejectActive: true,
		BeforeCommit: func(context.Context) error {
			hookRan = true
			return nil
		},
	})
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunRetentionLocked)
	assert.False(t, hookRan)

	// The run directory, attempts, and marker all survive.
	dataRoot := NewDataRoot(th.TmpDir, "test_DAG")
	run, findErr := dataRoot.FindByDAGRunID(th.Context, "held-run")
	require.NoError(t, findErr)
	require.FileExists(t, filepath.Join(run.baseDir, retentionFileName))
	_, err = run.LatestAttempt(th.Context, nil)
	require.NoError(t, err)

	// Once released, removal works through the repository as before.
	require.NoError(t, th.Repository.ReleaseRetentionLock(th.Context, ref))
	require.NoError(t, th.Repository.RemoveDAGRun(th.Context, ref, persis.DAGRunRemoveOptions{RejectActive: true}))
	_, err = dataRoot.FindByDAGRunID(th.Context, "held-run")
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunIDNotFound)
}

// Day/count sweeps classify held runs as retained, keep their files, and keep
// processing the other eligible runs.
func TestRetentionLockSurvivesRetentionSweep(t *testing.T) {
	th := setupTestRepository(t)
	old := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Now().UTC().Add(-time.Hour)

	oldHeld := th.CreateAttempt(t, old, "old-held", ir.Succeeded)
	oldFree := th.CreateAttempt(t, old, "old-free", ir.Succeeded)
	th.CreateAttempt(t, recent, "recent-free", ir.Succeeded)

	// The sweep gates on attempt-file modification times as well as run time.
	require.NoError(t, os.Chtimes(oldHeld.file, old, old))
	require.NoError(t, os.Chtimes(oldFree.file, old, old))

	ref := ir.NewDAGRunRef("test_DAG", "old-held")
	_, err := th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{LockedBy: "alice"})
	require.NoError(t, err)

	// Dry run clearly separates what it would delete from what it skips.
	preview, err := th.Repository.RemoveOldDAGRuns(th.Context, "test_DAG", 7, persis.DAGRunRetentionOptions{DryRun: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"old-free"}, preview.Removed)
	assert.Equal(t, []string{"old-held"}, preview.Retained)

	statuses, err := th.Repository.RecentStatuses(th.Context, "test_DAG", 10)
	require.NoError(t, err)
	require.Len(t, statuses, 3, "dry run must not delete anything")

	// Actual sweep removes only the unprotected old run.
	result, err := th.Repository.RemoveOldDAGRuns(th.Context, "test_DAG", 7, persis.DAGRunRetentionOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{"old-free"}, result.Removed)
	assert.Equal(t, []string{"old-held"}, result.Retained)

	dataRoot := NewDataRoot(th.TmpDir, "test_DAG")
	_, err = dataRoot.FindByDAGRunID(th.Context, "old-held")
	require.NoError(t, err, "held run survives the sweep")
	_, err = dataRoot.FindByDAGRunID(th.Context, "old-free")
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunIDNotFound)

	// Count-based retention honors the hold the same way.
	th.CreateAttempt(t, old, "old-held-2", ir.Failed)
	oldHeld2 := ir.NewDAGRunRef("test_DAG", "old-held-2")
	_, err = th.Repository.SetRetentionLock(th.Context, oldHeld2, persis.DAGRunRetentionLockInput{LockedBy: "bob"})
	require.NoError(t, err)

	keep := 1
	byRuns, err := th.Repository.RemoveOldDAGRuns(th.Context, "test_DAG", 0, persis.DAGRunRetentionOptions{
		RetentionRuns: &keep,
	})
	require.NoError(t, err)
	assert.NotContains(t, byRuns.Removed, "old-held")
	assert.NotContains(t, byRuns.Removed, "old-held-2")
	assert.Contains(t, byRuns.Retained, "old-held")
	assert.Contains(t, byRuns.Retained, "old-held-2")
}

// A marker that cannot be read fails closed: the run is treated as
// non-removable and removal returns an unreadable-state error.
func TestRetentionUnreadableFailsClosed(t *testing.T) {
	th := setupTestRepository(t)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	th.CreateAttempt(t, ts, "corrupt-run", ir.Succeeded)
	ref := ir.NewDAGRunRef("test_DAG", "corrupt-run")

	dataRoot := NewDataRoot(th.TmpDir, "test_DAG")
	run, err := dataRoot.FindByDAGRunID(th.Context, "corrupt-run")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(run.baseDir, retentionFileName), []byte("{not-json"), 0640))

	_, err = th.Repository.GetRetentionLock(th.Context, ref)
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunRetentionUnreadable)

	err = th.Repository.RemoveDAGRun(th.Context, ref, persis.DAGRunRemoveOptions{RejectActive: true})
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunRetentionUnreadable)

	// Set also refuses rather than overwriting an unreadable state.
	_, err = th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{LockedBy: "alice"})
	require.ErrorIs(t, err, domaindagrun.ErrDAGRunRetentionUnreadable)

	// The run and its marker are still on disk.
	_, err = dataRoot.FindByDAGRunID(th.Context, "corrupt-run")
	require.NoError(t, err)
}

// Set followed by a concurrent removal cannot interleave: whichever operation
// takes the ownership lock first finishes completely before the other starts.
func TestRetentionLockAndRemovalSerializeOnRootBoundary(t *testing.T) {
	th := setupTestRepository(t)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	th.CreateAttempt(t, ts, "race-run", ir.Succeeded)
	ref := ir.NewDAGRunRef("test_DAG", "race-run")

	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	removeErr := make(chan error, 1)
	go func() {
		removeErr <- th.Backend.RemoveDAGRun(th.Context, persis.DAGRunRemoveRequest{
			DAGRun:       ref,
			RejectActive: true,
			BeforeCommit: func(context.Context) error {
				close(hookEntered)
				<-releaseHook
				return nil
			},
		})
	}()

	<-hookEntered

	setErr := make(chan error, 1)
	go func() {
		_, e := th.Repository.SetRetentionLock(th.Context, ref, persis.DAGRunRetentionLockInput{LockedBy: "alice"})
		setErr <- e
	}()

	// While the removal holds the boundary, set cannot complete and the
	// removal cannot be preempted.
	select {
	case e := <-setErr:
		t.Fatalf("retention set completed while removal held the boundary: %v", e)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case <-removeErr:
		t.Fatal("removal finished before set blocked")
	default:
	}

	close(releaseHook)
	require.NoError(t, <-removeErr)
	// Set acquired the boundary only after the run was deleted, so it reports
	// not-found instead of protecting or resurrecting anything.
	require.ErrorIs(t, <-setErr, domaindagrun.ErrDAGRunIDNotFound)
}
