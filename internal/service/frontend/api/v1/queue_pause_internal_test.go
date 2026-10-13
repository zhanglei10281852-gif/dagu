// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	openapiv1 "github.com/dagucloud/dagu/v2/api/v1"
	"github.com/dagucloud/dagu/v2/internal/cmn/config"
	"github.com/dagucloud/dagu/v2/internal/persis/file"
	"github.com/dagucloud/dagu/v2/internal/persis/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queuePauseTestAPI(t *testing.T, configure func(*config.Config)) (*API, string) {
	t.Helper()
	tmpDir := t.TempDir()
	queueDir := filepath.Join(tmpDir, "queue")
	cfg := &config.Config{}
	if configure != nil {
		configure(cfg)
	}
	a := &API{
		queuePauseStore: store.NewQueuePauseStore(file.NewCollection(queueDir)),
		config:          cfg,
	}
	return a, "maintenance-q"
}

func TestGetQueuePauseStateDefaultsToNotPaused(t *testing.T) {
	t.Parallel()
	a, queueName := queuePauseTestAPI(t, nil)

	resp, err := a.GetQueuePauseState(context.Background(), openapiv1.GetQueuePauseStateRequestObject{Name: queueName})
	require.NoError(t, err)
	body, ok := resp.(openapiv1.GetQueuePauseState200JSONResponse)
	require.True(t, ok)
	assert.False(t, body.Paused)
	assert.Nil(t, body.PausedAt)
	assert.Nil(t, body.Reason)
}

func TestUpdateQueuePauseStateRoundTripAndIdempotent(t *testing.T) {
	t.Parallel()
	a, queueName := queuePauseTestAPI(t, func(c *config.Config) {
		c.Server.Permissions = map[config.Permission]bool{config.PermissionRunDAGs: true}
	})
	ctx := context.Background()

	reason := "downstream maintenance window"
	pauseResp, err := a.UpdateQueuePauseState(ctx, openapiv1.UpdateQueuePauseStateRequestObject{
		Name: queueName,
		Body: &openapiv1.UpdateQueuePauseStateJSONRequestBody{Paused: true, Reason: &reason},
	})
	require.NoError(t, err)
	paused, ok := pauseResp.(openapiv1.UpdateQueuePauseState200JSONResponse)
	require.True(t, ok)
	assert.True(t, paused.Paused)
	require.NotNil(t, paused.PausedAt)
	require.NotNil(t, paused.Reason)
	assert.Equal(t, reason, *paused.Reason)
	firstPausedAt := *paused.PausedAt

	// Repeating the pause is idempotent: same pausedAt, no error.
	againResp, err := a.UpdateQueuePauseState(ctx, openapiv1.UpdateQueuePauseStateRequestObject{
		Name: queueName,
		Body: &openapiv1.UpdateQueuePauseStateJSONRequestBody{Paused: true, Reason: &reason},
	})
	require.NoError(t, err)
	again, ok := againResp.(openapiv1.UpdateQueuePauseState200JSONResponse)
	require.True(t, ok)
	assert.True(t, again.Paused)
	assert.Equal(t, firstPausedAt, *again.PausedAt)

	// Resuming clears the metadata.
	resumeResp, err := a.UpdateQueuePauseState(ctx, openapiv1.UpdateQueuePauseStateRequestObject{
		Name: queueName,
		Body: &openapiv1.UpdateQueuePauseStateJSONRequestBody{Paused: false},
	})
	require.NoError(t, err)
	resumed, ok := resumeResp.(openapiv1.UpdateQueuePauseState200JSONResponse)
	require.True(t, ok)
	assert.False(t, resumed.Paused)
	assert.Nil(t, resumed.PausedAt)
	assert.Nil(t, resumed.Reason)

	// A second resume stays idempotent and successful.
	_, err = a.UpdateQueuePauseState(ctx, openapiv1.UpdateQueuePauseStateRequestObject{
		Name: queueName,
		Body: &openapiv1.UpdateQueuePauseStateJSONRequestBody{Paused: false},
	})
	require.NoError(t, err)
}

func TestUpdateQueuePauseStateRejectsLongReason(t *testing.T) {
	t.Parallel()
	a, queueName := queuePauseTestAPI(t, func(c *config.Config) {
		c.Server.Permissions = map[config.Permission]bool{config.PermissionRunDAGs: true}
	})
	longReason := strings.Repeat("x", maxQueuePauseReasonLength+1)

	_, err := a.UpdateQueuePauseState(context.Background(), openapiv1.UpdateQueuePauseStateRequestObject{
		Name: queueName,
		Body: &openapiv1.UpdateQueuePauseStateJSONRequestBody{Paused: true, Reason: &longReason},
	})
	require.Error(t, err)
	apiErr, ok := err.(*Error)
	require.True(t, ok)
	assert.Equal(t, 400, apiErr.HTTPStatus)
}

func TestUpdateQueuePauseStateRequiresRunPermission(t *testing.T) {
	t.Parallel()
	a, queueName := queuePauseTestAPI(t, nil)

	_, err := a.UpdateQueuePauseState(context.Background(), openapiv1.UpdateQueuePauseStateRequestObject{
		Name: queueName,
		Body: &openapiv1.UpdateQueuePauseStateJSONRequestBody{Paused: true},
	})
	require.Error(t, err)
	apiErr, ok := err.(*Error)
	require.True(t, ok)
	assert.Equal(t, 403, apiErr.HTTPStatus)
}

func TestQueuePauseEndpointsWithoutStoreReturnInternalError(t *testing.T) {
	t.Parallel()
	a := &API{config: &config.Config{}}

	getResp, err := a.GetQueuePauseState(context.Background(), openapiv1.GetQueuePauseStateRequestObject{Name: "q"})
	require.NoError(t, err)
	getErr, ok := getResp.(openapiv1.GetQueuePauseStatedefaultJSONResponse)
	require.True(t, ok)
	assert.Equal(t, 500, getErr.StatusCode)
}

func TestGetUnknownQueueStillNotFoundWithPauseStore(t *testing.T) {
	t.Parallel()
	a, queueName := queuePauseTestAPI(t, nil)

	resp, err := a.GetQueue(context.Background(), openapiv1.GetQueueRequestObject{Name: queueName})
	require.Error(t, err)
	assert.Nil(t, resp)
	apiErr, ok := err.(*Error)
	require.True(t, ok)
	assert.Equal(t, 404, apiErr.HTTPStatus, "a never-used queue name must stay not found just because a pause store is configured")
}

func TestGetQueueSurfacesPausedQueueWithoutActivity(t *testing.T) {
	t.Parallel()
	a, queueName := queuePauseTestAPI(t, nil)
	ctx := context.Background()
	_, err := a.queuePauseStore.SetPause(ctx, queueName, true, "alice", "frozen")
	require.NoError(t, err)

	resp, err := a.GetQueue(ctx, openapiv1.GetQueueRequestObject{Name: queueName})
	require.NoError(t, err)
	queueResp, ok := resp.(openapiv1.GetQueue200JSONResponse)
	require.True(t, ok)
	assert.Equal(t, queueName, queueResp.Name)
	require.NotNil(t, queueResp.Pause)
	assert.True(t, queueResp.Pause.Paused)
	require.NotNil(t, queueResp.Pause.Reason)
	assert.Equal(t, "frozen", *queueResp.Pause.Reason)
	assert.Equal(t, "alice", *queueResp.Pause.PausedBy)

	listResp, err := a.ListQueues(ctx, openapiv1.ListQueuesRequestObject{})
	require.NoError(t, err)
	list, ok := listResp.(openapiv1.ListQueues200JSONResponse)
	require.True(t, ok)
	var found bool
	for _, q := range list.Queues {
		if q.Name == queueName {
			found = true
			require.NotNil(t, q.Pause)
			assert.True(t, q.Pause.Paused)
		}
	}
	assert.True(t, found, "paused queue with no activity must still appear in the list")

	// After resume an inactive, empty queue no longer needs a listing entry.
	_, err = a.queuePauseStore.SetPause(ctx, queueName, false, "", "")
	require.NoError(t, err)
	resp2, err := a.GetQueue(ctx, openapiv1.GetQueueRequestObject{Name: queueName})
	require.NoError(t, err)
	queue2, ok := resp2.(openapiv1.GetQueue200JSONResponse)
	require.True(t, ok)
	require.NotNil(t, queue2.Pause)
	assert.False(t, queue2.Pause.Paused)
}
