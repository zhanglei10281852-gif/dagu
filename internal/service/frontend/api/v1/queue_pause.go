// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/dagucloud/dagu/v2/api/v1"
	"github.com/dagucloud/dagu/v2/internal/audit"
	"github.com/dagucloud/dagu/v2/internal/auth"
	"github.com/dagucloud/dagu/v2/internal/cmn/config"
	"github.com/dagucloud/dagu/v2/internal/cmn/logger"
	"github.com/dagucloud/dagu/v2/internal/cmn/logger/tag"
	"github.com/dagucloud/dagu/v2/internal/queue"
)

// queuePauseUnavailable is returned when no per-queue pause store is
// configured, which means the deployment cannot record or observe one.
const queuePauseUnavailable = "Queue pause state not configured"

// maxQueuePauseReasonLength bounds the operator-supplied pause reason. It is
// enforced here in addition to the OpenAPI maxLength, which only applies when
// strict validation is enabled.
const maxQueuePauseReasonLength = 512

// GetQueuePauseState returns the durable pause state of one queue.
//
// Deliberately readable by any authenticated user: a frozen queue needs an
// explanation that operators can see without administrative rights.
func (a *API) GetQueuePauseState(ctx context.Context, req api.GetQueuePauseStateRequestObject) (api.GetQueuePauseStateResponseObject, error) {
	if a.queuePauseStore == nil {
		return api.GetQueuePauseStatedefaultJSONResponse{
			Body:       api.Error{Code: api.ErrorCodeInternalError, Message: queuePauseUnavailable},
			StatusCode: http.StatusInternalServerError,
		}, nil
	}

	state, err := a.queuePauseStore.Get(ctx, req.Name)
	if err != nil {
		logger.Error(ctx, "Failed to read queue pause state", tag.Queue(req.Name), tag.Error(err))
		return api.GetQueuePauseStatedefaultJSONResponse{
			Body:       api.Error{Code: api.ErrorCodeInternalError, Message: "Failed to read queue pause state"},
			StatusCode: http.StatusInternalServerError,
		}, nil
	}
	return api.GetQueuePauseState200JSONResponse(toQueuePauseStateResource(state)), nil
}

// UpdateQueuePauseState pauses or resumes one queue. It requires run
// permission, matching the other queue mutation (manual dequeue).
func (a *API) UpdateQueuePauseState(ctx context.Context, req api.UpdateQueuePauseStateRequestObject) (api.UpdateQueuePauseStateResponseObject, error) {
	if err := a.isAllowed(config.PermissionRunDAGs); err != nil {
		return nil, err
	}
	if a.queuePauseStore == nil {
		return api.UpdateQueuePauseStatedefaultJSONResponse{
			Body:       api.Error{Code: api.ErrorCodeInternalError, Message: queuePauseUnavailable},
			StatusCode: http.StatusInternalServerError,
		}, nil
	}
	if req.Body == nil {
		return nil, &Error{
			HTTPStatus: http.StatusBadRequest,
			Code:       api.ErrorCodeBadRequest,
			Message:    "Request body is required",
		}
	}

	var reason string
	if req.Body.Reason != nil {
		reason = *req.Body.Reason
	}
	if utf8.RuneCountInString(reason) > maxQueuePauseReasonLength {
		return nil, &Error{
			HTTPStatus: http.StatusBadRequest,
			Code:       api.ErrorCodeBadRequest,
			Message:    fmt.Sprintf("reason must be at most %d characters", maxQueuePauseReasonLength),
		}
	}

	actor := ""
	if user, ok := auth.UserFromContext(ctx); ok && user != nil {
		actor = user.Username
	}

	state, err := a.queuePauseStore.SetPause(ctx, req.Name, req.Body.Paused, actor, reason)
	if err != nil {
		// The store preserves the previous state on a write failure.
		logger.Error(ctx, "Failed to persist queue pause state", tag.Queue(req.Name), tag.Error(err))
		return nil, &Error{
			HTTPStatus: http.StatusInternalServerError,
			Code:       api.ErrorCodeInternalError,
			Message:    "Failed to update queue pause state; the previous state was kept",
		}
	}

	action := "queue_pause"
	if !req.Body.Paused {
		action = "queue_resume"
	}
	a.logAudit(ctx, audit.CategorySystem, action, map[string]any{
		"queue_name": req.Name,
		"paused":     req.Body.Paused,
		"reason":     reason,
	})

	return api.UpdateQueuePauseState200JSONResponse(toQueuePauseStateResource(state)), nil
}

func toQueuePauseStateResource(state queue.PauseState) api.QueuePauseState {
	response := api.QueuePauseState{Paused: state.Paused}
	if !state.PausedAt.IsZero() {
		formatted := state.PausedAt.UTC().Format(time.RFC3339)
		response.PausedAt = &formatted
	}
	if state.PausedBy != "" {
		response.PausedBy = &state.PausedBy
	}
	if state.Reason != "" {
		response.Reason = &state.Reason
	}
	return response
}
