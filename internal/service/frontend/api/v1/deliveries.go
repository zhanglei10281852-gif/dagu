// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"net/http"

	"github.com/dagucloud/dagu/v2/api/v1"
	"github.com/dagucloud/dagu/v2/internal/audit"
	"github.com/dagucloud/dagu/v2/internal/service/chatbridge"
)

func errDeliveryControllerUnavailable() *Error {
	return &Error{
		HTTPStatus: http.StatusServiceUnavailable,
		Code:       api.ErrorCodeInternalError,
		Message:    "Delivery management is unavailable",
	}
}

func toAPIDeliveryRecord(view chatbridge.DeliveryRecordView) api.NotificationDeliveryRecord {
	record := api.NotificationDeliveryRecord{
		RecordId:    chatbridge.EncodeDeliveryRecordID(view.Destination, view.EventKey),
		Destination: view.Destination,
		EventKey:    view.EventKey,
		Status:      api.NotificationDeliveryStatus(view.Status),
		Attempts:    view.Attempts,
		Generation:  view.Generation,
		CreatedAt:   view.CreatedAt,
		UpdatedAt:   view.UpdatedAt,
	}
	if view.DAGName != "" {
		record.DagName = &view.DAGName
	}
	if view.DAGRunID != "" {
		record.DagRunId = &view.DAGRunID
	}
	if view.AttemptID != "" {
		record.AttemptId = &view.AttemptID
	}
	if view.EventType != "" {
		record.EventType = &view.EventType
	}
	if view.DAGFile != "" {
		record.DagFile = &view.DAGFile
	}
	if !view.ObservedAt.IsZero() {
		observedAt := view.ObservedAt
		record.ObservedAt = &observedAt
	}
	if view.RunStatus != 0 {
		runStatus := int(view.RunStatus)
		record.RunStatus = &runStatus
	}
	if view.LastErrorCategory != "" {
		category := api.NotificationDeliveryErrorCategory(view.LastErrorCategory)
		record.LastErrorCategory = &category
	}
	if view.LastErrorSummary != "" {
		record.LastErrorSummary = &view.LastErrorSummary
	}
	record.LastAttemptAt = view.LastAttemptAt
	record.NextAttemptAt = view.NextAttemptAt
	record.DeliveredAt = view.DeliveredAt
	record.DeadLetterAt = view.DeadLetterAt
	if view.DeadLetterReason != "" {
		record.DeadLetterReason = &view.DeadLetterReason
	}
	return record
}

func toAPIDeliveryRecords(views []chatbridge.DeliveryRecordView) []api.NotificationDeliveryRecord {
	records := make([]api.NotificationDeliveryRecord, 0, len(views))
	for _, view := range views {
		records = append(records, toAPIDeliveryRecord(view))
	}
	return records
}

func toChatbridgeStatuses[T ~string](values []T) []chatbridge.DeliveryStatus {
	statuses := make([]chatbridge.DeliveryStatus, 0, len(values))
	for _, value := range values {
		statuses = append(statuses, chatbridge.DeliveryStatus(value))
	}
	return statuses
}

func deliveryRefsFromIDs(recordIDs []string) ([]chatbridge.DeliveryRecordRef, *Error) {
	refs := make([]chatbridge.DeliveryRecordRef, 0, len(recordIDs))
	for _, recordID := range recordIDs {
		destination, eventKey, err := chatbridge.DecodeDeliveryRecordID(recordID)
		if err != nil {
			return nil, badNotificationRequest(err.Error())
		}
		refs = append(refs, chatbridge.DeliveryRecordRef{Destination: destination, EventKey: eventKey})
	}
	return refs, nil
}

func toAPIRedriveResults(results []chatbridge.DeliveryRedriveResult) []api.NotificationDeliveryRedriveResult {
	mapped := make([]api.NotificationDeliveryRedriveResult, 0, len(results))
	for _, result := range results {
		mapped = append(mapped, api.NotificationDeliveryRedriveResult{
			RecordId:   result.RecordID,
			Outcome:    api.NotificationDeliveryRedriveOutcome(result.Outcome),
			Status:     api.NotificationDeliveryStatus(result.Status),
			Generation: result.Generation,
		})
	}
	return mapped
}

func (a *API) ListNotificationDeliveries(ctx context.Context, request api.ListNotificationDeliveriesRequestObject) (api.ListNotificationDeliveriesResponseObject, error) {
	if err := a.requireNotificationManagement(ctx); err != nil {
		return nil, err
	}
	controller := a.notificationDeliveryService()
	if controller == nil {
		return nil, errDeliveryControllerUnavailable()
	}
	filter := chatbridge.DeliveryRecordFilter{}
	if request.Params.Status != nil {
		filter.Statuses = toChatbridgeStatuses(*request.Params.Status)
	}
	if request.Params.Destination != nil {
		filter.Destination = *request.Params.Destination
	}
	cursor := ""
	if request.Params.Cursor != nil {
		cursor = *request.Params.Cursor
	}
	limit := 0
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	page, err := controller.ListDeliveryRecords(ctx, filter, cursor, limit)
	if err != nil {
		return nil, badNotificationRequest(err.Error())
	}
	var nextCursor *string
	if page.NextCursor != "" {
		nextCursor = &page.NextCursor
	}
	return api.ListNotificationDeliveries200JSONResponse{
		Records:    toAPIDeliveryRecords(page.Records),
		NextCursor: nextCursor,
	}, nil
}

func (a *API) GetNotificationDelivery(ctx context.Context, request api.GetNotificationDeliveryRequestObject) (api.GetNotificationDeliveryResponseObject, error) {
	if err := a.requireNotificationManagement(ctx); err != nil {
		return nil, err
	}
	controller := a.notificationDeliveryService()
	if controller == nil {
		return nil, errDeliveryControllerUnavailable()
	}
	destination, eventKey, err := chatbridge.DecodeDeliveryRecordID(request.RecordId)
	if err != nil {
		return nil, badNotificationRequest(err.Error())
	}
	view, ok, err := controller.GetDeliveryRecord(ctx, destination, eventKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notificationNotFound("notification delivery record not found")
	}
	return api.GetNotificationDelivery200JSONResponse(toAPIDeliveryRecord(view)), nil
}

func (a *API) RedriveNotificationDeliveries(ctx context.Context, request api.RedriveNotificationDeliveriesRequestObject) (api.RedriveNotificationDeliveriesResponseObject, error) {
	if err := a.requireNotificationManagement(ctx); err != nil {
		return nil, err
	}
	if request.Body == nil || len(request.Body.RecordIds) == 0 {
		return nil, badNotificationRequest("recordIds is required")
	}
	refs, badRequest := deliveryRefsFromIDs(request.Body.RecordIds)
	if badRequest != nil {
		return nil, badRequest
	}
	controller := a.notificationDeliveryService()
	if controller == nil {
		return nil, errDeliveryControllerUnavailable()
	}
	results, err := controller.RedriveDeliveryRecords(ctx, refs)
	if err != nil {
		if err == chatbridge.ErrDeliveryControllerInactive {
			return api.RedriveNotificationDeliveries503JSONResponse(api.Error{
				Code:    api.ErrorCodeInternalError,
				Message: "Delivery management is unavailable",
			}), nil
		}
		return nil, err
	}
	redriven := 0
	for _, result := range results {
		if result.Outcome == chatbridge.RedriveOutcomeRedriven {
			redriven++
		}
	}
	if redriven > 0 {
		a.logAudit(ctx, audit.CategoryNotification, "notification_delivery_redrive", map[string]any{
			"requested": len(refs),
			"redriven":  redriven,
		})
	}
	return api.RedriveNotificationDeliveries200JSONResponse{
		Results: toAPIRedriveResults(results),
	}, nil
}

func (a *API) ListIncidentDeliveries(ctx context.Context, request api.ListIncidentDeliveriesRequestObject) (api.ListIncidentDeliveriesResponseObject, error) {
	if err := a.requireIncidentManagement(ctx); err != nil {
		return nil, err
	}
	controller := a.incidentDeliveryService()
	if controller == nil {
		return nil, errDeliveryControllerUnavailable()
	}
	filter := chatbridge.DeliveryRecordFilter{}
	if request.Params.Status != nil {
		filter.Statuses = toChatbridgeStatuses(*request.Params.Status)
	}
	if request.Params.Destination != nil {
		filter.Destination = *request.Params.Destination
	}
	cursor := ""
	if request.Params.Cursor != nil {
		cursor = *request.Params.Cursor
	}
	limit := 0
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	page, err := controller.ListDeliveryRecords(ctx, filter, cursor, limit)
	if err != nil {
		return nil, badIncidentRequest(err.Error())
	}
	var nextCursor *string
	if page.NextCursor != "" {
		nextCursor = &page.NextCursor
	}
	return api.ListIncidentDeliveries200JSONResponse{
		Records:    toAPIDeliveryRecords(page.Records),
		NextCursor: nextCursor,
	}, nil
}

func (a *API) GetIncidentDelivery(ctx context.Context, request api.GetIncidentDeliveryRequestObject) (api.GetIncidentDeliveryResponseObject, error) {
	if err := a.requireIncidentManagement(ctx); err != nil {
		return nil, err
	}
	controller := a.incidentDeliveryService()
	if controller == nil {
		return nil, errDeliveryControllerUnavailable()
	}
	destination, eventKey, err := chatbridge.DecodeDeliveryRecordID(request.RecordId)
	if err != nil {
		return nil, badIncidentRequest(err.Error())
	}
	view, ok, err := controller.GetDeliveryRecord(ctx, destination, eventKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errIncidentRecordNotFound()
	}
	return api.GetIncidentDelivery200JSONResponse(toAPIDeliveryRecord(view)), nil
}

func (a *API) RedriveIncidentDeliveries(ctx context.Context, request api.RedriveIncidentDeliveriesRequestObject) (api.RedriveIncidentDeliveriesResponseObject, error) {
	if err := a.requireIncidentManagement(ctx); err != nil {
		return nil, err
	}
	if request.Body == nil || len(request.Body.RecordIds) == 0 {
		return nil, badIncidentRequest("recordIds is required")
	}
	refs, badRequest := deliveryRefsFromIDs(request.Body.RecordIds)
	if badRequest != nil {
		return nil, badIncidentRequest(badRequest.Message)
	}
	controller := a.incidentDeliveryService()
	if controller == nil {
		return nil, errDeliveryControllerUnavailable()
	}
	results, err := controller.RedriveDeliveryRecords(ctx, refs)
	if err != nil {
		if err == chatbridge.ErrDeliveryControllerInactive {
			return api.RedriveIncidentDeliveries503JSONResponse(api.Error{
				Code:    api.ErrorCodeInternalError,
				Message: "Delivery management is unavailable",
			}), nil
		}
		return nil, err
	}
	redriven := 0
	for _, result := range results {
		if result.Outcome == chatbridge.RedriveOutcomeRedriven {
			redriven++
		}
	}
	if redriven > 0 {
		a.logAudit(ctx, audit.CategoryIncident, "incident_delivery_redrive", map[string]any{
			"requested": len(refs),
			"redriven":  redriven,
		})
	}
	return api.RedriveIncidentDeliveries200JSONResponse{
		Results: toAPIRedriveResults(results),
	}, nil
}

func errIncidentRecordNotFound() *Error {
	return &Error{
		HTTPStatus: http.StatusNotFound,
		Code:       api.ErrorCodeNotFound,
		Message:    "Incident delivery record not found",
	}
}
