// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"errors"
	"testing"
	"time"

	openapi "github.com/dagucloud/dagu/v2/api/v1"
	"github.com/dagucloud/dagu/v2/internal/auth"
	"github.com/dagucloud/dagu/v2/internal/service/chatbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDeliveryService struct {
	page         chatbridge.DeliveryRecordPage
	listErr      error
	lastFilter   chatbridge.DeliveryRecordFilter
	lastCursor   string
	lastLimit    int
	view         *chatbridge.DeliveryRecordView
	found        bool
	results      []chatbridge.DeliveryRedriveResult
	redriveErr   error
	redriveRefs  []chatbridge.DeliveryRecordRef
}

func (f *fakeDeliveryService) ListDeliveryRecords(_ context.Context, filter chatbridge.DeliveryRecordFilter, cursor string, limit int) (chatbridge.DeliveryRecordPage, error) {
	f.lastFilter = filter
	f.lastCursor = cursor
	f.lastLimit = limit
	return f.page, f.listErr
}

func (f *fakeDeliveryService) GetDeliveryRecord(_ context.Context, _, _ string) (chatbridge.DeliveryRecordView, bool, error) {
	if f.view == nil {
		return chatbridge.DeliveryRecordView{}, f.found, nil
	}
	return *f.view, f.found, nil
}

func (f *fakeDeliveryService) RedriveDeliveryRecords(_ context.Context, refs []chatbridge.DeliveryRecordRef) ([]chatbridge.DeliveryRedriveResult, error) {
	f.redriveRefs = refs
	return f.results, f.redriveErr
}

// notificationServiceAllowed is a nil-backed non-nil NotificationService that
// satisfies the management gate.
type notificationServiceAllowed struct{ NotificationService }

func deliveriesAPIWith(svc *fakeDeliveryService) *API {
	api := &API{notificationService: notificationServiceAllowed{}}
	api.notificationDeliveryProvider = func() DeliveryManagementService { return svc }
	api.incidentDeliveryProvider = func() DeliveryManagementService { return svc }
	return api
}

func sampleDeliveryView() chatbridge.DeliveryRecordView {
	now := time.Now().UTC()
	lastAttempt := now.Add(-time.Minute)
	next := now.Add(29 * time.Minute)
	category := chatbridge.DeliveryErrorRateLimited
	return chatbridge.DeliveryRecordView{
		Destination:       "dest-1",
		EventKey:          "run-1:attempt-1:failed",
		DAGName:           "briefing",
		DAGRunID:          "run-1",
		AttemptID:         "attempt-1",
		EventType:         "dag.run.failed",
		DAGFile:           "briefing.yaml",
		ObservedAt:        now.Add(-2 * time.Minute),
		Status:            chatbridge.DeliveryStatusPending,
		Attempts:          2,
		Generation:        1,
		LastErrorCategory: category,
		LastErrorSummary:  "HTTP 429",
		LastAttemptAt:     &lastAttempt,
		NextAttemptAt:     &next,
		CreatedAt:         now.Add(-3 * time.Minute),
		UpdatedAt:         now,
	}
}

func TestListNotificationDeliveries_MapsFilterAndPage(t *testing.T) {
	t.Parallel()

	svc := &fakeDeliveryService{page: chatbridge.DeliveryRecordPage{
		Records:    []chatbridge.DeliveryRecordView{sampleDeliveryView()},
		NextCursor: "next-page",
	}}
	api := deliveriesAPIWith(svc)

	destination := "dest-1"
	cursor := "cur"
	limit := 5
	status := openapi.ListNotificationDeliveriesParamsStatusDeadLetter
	resp, err := api.ListNotificationDeliveries(context.Background(), openapi.ListNotificationDeliveriesRequestObject{
		Params: openapi.ListNotificationDeliveriesParams{
			Status:      &[]openapi.ListNotificationDeliveriesParamsStatus{status},
			Destination: &destination,
			Cursor:      &cursor,
			Limit:       &limit,
		},
	})
	require.NoError(t, err)
	page, ok := resp.(openapi.ListNotificationDeliveries200JSONResponse)
	require.True(t, ok)
	require.Len(t, page.Records, 1)
	record := page.Records[0]
	assert.Equal(t, "dest-1", record.Destination)
	assert.Equal(t, openapi.NotificationDeliveryStatusPending, record.Status)
	assert.Equal(t, 2, record.Attempts)
	require.NotNil(t, record.LastErrorCategory)
	assert.Equal(t, openapi.NotificationDeliveryErrorCategoryRateLimited, *record.LastErrorCategory)
	assert.Equal(t, "HTTP 429", *record.LastErrorSummary)
	assert.Equal(t, "briefing", *record.DagName)
	assert.Equal(t, "run-1", *record.DagRunId)
	assert.Equal(t, "briefing.yaml", *record.DagFile)
	require.NotNil(t, record.NextAttemptAt)
	require.NotNil(t, page.NextCursor)
	assert.Equal(t, "next-page", *page.NextCursor)

	assert.Equal(t, []chatbridge.DeliveryStatus{chatbridge.DeliveryStatusDeadLetter}, svc.lastFilter.Statuses)
	assert.Equal(t, "dest-1", svc.lastFilter.Destination)
	assert.Equal(t, "cur", svc.lastCursor)
	assert.Equal(t, 5, svc.lastLimit)
}

func TestListNotificationDeliveries_RejectsBadCursor(t *testing.T) {
	t.Parallel()

	svc := &fakeDeliveryService{listErr: errors.New("pagination cursor is malformed")}
	api := deliveriesAPIWith(svc)
	cursor := "%%%not-base64"
	_, err := api.ListNotificationDeliveries(context.Background(), openapi.ListNotificationDeliveriesRequestObject{
		Params: openapi.ListNotificationDeliveriesParams{Cursor: &cursor},
	})
	require.Error(t, err)
	badRequest, ok := err.(*Error)
	require.True(t, ok)
	assert.Equal(t, 400, badRequest.HTTPStatus)
}

func TestListNotificationDeliveries_UnavailableWhenControllerMissing(t *testing.T) {
	t.Parallel()

	api := &API{notificationService: notificationServiceAllowed{}}
	_, err := api.ListNotificationDeliveries(context.Background(), openapi.ListNotificationDeliveriesRequestObject{})
	require.Error(t, err)
	assert.Equal(t, 503, err.(*Error).HTTPStatus)
}

func TestGetNotificationDelivery_DetailAndNotFound(t *testing.T) {
	t.Parallel()

	view := sampleDeliveryView()
	svc := &fakeDeliveryService{view: &view, found: true}
	api := deliveriesAPIWith(svc)
	recordID := chatbridge.EncodeDeliveryRecordID("dest-1", "run-1:attempt-1:failed")

	resp, err := api.GetNotificationDelivery(context.Background(), openapi.GetNotificationDeliveryRequestObject{RecordId: recordID})
	require.NoError(t, err)
	found, ok := resp.(openapi.GetNotificationDelivery200JSONResponse)
	require.True(t, ok)
	assert.Equal(t, "run-1:attempt-1:failed", found.EventKey)

	svc.found = false
	_, err = api.GetNotificationDelivery(context.Background(), openapi.GetNotificationDeliveryRequestObject{RecordId: recordID})
	require.Error(t, err)
	assert.Equal(t, 404, err.(*Error).HTTPStatus)

	_, err = api.GetNotificationDelivery(context.Background(), openapi.GetNotificationDeliveryRequestObject{RecordId: "bad"})
	require.Error(t, err)
	assert.Equal(t, 400, err.(*Error).HTTPStatus)
}

func TestRedriveNotificationDeliveries_MapsStableOutcomes(t *testing.T) {
	t.Parallel()

	deadID := chatbridge.EncodeDeliveryRecordID("dest-1", "dead-key")
	pendingID := chatbridge.EncodeDeliveryRecordID("dest-1", "pending-key")
	svc := &fakeDeliveryService{results: []chatbridge.DeliveryRedriveResult{
		{RecordID: deadID, Outcome: chatbridge.RedriveOutcomeRedriven, Status: chatbridge.DeliveryStatusPending, Generation: 2},
		{RecordID: pendingID, Outcome: chatbridge.RedriveOutcomeAlreadyPending, Status: chatbridge.DeliveryStatusPending, Generation: 2},
	}}
	api := deliveriesAPIWith(svc)

	resp, err := api.RedriveNotificationDeliveries(context.Background(), openapi.RedriveNotificationDeliveriesRequestObject{
		Body: &openapi.RedriveNotificationDeliveriesJSONRequestBody{RecordIds: []string{deadID, pendingID}},
	})
	require.NoError(t, err)
	page, ok := resp.(openapi.RedriveNotificationDeliveries200JSONResponse)
	require.True(t, ok)
	require.Len(t, page.Results, 2)
	assert.Equal(t, openapi.NotificationDeliveryRedriveOutcomeRedriven, page.Results[0].Outcome)
	assert.Equal(t, 2, page.Results[0].Generation)
	require.Len(t, svc.redriveRefs, 2)
	assert.Equal(t, "dest-1", svc.redriveRefs[0].Destination)
	assert.Equal(t, "dead-key", svc.redriveRefs[0].EventKey)

	// Empty body.
	_, err = api.RedriveNotificationDeliveries(context.Background(), openapi.RedriveNotificationDeliveriesRequestObject{
		Body: &openapi.RedriveNotificationDeliveriesJSONRequestBody{},
	})
	require.Error(t, err)
	assert.Equal(t, 400, err.(*Error).HTTPStatus)

	// Malformed id.
	_, err = api.RedriveNotificationDeliveries(context.Background(), openapi.RedriveNotificationDeliveriesRequestObject{
		Body: &openapi.RedriveNotificationDeliveriesJSONRequestBody{RecordIds: []string{"!!!"}},
	})
	require.Error(t, err)
	assert.Equal(t, 400, err.(*Error).HTTPStatus)
}

func TestRedriveNotificationDeliveries_InactiveControllerReturns503(t *testing.T) {
	t.Parallel()

	svc := &fakeDeliveryService{redriveErr: chatbridge.ErrDeliveryControllerInactive}
	api := deliveriesAPIWith(svc)
	id := chatbridge.EncodeDeliveryRecordID("dest-1", "dead-key")
	resp, err := api.RedriveNotificationDeliveries(context.Background(), openapi.RedriveNotificationDeliveriesRequestObject{
		Body: &openapi.RedriveNotificationDeliveriesJSONRequestBody{RecordIds: []string{id}},
	})
	require.NoError(t, err)
	_, ok := resp.(openapi.RedriveNotificationDeliveries503JSONResponse)
	assert.True(t, ok)
}

func TestIncidentDeliveryHandlers_WiredAndGated(t *testing.T) {
	t.Parallel()

	// Incident gate rejects when incident service management is unavailable.
	api := deliveriesAPIWith(&fakeDeliveryService{})
	_, err := api.ListIncidentDeliveries(context.Background(), openapi.ListIncidentDeliveriesRequestObject{})
	require.Error(t, err)
	assert.Equal(t, 404, err.(*Error).HTTPStatus)
}

func TestDeliveryHandlers_RequirePermission(t *testing.T) {
	t.Parallel()

	structAPI := &API{
		notificationService: notificationServiceAllowed{},
		authService:         struct{ AuthService }{},
	}
	ctx := auth.WithUser(context.Background(), &auth.User{ID: "viewer", Role: auth.RoleViewer})

	_, err := structAPI.ListNotificationDeliveries(ctx, openapi.ListNotificationDeliveriesRequestObject{})
	require.Error(t, err)
	assert.Equal(t, 403, err.(*Error).HTTPStatus)
}
