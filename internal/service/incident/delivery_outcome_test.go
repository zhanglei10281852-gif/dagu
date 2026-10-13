// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package incident

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/service/chatbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_DoJSONRequestClassifiesProviderResponses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		status       int
		retryAfter   string
		wantRetry    bool
		wantCategory chatbridge.DeliveryErrorCategory
	}{
		{name: "rate limited", status: http.StatusTooManyRequests, retryAfter: "15", wantRetry: true, wantCategory: chatbridge.DeliveryErrorRateLimited},
		{name: "server error", status: http.StatusBadGateway, wantRetry: true, wantCategory: chatbridge.DeliveryErrorServerError},
		{name: "request timeout", status: http.StatusRequestTimeout, wantRetry: true, wantCategory: chatbridge.DeliveryErrorNetwork},
		{name: "unauthorized", status: http.StatusUnauthorized, wantRetry: false, wantCategory: chatbridge.DeliveryErrorClientError},
		{name: "not found", status: http.StatusNotFound, wantRetry: false, wantCategory: chatbridge.DeliveryErrorClientError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(server.Close)

			svc := New(nil, WithHTTPClient(server.Client()))
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, strings.NewReader("{}"))
			require.NoError(t, err)

			_, err = svc.doJSONRequest(req)
			require.Error(t, err)
			outcome := deliveryOutcomeFromError(err)
			assert.Equal(t, tc.wantRetry, outcome.Retryable)
			assert.Equal(t, tc.wantCategory, outcome.Category)
			if tc.retryAfter != "" {
				assert.Equal(t, 15*time.Second, outcome.RetryAfter)
			}
		})
	}
}

func TestService_NetworkFailureSummaryIsSanitized(t *testing.T) {
	t.Parallel()

	secret := "abcdef1234567890abcdef1234567890"
	svc := New(nil)
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://127.0.0.1:1/hooks/"+secret,
		strings.NewReader("{}"),
	)
	require.NoError(t, err)

	_, err = svc.doJSONRequest(req)
	require.Error(t, err)
	outcome := deliveryOutcomeFromError(err)
	assert.True(t, outcome.Retryable)
	assert.Equal(t, chatbridge.DeliveryErrorNetwork, outcome.Category)
	assert.NotContains(t, outcome.Summary, secret)
}
