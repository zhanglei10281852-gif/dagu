// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package notification

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/service/chatbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_DeliverRequestClassifiesHTTPStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		status      int
		headers     map[string]string
		wantRetry   bool
		wantCategory chatbridge.DeliveryErrorCategory
	}{
		{name: "rate limited", status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "42"}, wantRetry: true, wantCategory: chatbridge.DeliveryErrorRateLimited},
		{name: "server error", status: http.StatusServiceUnavailable, wantRetry: true, wantCategory: chatbridge.DeliveryErrorServerError},
		{name: "request timeout", status: http.StatusRequestTimeout, wantRetry: true, wantCategory: chatbridge.DeliveryErrorNetwork},
		{name: "not found", status: http.StatusNotFound, wantRetry: false, wantCategory: chatbridge.DeliveryErrorClientError},
		{name: "bad request", status: http.StatusBadRequest, wantRetry: false, wantCategory: chatbridge.DeliveryErrorClientError},
		{name: "unauthorized", status: http.StatusUnauthorized, wantRetry: false, wantCategory: chatbridge.DeliveryErrorClientError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for key, value := range tc.headers {
					w.Header().Set(key, value)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":"failure"}`))
			})
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)

			svc := New(nil, nil, WithHTTPClient(server.Client()))
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, strings.NewReader("{}"))
			require.NoError(t, err)

			err = svc.deliverRequest(req, nil)
			require.Error(t, err)
			outcome := deliveryOutcomeFromError(err)
			assert.False(t, outcome.Delivered)
			assert.Equal(t, tc.wantRetry, outcome.Retryable)
			assert.Equal(t, tc.wantCategory, outcome.Category)
			if tc.status == http.StatusTooManyRequests {
				assert.Equal(t, 42*time.Second, outcome.RetryAfter)
			}
			assert.Contains(t, outcome.Summary, "HTTP")
		})
	}
}

func TestService_RetryAfterHTTPDate(t *testing.T) {
	t.Parallel()

	when := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", when)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	svc := New(nil, nil, WithHTTPClient(server.Client()))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, strings.NewReader("{}"))
	require.NoError(t, err)

	outcome := deliveryOutcomeFromError(svc.deliverRequest(req, nil))
	require.False(t, outcome.Delivered)
	assert.Equal(t, chatbridge.DeliveryErrorRateLimited, outcome.Category)
	assert.Greater(t, outcome.RetryAfter, 20*time.Second)
	assert.LessOrEqual(t, outcome.RetryAfter, 35*time.Second)
}

func TestService_NetworkErrorIsClassifiedAndSanitized(t *testing.T) {
	t.Parallel()

	// Nothing listens on the port; embed a bot token in the path to prove URL
	// secrets never reach the recorded summary.
	secret := "7901234567:AAEhBOweik_aeRushahjee9Thohk9aexaMP"
	targetURL := "http://127.0.0.1:1/bot" + secret + "/sendMessage"

	svc := New(nil, nil)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, targetURL, strings.NewReader("{}"))
	require.NoError(t, err)

	err = svc.deliverRequest(req, nil)
	require.Error(t, err)
	assert.True(t, isTemporaryDeliveryError(err))

	outcome := deliveryOutcomeFromError(err)
	assert.False(t, outcome.Delivered)
	assert.True(t, outcome.Retryable)
	assert.Equal(t, chatbridge.DeliveryErrorNetwork, outcome.Category)
	assert.NotEmpty(t, outcome.Summary)
	assert.NotContains(t, outcome.Summary, secret)
}

func TestService_LocalConfigurationErrorIsPermanent(t *testing.T) {
	t.Parallel()

	outcome := deliveryOutcomeFromError(errors.New("webhook url targets loopback or private network"))
	assert.False(t, outcome.Delivered)
	assert.False(t, outcome.Retryable)
	assert.Equal(t, chatbridge.DeliveryErrorConfiguration, outcome.Category)
}

func TestService_TeamsThrottleBodyIsRateLimited(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Microsoft Teams endpoint returned HTTP error 429"))
	}))
	t.Cleanup(server.Close)

	svc := New(nil, nil, WithHTTPClient(server.Client()))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, strings.NewReader("{}"))
	require.NoError(t, err)

	err = svc.deliverRequest(req, teamsThrottleError)
	require.Error(t, err)
	outcome := deliveryOutcomeFromError(err)
	assert.True(t, outcome.Retryable)
	assert.Equal(t, chatbridge.DeliveryErrorRateLimited, outcome.Category)
}

func TestService_DeliveryOutcomeSummaryRedactsEmbeddedSecrets(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid callback https://hooks.example.com/services/T000/B000/Shh_SecretToken?token=abc123secret"))
	}))
	t.Cleanup(server.Close)

	svc := New(nil, nil, WithHTTPClient(server.Client()))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, strings.NewReader("{}"))
	require.NoError(t, err)

	outcome := deliveryOutcomeFromError(svc.deliverRequest(req, nil))
	assert.Equal(t, chatbridge.DeliveryErrorClientError, outcome.Category)
	assert.NotContains(t, outcome.Summary, "Shh_SecretToken")
	assert.NotContains(t, outcome.Summary, "abc123secret")
	assert.NotContains(t, outcome.Summary, "/services/T000/B000")
}
