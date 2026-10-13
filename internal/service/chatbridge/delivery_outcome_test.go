// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeDeliveryMessage_RedactsSecrets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		text string
		bad  []string
	}{
		{
			name: "telegram bot token url",
			text: `Post "https://api.telegram.org/bot7901234567:AAEhBOweik_aeRushahjee9Thohk9aexaMP/sendMessage": dial tcp: i/o timeout`,
			bad:  []string{"AAEhBOweik_aeRushahjee9Thohk9aexaMP", "7901234567:AAEh"},
		},
		{
			name: "slack webhook path",
			text: `callback failed for https://hooks.slack.com/services/test-team/test-channel/test-webhook-token`,
			bad:  []string{"/services/test-team/test-channel/test-webhook-token", "test-webhook-token"},
		},
		{
			name: "bearer credential",
			bad:  []string{"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"},
			text: `authorization failed: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9 rejected`,
		},
		{
			name: "basic credential",
			text: `proxy auth failed: Basic dXNlcjpwYXNzd29yZA==`,
			bad:  []string{"dXNlcjpwYXNzd29yZA=="},
		},
		{
			name: "secret query parameter",
			text: `webhook https://example.com/hook?token=supersecretvalue&ok=1 returned 400`,
			bad:  []string{"supersecretvalue"},
		},
		{
			name: "url userinfo",
			text: `https://admin:p4ssword@example.com/webhooks/incoming denied`,
			bad:  []string{"admin:p4ssword"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SanitizeDeliveryMessage(tc.text)
			for _, secret := range tc.bad {
				assert.NotContains(t, got, secret)
			}
		})
	}
}

func TestSanitizeDeliveryMessage_BoundsLength(t *testing.T) {
	t.Parallel()

	got := SanitizeDeliveryMessage(strings.Repeat("x", maxDeliveryErrorSummaryRunes*2))
	assert.LessOrEqual(t, len([]rune(got)), maxDeliveryErrorSummaryRunes)
}

func TestSanitizeDeliveryMessage_EmptyPassthrough(t *testing.T) {
	t.Parallel()
	assert.Empty(t, SanitizeDeliveryMessage("  "))
	assert.Equal(t, "HTTP 503", SanitizeDeliveryMessage("  HTTP   503 "))
}
