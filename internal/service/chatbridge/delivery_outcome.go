// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// maxDeliveryErrorSummaryRunes bounds the credential-free error summary
// persisted with a delivery record.
const maxDeliveryErrorSummaryRunes = 500

// DeliveryOutcome is the structured result of one batch delivery attempt.
type DeliveryOutcome struct {
	// Delivered reports whether the batch was acknowledged by the destination.
	Delivered bool
	// Retryable reports whether the monitor should keep retrying with backoff
	// instead of moving the records to the dead-letter.
	Retryable bool
	// Category classifies the most recent failure.
	Category DeliveryErrorCategory
	// Summary is a short, credential-free human-readable failure description.
	Summary string
	// RetryAfter optionally carries a server-advertised minimum wait (429).
	RetryAfter time.Duration
}

// DeliveredDeliveryOutcome is the canonical successful outcome.
func DeliveredDeliveryOutcome() DeliveryOutcome {
	return DeliveryOutcome{Delivered: true}
}

// RetryableDeliveryOutcome marks a batch for persistent backoff retry.
func RetryableDeliveryOutcome(category DeliveryErrorCategory, summary string) DeliveryOutcome {
	return DeliveryOutcome{
		Delivered: false,
		Retryable: true,
		Category:   category,
		Summary:    SanitizeDeliveryMessage(summary),
	}
}

// PermanentDeliveryOutcome marks a batch as a deterministic failure that must
// stop automatic delivery.
func PermanentDeliveryOutcome(category DeliveryErrorCategory, summary string) DeliveryOutcome {
	return DeliveryOutcome{
		Delivered: false,
		Retryable: false,
		Category:   category,
		Summary:    SanitizeDeliveryMessage(summary),
	}
}

// DetailedNotificationTransport is implemented by transports that can report
// why a batch delivery failed. Transports keep satisfying
// NotificationTransport, so the interface is optional and backward
// compatible; the monitor falls back to the boolean result otherwise.
type DetailedNotificationTransport interface {
	FlushNotificationBatchWithResult(ctx context.Context, destination string, batch NotificationBatch, allowLLM bool) DeliveryOutcome
}

var (
	// Full URLs: keep scheme and host only, redacting path/query secrets such
	// as Slack incoming webhook paths and Telegram bot tokens.
	deliveryURLPattern = regexp.MustCompile("https?://[^\\s\"'<>`]+")
	// Telegram bot tokens embedded outside of a URL.
	deliveryTelegramTokenPattern = regexp.MustCompile(`bot\d{5,}:[A-Za-z0-9_-]{20,}`)
	// Bearer/basic credentials.
	deliveryCredentialHeaderPattern = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	// Secret-bearing query parameters.
	deliverySecretQueryPattern = regexp.MustCompile("(?i)([?&](?:token|access_token|api_key|apikey|secret|password|passwd|key)=)[^&\\s\"'<>`]*")
	// URL userinfo.
	deliveryUserInfoPattern = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/@"'<>]+@`)
)

// SanitizeDeliveryMessage removes credentials and secrets from a transport
// error message and bounds its length. It never returns the empty string for
// non-empty input so operators retain an actionable hint.
func SanitizeDeliveryMessage(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}

	text = deliveryURLPattern.ReplaceAllStringFunc(text, func(match string) string {
		parsed, err := url.Parse(match)
		if err != nil || parsed.Host == "" {
			return "[url redacted]"
		}
		return parsed.Scheme + "://" + parsed.Host + "/[redacted]"
	})
	text = deliveryUserInfoPattern.ReplaceAllString(text, "${1}[redacted]@")
	text = deliveryTelegramTokenPattern.ReplaceAllString(text, "bot[redacted]")
	text = deliveryCredentialHeaderPattern.ReplaceAllString(text, "${1} [redacted]")
	text = deliverySecretQueryPattern.ReplaceAllString(text, "${1}[redacted]")
	text = strings.Join(strings.Fields(text), " ")

	runes := []rune(text)
	if len(runes) > maxDeliveryErrorSummaryRunes {
		text = string(runes[:maxDeliveryErrorSummaryRunes-1]) + "…"
	}
	return text
}
