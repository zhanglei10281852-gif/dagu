// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationDeliveryDefaults(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader("")))

	loader := NewConfigLoader(v, WithService(ServiceServer), WithAppHomeDir(t.TempDir()))
	cfg, err := loader.Load()
	require.NoError(t, err)

	delivery := cfg.Notification.Delivery
	assert.Equal(t, 10, delivery.MaxAttempts)
	assert.Equal(t, 30*time.Second, delivery.InitialBackoff)
	assert.Equal(t, time.Hour, delivery.MaxBackoff)
	assert.Equal(t, 14*24*time.Hour, delivery.DeadLetterTTL)
	assert.Equal(t, 1000, delivery.DeadLetterLimit)
}

func TestNotificationDeliveryOverrides(t *testing.T) {
	yaml := `
notification:
  delivery:
    max_attempts: 3
    initial_backoff: 5s
    max_backoff: 2m
    dead_letter_ttl: 24h
    dead_letter_limit: 42
`
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yaml)))

	loader := NewConfigLoader(v, WithService(ServiceServer), WithAppHomeDir(t.TempDir()))
	cfg, err := loader.Load()
	require.NoError(t, err)

	delivery := cfg.Notification.Delivery
	assert.Equal(t, 3, delivery.MaxAttempts)
	assert.Equal(t, 5*time.Second, delivery.InitialBackoff)
	assert.Equal(t, 2*time.Minute, delivery.MaxBackoff)
	assert.Equal(t, 24*time.Hour, delivery.DeadLetterTTL)
	assert.Equal(t, 42, delivery.DeadLetterLimit)
}

func TestNotificationDeliveryInvalidDurationWarnsAndKeepsDefault(t *testing.T) {
	yaml := `
notification:
  delivery:
    initial_backoff: "soon"
`
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yaml)))

	loader := NewConfigLoader(v, WithService(ServiceServer), WithAppHomeDir(t.TempDir()))
	cfg, err := loader.Load()
	require.NoError(t, err)
	require.NotEmpty(t, loader.warnings)
	assert.True(t, strings.Contains(loader.warnings[0], "initial_backoff"))
	// The invalid value does not get through; chatbridge config normalization
	// (covered in the chatbridge package) turns zero into the built-in default.
	assert.Zero(t, cfg.Notification.Delivery.InitialBackoff)
}
