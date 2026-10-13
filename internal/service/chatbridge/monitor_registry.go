// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package chatbridge

import "sync"

// MonitorRegistry tracks the monitor instance that currently owns the
// delivery lease within a process. In a start-all deployment the scheduler
// and frontend each create a monitor but only one wins the file lease;
// management operations (manual redrive) are routed to that instance.
type MonitorRegistry struct {
	mu     sync.RWMutex
	active *NotificationMonitor
}

// NewMonitorRegistry creates an empty registry.
func NewMonitorRegistry() *MonitorRegistry {
	return &MonitorRegistry{}
}

// SetActive registers the lease-owning monitor.
func (r *MonitorRegistry) SetActive(monitor *NotificationMonitor) {
	if r == nil || monitor == nil {
		return
	}
	r.mu.Lock()
	r.active = monitor
	r.mu.Unlock()
}

// ClearActive unregisters the monitor only if it is still the registered one,
// so a standby instance never clears a newly active owner.
func (r *MonitorRegistry) ClearActive(monitor *NotificationMonitor) {
	if r == nil || monitor == nil {
		return
	}
	r.mu.Lock()
	if r.active == monitor {
		r.active = nil
	}
	r.mu.Unlock()
}

// Active returns the lease-owning monitor, or nil when this process has no
// active delivery controller.
func (r *MonitorRegistry) Active() *NotificationMonitor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	active := r.active
	r.mu.RUnlock()
	return active
}
