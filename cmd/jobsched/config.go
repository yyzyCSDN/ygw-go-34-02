// Command jobsched runs the distributed job scheduler with a publish
// API, executor WebSocket endpoint and an operator console.
package main

import (
	"time"

	"jobsched/internal/flow"
)

// Config carries the scheduler runtime settings.
type Config struct {
	Addr           string
	TickInterval   time.Duration
	HeartbeatEvery time.Duration
	LeaseTimeout   time.Duration
	QueueSize      int
	StoreCapacity  int
	RunnerQueue    int
	RunnerTimeout  time.Duration
	MaxAttempts    int
	Flow           flow.Policy
}

// Load returns the default configuration for an address.
func Load(addr string) Config {
	return Config{
		Addr:           addr,
		TickInterval:   time.Second,
		HeartbeatEvery: 2 * time.Second,
		LeaseTimeout:   15 * time.Second,
		QueueSize:      1024,
		StoreCapacity:  2048,
		RunnerQueue:    64,
		RunnerTimeout:  2 * time.Second,
		MaxAttempts:    3,
		Flow:           flow.DefaultPolicy(),
	}
}
