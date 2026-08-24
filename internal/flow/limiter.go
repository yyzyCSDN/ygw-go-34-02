package flow

import (
	"sync"
	"time"

	"jobsched/internal/model"
)

// Limiter is a per-executor token bucket.
type Limiter struct {
	mu    sync.Mutex
	rate  float64
	burst float64
	last  map[model.ExecutorID]bucket
}

type bucket struct {
	tokens float64
	seen   time.Time
}

// NewLimiter builds a limiter from a validated policy.
func NewLimiter(p Policy) *Limiter {
	if err := p.Validate(); err != nil {
		p = DefaultPolicy()
	}
	return &Limiter{
		rate:  p.Rate,
		burst: float64(p.Burst),
		last:  make(map[model.ExecutorID]bucket),
	}
}

// Allow consumes one token for the executor.
func (l *Limiter) Allow(exec model.ExecutorID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b := l.last[exec]
	if b.seen.IsZero() {
		b = bucket{tokens: l.burst, seen: now}
	} else {
		elapsed := now.Sub(b.seen).Seconds()
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.seen = now
	}
	if b.tokens < 1 {
		l.last[exec] = b
		return false
	}
	b.tokens--
	l.last[exec] = b
	return true
}

// Credits returns the remaining budget for the executor.
func (l *Limiter) Credits(exec model.ExecutorID) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last[exec].tokens
}
