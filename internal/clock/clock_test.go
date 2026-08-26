package clock

import (
	"testing"
	"time"
)

func TestNextWindowContains(t *testing.T) {
	now := time.Date(2026, 8, 22, 10, 0, 30, 0, time.UTC)
	win := NextWindow(now, time.Minute)
	if !win.Contains(time.Date(2026, 8, 22, 10, 0, 45, 0, time.UTC)) {
		t.Fatal("window should contain a due time inside the interval")
	}
	if win.Contains(time.Date(2026, 8, 22, 10, 1, 0, 0, time.UTC)) {
		t.Fatal("window should exclude the next interval start")
	}
}

func TestSystemClock(t *testing.T) {
	if (System{}).Now().IsZero() {
		t.Fatal("system clock returned zero time")
	}
}
