package queue

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{10, 1024 * time.Second},
		{100, time.Hour}, // capped at 1 hour
	}

	for _, tc := range tests {
		got := exponentialBackoff(tc.attempt)
		if got != tc.expected {
			t.Errorf("attempt %d: got %v, want %v", tc.attempt, got, tc.expected)
		}
	}
}

func TestExponentialBackoff_NeverExceedsOneHour(t *testing.T) {
	for attempt := 1; attempt <= 200; attempt++ {
		d := exponentialBackoff(attempt)
		if d > time.Hour {
			t.Errorf("attempt %d returned %v, exceeds 1 hour cap", attempt, d)
		}
	}
}
