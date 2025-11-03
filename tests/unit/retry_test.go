package unit

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sdd-kafka-producer/internal/retry"
)

func TestExponentialBackoff_CalculateDelay(t *testing.T) {
	tests := []struct {
		name            string
		initialDelay    time.Duration
		multiplier      float64
		maxDelay        time.Duration
		attempt         int
		expectedDelayMs int64 // Expected delay in milliseconds
		tolerance       int64 // Tolerance for jitter in milliseconds
	}{
		{
			name:            "first attempt",
			initialDelay:    100 * time.Millisecond,
			multiplier:      2.0,
			maxDelay:        10 * time.Second,
			attempt:         1,
			expectedDelayMs: 100,
			tolerance:       20, // 20% jitter tolerance
		},
		{
			name:            "second attempt",
			initialDelay:    100 * time.Millisecond,
			multiplier:      2.0,
			maxDelay:        10 * time.Second,
			attempt:         2,
			expectedDelayMs: 200,
			tolerance:       40,
		},
		{
			name:            "third attempt",
			initialDelay:    100 * time.Millisecond,
			multiplier:      2.0,
			maxDelay:        10 * time.Second,
			attempt:         3,
			expectedDelayMs: 400,
			tolerance:       80,
		},
		{
			name:            "max delay reached",
			initialDelay:    1 * time.Second,
			multiplier:      2.0,
			maxDelay:        5 * time.Second,
			attempt:         10, // Should hit max delay
			expectedDelayMs: 5000,
			tolerance:       1000,
		},
		{
			name:            "zero attempt",
			initialDelay:    100 * time.Millisecond,
			multiplier:      2.0,
			maxDelay:        10 * time.Second,
			attempt:         0,
			expectedDelayMs: 100, // Should default to initial delay
			tolerance:       20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := retry.NewExponentialBackoff(tt.initialDelay, tt.multiplier, tt.maxDelay)

			delay := backoff.CalculateDelay(tt.attempt)
			delayMs := delay.Milliseconds()

			// Check that delay is within expected range (considering jitter)
			minDelayMs := tt.expectedDelayMs - tt.tolerance
			maxDelayMs := tt.expectedDelayMs + tt.tolerance

			assert.GreaterOrEqual(t, delayMs, minDelayMs,
				"Delay %dms should be >= %dms", delayMs, minDelayMs)
			assert.LessOrEqual(t, delayMs, maxDelayMs,
				"Delay %dms should be <= %dms", delayMs, maxDelayMs)
		})
	}
}

func TestExponentialBackoff_WithoutJitter(t *testing.T) {
	backoff := retry.NewExponentialBackoffWithOptions(retry.BackoffOptions{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     10 * time.Second,
		Jitter:       false, // Disable jitter for exact testing
	})

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 3200 * time.Millisecond},
		{7, 6400 * time.Millisecond},
		{8, 10 * time.Second},  // Should be capped at max delay
		{10, 10 * time.Second}, // Should remain at max delay
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			delay := backoff.CalculateDelay(tt.attempt)
			assert.Equal(t, tt.expected, delay,
				"Attempt %d should have delay %v, got %v", tt.attempt, tt.expected, delay)
		})
	}
}

func TestExponentialBackoff_Reset(t *testing.T) {
	backoff := retry.NewExponentialBackoff(100*time.Millisecond, 2.0, 10*time.Second)

	// Calculate some delays to change internal state
	backoff.CalculateDelay(3)
	backoff.CalculateDelay(5)

	// Reset should restore to initial state
	backoff.Reset()

	// First delay after reset should be initial delay
	delay := backoff.CalculateDelay(1)
	expectedRange := 100 * time.Millisecond
	tolerance := 20 * time.Millisecond // 20% tolerance for jitter

	assert.InDelta(t, expectedRange.Milliseconds(), delay.Milliseconds(),
		float64(tolerance.Milliseconds()), "Reset should restore initial delay")
}

func TestExponentialBackoff_InvalidInputs(t *testing.T) {
	tests := []struct {
		name         string
		initialDelay time.Duration
		multiplier   float64
		maxDelay     time.Duration
		shouldPanic  bool
	}{
		{
			name:         "negative initial delay",
			initialDelay: -100 * time.Millisecond,
			multiplier:   2.0,
			maxDelay:     10 * time.Second,
			shouldPanic:  true,
		},
		{
			name:         "zero multiplier",
			initialDelay: 100 * time.Millisecond,
			multiplier:   0.0,
			maxDelay:     10 * time.Second,
			shouldPanic:  true,
		},
		{
			name:         "negative multiplier",
			initialDelay: 100 * time.Millisecond,
			multiplier:   -1.5,
			maxDelay:     10 * time.Second,
			shouldPanic:  true,
		},
		{
			name:         "max delay less than initial",
			initialDelay: 10 * time.Second,
			multiplier:   2.0,
			maxDelay:     1 * time.Second,
			shouldPanic:  true,
		},
		{
			name:         "valid inputs",
			initialDelay: 100 * time.Millisecond,
			multiplier:   1.5,
			maxDelay:     30 * time.Second,
			shouldPanic:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					retry.NewExponentialBackoff(tt.initialDelay, tt.multiplier, tt.maxDelay)
				}, "Should panic with invalid inputs")
			} else {
				assert.NotPanics(t, func() {
					retry.NewExponentialBackoff(tt.initialDelay, tt.multiplier, tt.maxDelay)
				}, "Should not panic with valid inputs")
			}
		})
	}
}

func TestExponentialBackoff_Jitter(t *testing.T) {
	backoff := retry.NewExponentialBackoffWithOptions(retry.BackoffOptions{
		InitialDelay: 1000 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     10 * time.Second,
		Jitter:       true,
	})

	// Test that jitter introduces variance
	delays := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		delays[i] = backoff.CalculateDelay(3) // Same attempt number
		backoff.Reset()                       // Reset to ensure consistent base calculation
	}

	// Check that we have some variance (not all delays are identical)
	firstDelay := delays[0]
	hasVariance := false
	for _, delay := range delays[1:] {
		if delay != firstDelay {
			hasVariance = true
			break
		}
	}

	assert.True(t, hasVariance, "Jitter should introduce variance in delays")

	// Check that all delays are within reasonable bounds (±20% of expected)
	expected := 4000 * time.Millisecond // 1000ms * 2^2 = 4000ms for attempt 3
	tolerance := 800 * time.Millisecond // 20% tolerance

	for i, delay := range delays {
		assert.InDelta(t, expected.Milliseconds(), delay.Milliseconds(),
			float64(tolerance.Milliseconds()),
			"Delay %d (%v) should be within tolerance of expected %v", i, delay, expected)
	}
}

func TestBackoffStrategy_Interface(t *testing.T) {
	// Test that ExponentialBackoff implements BackoffStrategy interface
	var strategy retry.BackoffStrategy
	strategy = retry.NewExponentialBackoff(100*time.Millisecond, 2.0, 10*time.Second)

	require.NotNil(t, strategy, "Should implement BackoffStrategy interface")

	delay := strategy.CalculateDelay(1)
	assert.Greater(t, delay, time.Duration(0), "Should return positive delay")

	strategy.Reset()
	// Should not panic
}

func TestExponentialBackoff_ThreadSafety(t *testing.T) {
	backoff := retry.NewExponentialBackoff(100*time.Millisecond, 2.0, 10*time.Second)

	// Run multiple goroutines concurrently
	const numGoroutines = 10
	const numCalculations = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			for j := 0; j < numCalculations; j++ {
				delay := backoff.CalculateDelay(j%5 + 1) // Vary attempt numbers

				// Basic sanity check
				assert.Greater(t, delay, time.Duration(0),
					"Goroutine %d, iteration %d: delay should be positive", goroutineID, j)
				assert.LessOrEqual(t, delay, 10*time.Second,
					"Goroutine %d, iteration %d: delay should not exceed max", goroutineID, j)

				if j%20 == 0 {
					backoff.Reset()
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
			// Goroutine completed successfully
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out - potential deadlock or infinite loop")
		}
	}
}

func BenchmarkExponentialBackoff_CalculateDelay(b *testing.B) {
	backoff := retry.NewExponentialBackoff(100*time.Millisecond, 2.0, 10*time.Second)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Vary attempt numbers to test different code paths
		attempt := (i % 10) + 1
		backoff.CalculateDelay(attempt)
	}
}

func BenchmarkExponentialBackoff_WithJitter(b *testing.B) {
	backoff := retry.NewExponentialBackoffWithOptions(retry.BackoffOptions{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
		MaxDelay:     10 * time.Second,
		Jitter:       true,
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		attempt := (i % 10) + 1
		backoff.CalculateDelay(attempt)
	}
}
