package retry

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// BackoffStrategy defines the interface for different backoff strategies
type BackoffStrategy interface {
	CalculateDelay(attempt int) time.Duration
	Reset()
}

// BackoffOptions contains configuration options for exponential backoff
type BackoffOptions struct {
	InitialDelay time.Duration
	Multiplier   float64
	MaxDelay     time.Duration
	Jitter       bool
}

// ExponentialBackoff implements exponential backoff with optional jitter
type ExponentialBackoff struct {
	initialDelay time.Duration
	multiplier   float64
	maxDelay     time.Duration
	jitter       bool
	mutex        sync.RWMutex
	rand         *rand.Rand
}

// NewExponentialBackoff creates a new ExponentialBackoff with default jitter enabled
func NewExponentialBackoff(initialDelay time.Duration, multiplier float64, maxDelay time.Duration) *ExponentialBackoff {
	return NewExponentialBackoffWithOptions(BackoffOptions{
		InitialDelay: initialDelay,
		Multiplier:   multiplier,
		MaxDelay:     maxDelay,
		Jitter:       true, // Default to enabled for better distribution
	})
}

// NewExponentialBackoffWithOptions creates a new ExponentialBackoff with custom options
func NewExponentialBackoffWithOptions(options BackoffOptions) *ExponentialBackoff {
	// Validate inputs
	if options.InitialDelay <= 0 {
		panic("initial delay must be positive")
	}
	if options.Multiplier <= 0 {
		panic("multiplier must be positive")
	}
	if options.MaxDelay <= 0 {
		panic("max delay must be positive")
	}
	if options.MaxDelay < options.InitialDelay {
		panic("max delay must be greater than or equal to initial delay")
	}

	return &ExponentialBackoff{
		initialDelay: options.InitialDelay,
		multiplier:   options.Multiplier,
		maxDelay:     options.MaxDelay,
		jitter:       options.Jitter,
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// CalculateDelay calculates the delay for the given attempt number
func (eb *ExponentialBackoff) CalculateDelay(attempt int) time.Duration {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()

	// Ensure minimum attempt number of 1
	if attempt <= 0 {
		attempt = 1
	}

	// Calculate base delay: initialDelay * multiplier^(attempt-1)
	baseDelay := float64(eb.initialDelay) * math.Pow(eb.multiplier, float64(attempt-1))

	// Apply max delay cap
	if baseDelay > float64(eb.maxDelay) {
		baseDelay = float64(eb.maxDelay)
	}

	delay := time.Duration(baseDelay)

	// Apply jitter if enabled (±10% random variance)
	if eb.jitter {
		jitterRange := float64(delay) * 0.1                         // 10% jitter
		jitterOffset := (eb.rand.Float64() - 0.5) * 2 * jitterRange // -10% to +10%
		jitteredDelay := float64(delay) + jitterOffset

		// Ensure jittered delay is still positive and within reasonable bounds
		if jitteredDelay < 0 {
			jitteredDelay = float64(delay) * 0.1 // Minimum 10% of base delay
		}

		delay = time.Duration(jitteredDelay)
	}

	return delay
}

// Reset resets the backoff strategy to its initial state
func (eb *ExponentialBackoff) Reset() {
	eb.mutex.Lock()
	defer eb.mutex.Unlock()

	// Re-seed the random number generator for better distribution
	eb.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// GetInitialDelay returns the configured initial delay
func (eb *ExponentialBackoff) GetInitialDelay() time.Duration {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()
	return eb.initialDelay
}

// GetMultiplier returns the configured multiplier
func (eb *ExponentialBackoff) GetMultiplier() float64 {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()
	return eb.multiplier
}

// GetMaxDelay returns the configured maximum delay
func (eb *ExponentialBackoff) GetMaxDelay() time.Duration {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()
	return eb.maxDelay
}

// IsJitterEnabled returns whether jitter is enabled
func (eb *ExponentialBackoff) IsJitterEnabled() bool {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()
	return eb.jitter
}

// LinearBackoff implements linear backoff strategy
type LinearBackoff struct {
	initialDelay time.Duration
	increment    time.Duration
	maxDelay     time.Duration
	mutex        sync.RWMutex
}

// NewLinearBackoff creates a new LinearBackoff strategy
func NewLinearBackoff(initialDelay, increment, maxDelay time.Duration) *LinearBackoff {
	if initialDelay <= 0 {
		panic("initial delay must be positive")
	}
	if increment <= 0 {
		panic("increment must be positive")
	}
	if maxDelay <= 0 {
		panic("max delay must be positive")
	}
	if maxDelay < initialDelay {
		panic("max delay must be greater than or equal to initial delay")
	}

	return &LinearBackoff{
		initialDelay: initialDelay,
		increment:    increment,
		maxDelay:     maxDelay,
	}
}

// CalculateDelay calculates the delay for linear backoff
func (lb *LinearBackoff) CalculateDelay(attempt int) time.Duration {
	lb.mutex.RLock()
	defer lb.mutex.RUnlock()

	if attempt <= 0 {
		attempt = 1
	}

	// Linear progression: initialDelay + (attempt-1) * increment
	delay := lb.initialDelay + time.Duration(attempt-1)*lb.increment

	if delay > lb.maxDelay {
		delay = lb.maxDelay
	}

	return delay
}

// Reset resets the linear backoff to initial state
func (lb *LinearBackoff) Reset() {
	// Linear backoff is stateless, nothing to reset
}

// FixedBackoff implements fixed delay strategy
type FixedBackoff struct {
	delay time.Duration
	mutex sync.RWMutex
}

// NewFixedBackoff creates a new FixedBackoff strategy
func NewFixedBackoff(delay time.Duration) *FixedBackoff {
	if delay <= 0 {
		panic("delay must be positive")
	}

	return &FixedBackoff{
		delay: delay,
	}
}

// CalculateDelay returns the fixed delay regardless of attempt number
func (fb *FixedBackoff) CalculateDelay(attempt int) time.Duration {
	fb.mutex.RLock()
	defer fb.mutex.RUnlock()
	return fb.delay
}

// Reset resets the fixed backoff (no-op for fixed strategy)
func (fb *FixedBackoff) Reset() {
	// Fixed backoff is stateless, nothing to reset
}
