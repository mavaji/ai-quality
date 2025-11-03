package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"sdd-kafka-producer/internal/models"
)

// CircuitState represents the state of a circuit breaker
type CircuitState string

const (
	CircuitStateClosed   CircuitState = "closed"    // Normal operation
	CircuitStateOpen     CircuitState = "open"      // Blocking requests due to failures
	CircuitStateHalfOpen CircuitState = "half_open" // Testing if service has recovered
)

// CircuitBreakerConfig contains configuration for the circuit breaker
type CircuitBreakerConfig struct {
	// Failure threshold - number of failures before opening circuit
	FailureThreshold int

	// Success threshold - number of successes in half-open state before closing
	SuccessThreshold int

	// Timeout after which to try half-open from open state
	RecoveryTimeout time.Duration

	// Window duration for counting failures
	WindowDuration time.Duration

	// Maximum number of concurrent requests in half-open state
	MaxConcurrentRequests int
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config           CircuitBreakerConfig
	state            CircuitState
	failureCount     int
	successCount     int
	lastFailureTime  time.Time
	lastSuccessTime  time.Time
	halfOpenRequests int
	mutex            sync.RWMutex
	logger           *zap.Logger

	// Metrics
	totalRequests    int64
	totalFailures    int64
	totalSuccesses   int64
	totalRejected    int64
	circuitOpenCount int64
	lastStateChange  time.Time
}

// CircuitBreakerMetrics contains metrics about circuit breaker operation
type CircuitBreakerMetrics struct {
	State                CircuitState  `json:"state"`
	FailureCount         int           `json:"failure_count"`
	SuccessCount         int           `json:"success_count"`
	TotalRequests        int64         `json:"total_requests"`
	TotalFailures        int64         `json:"total_failures"`
	TotalSuccesses       int64         `json:"total_successes"`
	TotalRejected        int64         `json:"total_rejected"`
	CircuitOpenCount     int64         `json:"circuit_open_count"`
	LastStateChange      time.Time     `json:"last_state_change"`
	TimeSinceLastFailure time.Duration `json:"time_since_last_failure"`
	TimeSinceLastSuccess time.Duration `json:"time_since_last_success"`
}

// CircuitBreakerError represents an error when circuit is open
type CircuitBreakerError struct {
	State        CircuitState
	FailureCount int
	Message      string
}

func (e *CircuitBreakerError) Error() string {
	return fmt.Sprintf("circuit breaker is %s: %s (failures: %d)", e.State, e.Message, e.FailureCount)
}

// Operation represents a function that can be executed through the circuit breaker
type Operation func(ctx context.Context) error

// NewCircuitBreaker creates a new circuit breaker with the given configuration
func NewCircuitBreaker(config CircuitBreakerConfig, logger *zap.Logger) *CircuitBreaker {
	// Set defaults if not provided
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 2
	}
	if config.RecoveryTimeout <= 0 {
		config.RecoveryTimeout = 30 * time.Second
	}
	if config.WindowDuration <= 0 {
		config.WindowDuration = 60 * time.Second
	}
	if config.MaxConcurrentRequests <= 0 {
		config.MaxConcurrentRequests = 1
	}

	return &CircuitBreaker{
		config:          config,
		state:           CircuitStateClosed,
		logger:          logger,
		lastStateChange: time.Now(),
	}
}

// Execute executes an operation through the circuit breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, operation Operation) error {
	// Check if we can proceed
	if !cb.canExecute() {
		cb.recordRejection()
		return &CircuitBreakerError{
			State:        cb.getState(),
			FailureCount: cb.getFailureCount(),
			Message:      "circuit breaker is open, rejecting request",
		}
	}

	// If in half-open state, limit concurrent requests
	if cb.getState() == CircuitStateHalfOpen {
		cb.incrementHalfOpenRequests()
		defer cb.decrementHalfOpenRequests()
	}

	// Execute the operation
	cb.recordRequest()
	err := operation(ctx)

	if err != nil {
		cb.recordFailure(err)
		return err
	}

	cb.recordSuccess()
	return nil
}

// canExecute checks if the circuit breaker allows execution
func (cb *CircuitBreaker) canExecute() bool {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	switch cb.state {
	case CircuitStateClosed:
		return true

	case CircuitStateOpen:
		// Check if we should transition to half-open
		if time.Since(cb.lastFailureTime) >= cb.config.RecoveryTimeout {
			cb.transitionToHalfOpen()
			return true
		}
		return false

	case CircuitStateHalfOpen:
		// Allow limited concurrent requests
		return cb.halfOpenRequests < cb.config.MaxConcurrentRequests

	default:
		return false
	}
}

// recordRequest records that a request was made
func (cb *CircuitBreaker) recordRequest() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.totalRequests++
}

// recordSuccess records a successful operation
func (cb *CircuitBreaker) recordSuccess() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.totalSuccesses++
	cb.lastSuccessTime = time.Now()

	switch cb.state {
	case CircuitStateClosed:
		// Reset failure count on success
		cb.failureCount = 0

	case CircuitStateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.config.SuccessThreshold {
			cb.transitionToClosed()
		}
	}

	cb.logger.Debug("Circuit breaker recorded success",
		zap.String("state", string(cb.state)),
		zap.Int("success_count", cb.successCount),
		zap.Int("failure_count", cb.failureCount))
}

// recordFailure records a failed operation
func (cb *CircuitBreaker) recordFailure(err error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.totalFailures++
	cb.lastFailureTime = time.Now()

	// Only count failures within the time window
	if time.Since(cb.lastFailureTime) <= cb.config.WindowDuration {
		cb.failureCount++
	} else {
		// Reset failure count if outside window
		cb.failureCount = 1
	}

	switch cb.state {
	case CircuitStateClosed:
		if cb.failureCount >= cb.config.FailureThreshold {
			cb.transitionToOpen()
		}

	case CircuitStateHalfOpen:
		// Any failure in half-open state transitions back to open
		cb.transitionToOpen()
	}

	cb.logger.Warn("Circuit breaker recorded failure",
		zap.String("state", string(cb.state)),
		zap.Int("failure_count", cb.failureCount),
		zap.Int("threshold", cb.config.FailureThreshold),
		zap.Error(err))
}

// recordRejection records that a request was rejected
func (cb *CircuitBreaker) recordRejection() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.totalRejected++
}

// transitionToClosed transitions the circuit breaker to closed state
func (cb *CircuitBreaker) transitionToClosed() {
	if cb.state != CircuitStateClosed {
		cb.logger.Info("Circuit breaker transitioning to CLOSED",
			zap.String("from_state", string(cb.state)),
			zap.Int("success_count", cb.successCount))

		cb.state = CircuitStateClosed
		cb.failureCount = 0
		cb.successCount = 0
		cb.halfOpenRequests = 0
		cb.lastStateChange = time.Now()
	}
}

// transitionToOpen transitions the circuit breaker to open state
func (cb *CircuitBreaker) transitionToOpen() {
	if cb.state != CircuitStateOpen {
		cb.logger.Warn("Circuit breaker transitioning to OPEN",
			zap.String("from_state", string(cb.state)),
			zap.Int("failure_count", cb.failureCount),
			zap.Int("threshold", cb.config.FailureThreshold))

		cb.state = CircuitStateOpen
		cb.successCount = 0
		cb.halfOpenRequests = 0
		cb.circuitOpenCount++
		cb.lastStateChange = time.Now()
	}
}

// transitionToHalfOpen transitions the circuit breaker to half-open state
func (cb *CircuitBreaker) transitionToHalfOpen() {
	if cb.state != CircuitStateHalfOpen {
		cb.logger.Info("Circuit breaker transitioning to HALF_OPEN",
			zap.String("from_state", string(cb.state)),
			zap.Duration("recovery_timeout", cb.config.RecoveryTimeout))

		cb.state = CircuitStateHalfOpen
		cb.successCount = 0
		cb.halfOpenRequests = 0
		cb.lastStateChange = time.Now()
	}
}

// incrementHalfOpenRequests increments the count of half-open requests
func (cb *CircuitBreaker) incrementHalfOpenRequests() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.halfOpenRequests++
}

// decrementHalfOpenRequests decrements the count of half-open requests
func (cb *CircuitBreaker) decrementHalfOpenRequests() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	if cb.halfOpenRequests > 0 {
		cb.halfOpenRequests--
	}
}

// getState returns the current state of the circuit breaker
func (cb *CircuitBreaker) getState() CircuitState {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// getFailureCount returns the current failure count
func (cb *CircuitBreaker) getFailureCount() int {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.failureCount
}

// GetMetrics returns current metrics for the circuit breaker
func (cb *CircuitBreaker) GetMetrics() CircuitBreakerMetrics {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	var timeSinceLastFailure time.Duration
	var timeSinceLastSuccess time.Duration

	if !cb.lastFailureTime.IsZero() {
		timeSinceLastFailure = time.Since(cb.lastFailureTime)
	}

	if !cb.lastSuccessTime.IsZero() {
		timeSinceLastSuccess = time.Since(cb.lastSuccessTime)
	}

	return CircuitBreakerMetrics{
		State:                cb.state,
		FailureCount:         cb.failureCount,
		SuccessCount:         cb.successCount,
		TotalRequests:        cb.totalRequests,
		TotalFailures:        cb.totalFailures,
		TotalSuccesses:       cb.totalSuccesses,
		TotalRejected:        cb.totalRejected,
		CircuitOpenCount:     cb.circuitOpenCount,
		LastStateChange:      cb.lastStateChange,
		TimeSinceLastFailure: timeSinceLastFailure,
		TimeSinceLastSuccess: timeSinceLastSuccess,
	}
}

// IsOpen returns true if the circuit breaker is in open state
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.getState() == CircuitStateOpen
}

// IsClosed returns true if the circuit breaker is in closed state
func (cb *CircuitBreaker) IsClosed() bool {
	return cb.getState() == CircuitStateClosed
}

// IsHalfOpen returns true if the circuit breaker is in half-open state
func (cb *CircuitBreaker) IsHalfOpen() bool {
	return cb.getState() == CircuitStateHalfOpen
}

// Reset resets the circuit breaker to closed state with cleared counters
func (cb *CircuitBreaker) Reset() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.logger.Info("Resetting circuit breaker",
		zap.String("current_state", string(cb.state)))

	cb.state = CircuitStateClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfOpenRequests = 0
	cb.lastStateChange = time.Now()
	cb.totalRequests = 0
	cb.totalFailures = 0
	cb.totalSuccesses = 0
	cb.totalRejected = 0
	cb.circuitOpenCount = 0
	cb.lastFailureTime = time.Time{}
	cb.lastSuccessTime = time.Time{}
}

// ForceOpen forces the circuit breaker into open state
func (cb *CircuitBreaker) ForceOpen() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.logger.Warn("Forcing circuit breaker to OPEN state",
		zap.String("current_state", string(cb.state)))

	cb.transitionToOpen()
}

// ForceClose forces the circuit breaker into closed state
func (cb *CircuitBreaker) ForceClose() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.logger.Info("Forcing circuit breaker to CLOSED state",
		zap.String("current_state", string(cb.state)))

	cb.transitionToClosed()
}

// GetConfig returns the current configuration
func (cb *CircuitBreaker) GetConfig() CircuitBreakerConfig {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.config
}

// UpdateConfig updates the circuit breaker configuration
func (cb *CircuitBreaker) UpdateConfig(config CircuitBreakerConfig) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.logger.Info("Updating circuit breaker configuration",
		zap.Int("old_failure_threshold", cb.config.FailureThreshold),
		zap.Int("new_failure_threshold", config.FailureThreshold),
		zap.Duration("old_recovery_timeout", cb.config.RecoveryTimeout),
		zap.Duration("new_recovery_timeout", config.RecoveryTimeout))

	cb.config = config
}
