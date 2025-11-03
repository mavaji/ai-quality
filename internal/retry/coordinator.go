package retry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"sdd-kafka-producer/internal/models"
)

// RetryStrategy defines different retry strategies
type RetryStrategy string

const (
	RetryStrategyNone        RetryStrategy = "none"
	RetryStrategyImmediate   RetryStrategy = "immediate"
	RetryStrategyFixed       RetryStrategy = "fixed"
	RetryStrategyLinear      RetryStrategy = "linear"
	RetryStrategyExponential RetryStrategy = "exponential"
)

// RetryPolicy defines the retry policy for operations
type RetryPolicy struct {
	MaxAttempts      int
	Strategy         RetryStrategy
	BackoffStrategy  BackoffStrategy
	RetryableErrors  []models.ErrorType
	StopOnErrorTypes []models.ErrorType
}

// RetryResult represents the result of a retry operation
type RetryResult struct {
	Success      bool
	FinalError   error
	AttemptCount int
	TotalDelay   time.Duration
	ErrorReport  *models.ErrorReport
}

// RetryableOperation defines a function that can be retried
type RetryableOperation func(ctx context.Context, attempt int) error

// Coordinator manages retry operations with different strategies
type Coordinator struct {
	policy           RetryPolicy
	logger           *zap.Logger
	mutex            sync.RWMutex
	activeRetries    map[string]*models.ErrorReport
	retryMetrics     *RetryMetrics
	errorCategorizer ErrorCategorizer
}

// RetryMetrics tracks retry statistics
type RetryMetrics struct {
	TotalRetries      int64
	SuccessfulRetries int64
	FailedRetries     int64
	AverageAttempts   float64
	AverageDelay      time.Duration
	mutex             sync.RWMutex
}

// ErrorCategorizer interface for categorizing errors
type ErrorCategorizer interface {
	CategorizeError(err error) ErrorInfo
	IsRetryable(errorType models.ErrorType, severity models.ErrorSeverity) bool
	GetRetryStrategy(errorType models.ErrorType, severity models.ErrorSeverity) RetryStrategy
}

// ErrorInfo contains categorized error information
type ErrorInfo struct {
	Type          models.ErrorType
	Severity      models.ErrorSeverity
	IsRetryable   bool
	Category      string
	OriginalError string
	CategorizedAt time.Time
}

// CoordinatorConfig contains configuration for the retry coordinator
type CoordinatorConfig struct {
	MaxAttempts      int
	DefaultStrategy  RetryStrategy
	BackoffOptions   BackoffOptions
	RetryableErrors  []models.ErrorType
	StopOnErrorTypes []models.ErrorType
}

// NewCoordinator creates a new retry coordinator
func NewCoordinator(config CoordinatorConfig, logger *zap.Logger, categorizer ErrorCategorizer) *Coordinator {
	var backoffStrategy BackoffStrategy

	switch config.DefaultStrategy {
	case RetryStrategyExponential:
		backoffStrategy = NewExponentialBackoffWithOptions(config.BackoffOptions)
	case RetryStrategyLinear:
		backoffStrategy = NewLinearBackoff(
			config.BackoffOptions.InitialDelay,
			config.BackoffOptions.InitialDelay, // Use initial delay as increment
			config.BackoffOptions.MaxDelay,
		)
	case RetryStrategyFixed:
		backoffStrategy = NewFixedBackoff(config.BackoffOptions.InitialDelay)
	default:
		backoffStrategy = NewExponentialBackoffWithOptions(config.BackoffOptions)
	}

	policy := RetryPolicy{
		MaxAttempts:      config.MaxAttempts,
		Strategy:         config.DefaultStrategy,
		BackoffStrategy:  backoffStrategy,
		RetryableErrors:  config.RetryableErrors,
		StopOnErrorTypes: config.StopOnErrorTypes,
	}

	return &Coordinator{
		policy:           policy,
		logger:           logger,
		activeRetries:    make(map[string]*models.ErrorReport),
		retryMetrics:     &RetryMetrics{},
		errorCategorizer: categorizer,
	}
}

// ExecuteWithRetry executes an operation with retry logic
func (c *Coordinator) ExecuteWithRetry(
	ctx context.Context,
	operationID string,
	messageID string,
	topic string,
	operation RetryableOperation,
) *RetryResult {
	c.logger.Info("Starting retry operation",
		zap.String("operation_id", operationID),
		zap.String("message_id", messageID),
		zap.String("topic", topic),
		zap.Int("max_attempts", c.policy.MaxAttempts))

	var lastError error
	var errorReport *models.ErrorReport
	totalDelay := time.Duration(0)

	for attempt := 1; attempt <= c.policy.MaxAttempts; attempt++ {
		// Check context cancellation
		if ctx.Err() != nil {
			c.logger.Warn("Operation cancelled by context",
				zap.String("operation_id", operationID),
				zap.Int("attempt", attempt),
				zap.Error(ctx.Err()))

			return &RetryResult{
				Success:      false,
				FinalError:   ctx.Err(),
				AttemptCount: attempt - 1,
				TotalDelay:   totalDelay,
				ErrorReport:  errorReport,
			}
		}

		// Execute the operation
		operationStart := time.Now()
		err := operation(ctx, attempt)
		operationDuration := time.Since(operationStart)

		if err == nil {
			// Success!
			c.logger.Info("Operation succeeded",
				zap.String("operation_id", operationID),
				zap.Int("attempt", attempt),
				zap.Duration("duration", operationDuration),
				zap.Duration("total_delay", totalDelay))

			// Mark error report as resolved if we have one
			if errorReport != nil {
				errorReport.MarkResolved(fmt.Sprintf("Succeeded on attempt %d", attempt))
				c.removeActiveRetry(operationID)
			}

			c.recordSuccess(attempt, totalDelay)

			return &RetryResult{
				Success:      true,
				FinalError:   nil,
				AttemptCount: attempt,
				TotalDelay:   totalDelay,
				ErrorReport:  errorReport,
			}
		}

		// Operation failed, categorize the error
		lastError = err
		errorInfo := c.errorCategorizer.CategorizeError(err)

		c.logger.Warn("Operation failed",
			zap.String("operation_id", operationID),
			zap.Int("attempt", attempt),
			zap.Error(err),
			zap.String("error_type", string(errorInfo.Type)),
			zap.String("error_severity", string(errorInfo.Severity)),
			zap.Bool("retryable", errorInfo.IsRetryable))

		// Create or update error report
		if errorReport == nil {
			errorReport = models.NewErrorReport(messageID, topic, err, errorInfo.Type, errorInfo.Severity)
			c.trackActiveRetry(operationID, errorReport)
		} else {
			// Add retry attempt to existing report
			delay := c.calculateDelay(attempt-1, errorInfo)
			errorReport.AddRetryAttempt(err, delay)
		}

		// Check if we should stop retrying
		if !c.shouldRetry(errorInfo, attempt) {
			c.logger.Warn("Stopping retries",
				zap.String("operation_id", operationID),
				zap.Int("final_attempt", attempt),
				zap.String("reason", c.getStopReason(errorInfo, attempt)))

			c.recordFailure(attempt, totalDelay)
			c.removeActiveRetry(operationID)

			return &RetryResult{
				Success:      false,
				FinalError:   err,
				AttemptCount: attempt,
				TotalDelay:   totalDelay,
				ErrorReport:  errorReport,
			}
		}

		// Calculate delay for next attempt
		if attempt < c.policy.MaxAttempts {
			delay := c.calculateDelay(attempt, errorInfo)
			totalDelay += delay

			c.logger.Info("Waiting before retry",
				zap.String("operation_id", operationID),
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay),
				zap.Int("next_attempt", attempt+1))

			// Wait for the calculated delay
			select {
			case <-ctx.Done():
				c.logger.Warn("Context cancelled during retry delay",
					zap.String("operation_id", operationID))

				c.removeActiveRetry(operationID)
				return &RetryResult{
					Success:      false,
					FinalError:   ctx.Err(),
					AttemptCount: attempt,
					TotalDelay:   totalDelay,
					ErrorReport:  errorReport,
				}
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	// All attempts exhausted
	c.logger.Error("All retry attempts exhausted",
		zap.String("operation_id", operationID),
		zap.Int("total_attempts", c.policy.MaxAttempts),
		zap.Duration("total_delay", totalDelay),
		zap.Error(lastError))

	c.recordFailure(c.policy.MaxAttempts, totalDelay)
	c.removeActiveRetry(operationID)

	return &RetryResult{
		Success:      false,
		FinalError:   lastError,
		AttemptCount: c.policy.MaxAttempts,
		TotalDelay:   totalDelay,
		ErrorReport:  errorReport,
	}
}

// shouldRetry determines if an operation should be retried
func (c *Coordinator) shouldRetry(errorInfo ErrorInfo, attempt int) bool {
	// Check if we've reached max attempts
	if attempt >= c.policy.MaxAttempts {
		return false
	}

	// Check if error type should stop retries
	for _, stopType := range c.policy.StopOnErrorTypes {
		if errorInfo.Type == stopType {
			return false
		}
	}

	// Use error categorizer to determine if retryable
	return c.errorCategorizer.IsRetryable(errorInfo.Type, errorInfo.Severity)
}

// calculateDelay calculates the delay for the next retry attempt
func (c *Coordinator) calculateDelay(attempt int, errorInfo ErrorInfo) time.Duration {
	// Get strategy from error categorizer if available
	strategy := c.errorCategorizer.GetRetryStrategy(errorInfo.Type, errorInfo.Severity)

	// Fall back to default strategy if needed
	if strategy == RetryStrategyNone {
		strategy = c.policy.Strategy
	}

	switch strategy {
	case RetryStrategyImmediate:
		return 0
	case RetryStrategyNone:
		return 0
	default:
		return c.policy.BackoffStrategy.CalculateDelay(attempt + 1)
	}
}

// getStopReason returns a human-readable reason for stopping retries
func (c *Coordinator) getStopReason(errorInfo ErrorInfo, attempt int) string {
	if attempt >= c.policy.MaxAttempts {
		return "maximum attempts reached"
	}

	for _, stopType := range c.policy.StopOnErrorTypes {
		if errorInfo.Type == stopType {
			return fmt.Sprintf("error type %s configured to stop retries", stopType)
		}
	}

	if !c.errorCategorizer.IsRetryable(errorInfo.Type, errorInfo.Severity) {
		return fmt.Sprintf("error type %s with severity %s is not retryable", errorInfo.Type, errorInfo.Severity)
	}

	return "unknown reason"
}

// trackActiveRetry tracks an active retry operation
func (c *Coordinator) trackActiveRetry(operationID string, report *models.ErrorReport) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.activeRetries[operationID] = report
}

// removeActiveRetry removes an active retry operation
func (c *Coordinator) removeActiveRetry(operationID string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.activeRetries, operationID)
}

// GetActiveRetries returns a snapshot of currently active retry operations
func (c *Coordinator) GetActiveRetries() map[string]*models.ErrorReport {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make(map[string]*models.ErrorReport)
	for k, v := range c.activeRetries {
		result[k] = v
	}
	return result
}

// recordSuccess records metrics for a successful retry operation
func (c *Coordinator) recordSuccess(attempts int, totalDelay time.Duration) {
	c.retryMetrics.mutex.Lock()
	defer c.retryMetrics.mutex.Unlock()

	c.retryMetrics.TotalRetries++
	c.retryMetrics.SuccessfulRetries++

	// Update rolling average
	c.retryMetrics.AverageAttempts = (c.retryMetrics.AverageAttempts*float64(c.retryMetrics.TotalRetries-1) + float64(attempts)) / float64(c.retryMetrics.TotalRetries)
	c.retryMetrics.AverageDelay = time.Duration((int64(c.retryMetrics.AverageDelay)*int64(c.retryMetrics.TotalRetries-1) + int64(totalDelay)) / int64(c.retryMetrics.TotalRetries))
}

// recordFailure records metrics for a failed retry operation
func (c *Coordinator) recordFailure(attempts int, totalDelay time.Duration) {
	c.retryMetrics.mutex.Lock()
	defer c.retryMetrics.mutex.Unlock()

	c.retryMetrics.TotalRetries++
	c.retryMetrics.FailedRetries++

	// Update rolling average
	c.retryMetrics.AverageAttempts = (c.retryMetrics.AverageAttempts*float64(c.retryMetrics.TotalRetries-1) + float64(attempts)) / float64(c.retryMetrics.TotalRetries)
	c.retryMetrics.AverageDelay = time.Duration((int64(c.retryMetrics.AverageDelay)*int64(c.retryMetrics.TotalRetries-1) + int64(totalDelay)) / int64(c.retryMetrics.TotalRetries))
}

// GetMetrics returns current retry metrics
func (c *Coordinator) GetMetrics() RetryMetrics {
	c.retryMetrics.mutex.RLock()
	defer c.retryMetrics.mutex.RUnlock()
	return *c.retryMetrics
}

// UpdatePolicy updates the retry policy
func (c *Coordinator) UpdatePolicy(policy RetryPolicy) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.policy = policy
}

// Reset resets the coordinator state
func (c *Coordinator) Reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.activeRetries = make(map[string]*models.ErrorReport)
	c.retryMetrics = &RetryMetrics{}
	c.policy.BackoffStrategy.Reset()
}
