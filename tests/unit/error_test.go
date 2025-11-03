package unit

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

func TestErrorCategorizer_CategorizeError(t *testing.T) {
	categorizer := producer.NewErrorCategorizer()

	tests := []struct {
		name             string
		inputError       error
		expectedType     producer.ErrorType
		expectedSeverity producer.ErrorSeverity
		isRetryable      bool
	}{
		// Network errors - usually retryable
		{
			name:             "connection refused",
			inputError:       errors.New("connection refused"),
			expectedType:     producer.ErrorTypeNetwork,
			expectedSeverity: producer.ErrorSeverityMedium,
			isRetryable:      true,
		},
		{
			name:             "timeout error",
			inputError:       errors.New("context deadline exceeded"),
			expectedType:     producer.ErrorTypeTimeout,
			expectedSeverity: producer.ErrorSeverityMedium,
			isRetryable:      true,
		},
		{
			name:             "network unreachable",
			inputError:       errors.New("network is unreachable"),
			expectedType:     producer.ErrorTypeNetwork,
			expectedSeverity: producer.ErrorSeverityHigh,
			isRetryable:      true,
		},

		// Kafka broker errors
		{
			name:             "broker not available",
			inputError:       errors.New("kafka: broker not available"),
			expectedType:     producer.ErrorTypeBroker,
			expectedSeverity: producer.ErrorSeverityHigh,
			isRetryable:      true,
		},
		{
			name:             "leader not available",
			inputError:       errors.New("kafka: leader not available"),
			expectedType:     producer.ErrorTypeBroker,
			expectedSeverity: producer.ErrorSeverityMedium,
			isRetryable:      true,
		},
		{
			name:             "request timed out",
			inputError:       errors.New("kafka: request timed out"),
			expectedType:     producer.ErrorTypeTimeout,
			expectedSeverity: producer.ErrorSeverityMedium,
			isRetryable:      true,
		},

		// Authentication/Authorization errors - not retryable
		{
			name:             "invalid credentials",
			inputError:       errors.New("kafka: invalid credentials"),
			expectedType:     producer.ErrorTypeAuthentication,
			expectedSeverity: producer.ErrorSeverityCritical,
			isRetryable:      false,
		},
		{
			name:             "not authorized",
			inputError:       errors.New("kafka: not authorized"),
			expectedType:     producer.ErrorTypeAuthorization,
			expectedSeverity: producer.ErrorSeverityCritical,
			isRetryable:      false,
		},

		// Message validation errors - not retryable
		{
			name:             "message too large",
			inputError:       errors.New("message size exceeds limit"),
			expectedType:     producer.ErrorTypeValidation,
			expectedSeverity: producer.ErrorSeverityLow,
			isRetryable:      false,
		},
		{
			name:             "invalid topic",
			inputError:       errors.New("topic validation failed"),
			expectedType:     producer.ErrorTypeValidation,
			expectedSeverity: producer.ErrorSeverityLow,
			isRetryable:      false,
		},

		// Configuration errors - not retryable
		{
			name:             "invalid configuration",
			inputError:       errors.New("invalid broker configuration"),
			expectedType:     producer.ErrorTypeConfiguration,
			expectedSeverity: producer.ErrorSeverityCritical,
			isRetryable:      false,
		},

		// Unknown errors - categorized as unknown, medium severity, retryable with caution
		{
			name:             "unknown error",
			inputError:       errors.New("some unknown error occurred"),
			expectedType:     producer.ErrorTypeUnknown,
			expectedSeverity: producer.ErrorSeverityMedium,
			isRetryable:      true,
		},

		// Serialization errors - not retryable
		{
			name:             "serialization failure",
			inputError:       errors.New("failed to serialize message"),
			expectedType:     producer.ErrorTypeSerialization,
			expectedSeverity: producer.ErrorSeverityMedium,
			isRetryable:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorInfo := categorizer.CategorizeError(tt.inputError)

			assert.Equal(t, tt.expectedType, errorInfo.Type, "Error type should match")
			assert.Equal(t, tt.expectedSeverity, errorInfo.Severity, "Error severity should match")
			assert.Equal(t, tt.isRetryable, errorInfo.IsRetryable, "Retryability should match")
			assert.Equal(t, tt.inputError.Error(), errorInfo.OriginalError, "Original error should be preserved")
			assert.NotEmpty(t, errorInfo.Category, "Category should not be empty")
			assert.NotZero(t, errorInfo.CategorizedAt, "CategorizedAt should be set")
		})
	}
}

func TestErrorCategorizer_IsRetryable(t *testing.T) {
	categorizer := producer.NewErrorCategorizer()

	tests := []struct {
		name      string
		errorType producer.ErrorType
		severity  producer.ErrorSeverity
		expected  bool
	}{
		{
			name:      "network error - retryable",
			errorType: producer.ErrorTypeNetwork,
			severity:  producer.ErrorSeverityMedium,
			expected:  true,
		},
		{
			name:      "timeout error - retryable",
			errorType: producer.ErrorTypeTimeout,
			severity:  producer.ErrorSeverityMedium,
			expected:  true,
		},
		{
			name:      "broker error - retryable",
			errorType: producer.ErrorTypeBroker,
			severity:  producer.ErrorSeverityHigh,
			expected:  true,
		},
		{
			name:      "authentication error - not retryable",
			errorType: producer.ErrorTypeAuthentication,
			severity:  producer.ErrorSeverityCritical,
			expected:  false,
		},
		{
			name:      "authorization error - not retryable",
			errorType: producer.ErrorTypeAuthorization,
			severity:  producer.ErrorSeverityCritical,
			expected:  false,
		},
		{
			name:      "validation error - not retryable",
			errorType: producer.ErrorTypeValidation,
			severity:  producer.ErrorSeverityLow,
			expected:  false,
		},
		{
			name:      "configuration error - not retryable",
			errorType: producer.ErrorTypeConfiguration,
			severity:  producer.ErrorSeverityCritical,
			expected:  false,
		},
		{
			name:      "serialization error - not retryable",
			errorType: producer.ErrorTypeSerialization,
			severity:  producer.ErrorSeverityMedium,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := categorizer.IsRetryable(tt.errorType, tt.severity)
			assert.Equal(t, tt.expected, result, "Retryability should match expected value")
		})
	}
}

func TestErrorCategorizer_GetRetryStrategy(t *testing.T) {
	categorizer := producer.NewErrorCategorizer()

	tests := []struct {
		name             string
		errorType        producer.ErrorType
		severity         producer.ErrorSeverity
		expectedStrategy producer.RetryStrategy
	}{
		{
			name:             "network error - immediate retry",
			errorType:        producer.ErrorTypeNetwork,
			severity:         producer.ErrorSeverityMedium,
			expectedStrategy: producer.RetryStrategyImmediate,
		},
		{
			name:             "timeout error - exponential backoff",
			errorType:        producer.ErrorTypeTimeout,
			severity:         producer.ErrorSeverityMedium,
			expectedStrategy: producer.RetryStrategyExponential,
		},
		{
			name:             "broker error high severity - exponential backoff",
			errorType:        producer.ErrorTypeBroker,
			severity:         producer.ErrorSeverityHigh,
			expectedStrategy: producer.RetryStrategyExponential,
		},
		{
			name:             "broker error medium severity - linear backoff",
			errorType:        producer.ErrorTypeBroker,
			severity:         producer.ErrorSeverityMedium,
			expectedStrategy: producer.RetryStrategyLinear,
		},
		{
			name:             "authentication error - no retry",
			errorType:        producer.ErrorTypeAuthentication,
			severity:         producer.ErrorSeverityCritical,
			expectedStrategy: producer.RetryStrategyNone,
		},
		{
			name:             "validation error - no retry",
			errorType:        producer.ErrorTypeValidation,
			severity:         producer.ErrorSeverityLow,
			expectedStrategy: producer.RetryStrategyNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := categorizer.GetRetryStrategy(tt.errorType, tt.severity)
			assert.Equal(t, tt.expectedStrategy, strategy, "Retry strategy should match expected")
		})
	}
}

func TestErrorReport_Creation(t *testing.T) {
	originalError := errors.New("test error message")

	report := models.NewErrorReport(
		"msg-123",
		"test-topic",
		originalError,
		producer.ErrorTypeNetwork,
		producer.ErrorSeverityMedium,
	)

	require.NotNil(t, report, "Error report should not be nil")
	assert.Equal(t, "msg-123", report.MessageID, "Message ID should match")
	assert.Equal(t, "test-topic", report.Topic, "Topic should match")
	assert.Equal(t, originalError.Error(), report.ErrorMessage, "Error message should match")
	assert.Equal(t, producer.ErrorTypeNetwork, report.ErrorType, "Error type should match")
	assert.Equal(t, producer.ErrorSeverityMedium, report.Severity, "Severity should match")
	assert.NotZero(t, report.OccurredAt, "OccurredAt should be set")
	assert.Equal(t, 1, report.AttemptNumber, "Initial attempt number should be 1")
	assert.False(t, report.IsResolved, "Should not be resolved initially")
}

func TestErrorReport_IncrementAttempt(t *testing.T) {
	report := models.NewErrorReport(
		"msg-123",
		"test-topic",
		errors.New("test error"),
		producer.ErrorTypeTimeout,
		producer.ErrorSeverityMedium,
	)

	initialAttempt := report.AttemptNumber
	initialTime := report.LastAttemptAt

	// Wait a small amount to ensure time difference
	time.Sleep(1 * time.Millisecond)

	report.IncrementAttempt()

	assert.Equal(t, initialAttempt+1, report.AttemptNumber, "Attempt number should increment")
	assert.True(t, report.LastAttemptAt.After(initialTime), "LastAttemptAt should be updated")
}

func TestErrorReport_MarkResolved(t *testing.T) {
	report := models.NewErrorReport(
		"msg-123",
		"test-topic",
		errors.New("test error"),
		producer.ErrorTypeNetwork,
		producer.ErrorSeverityMedium,
	)

	assert.False(t, report.IsResolved, "Should not be resolved initially")
	assert.True(t, report.ResolvedAt.IsZero(), "ResolvedAt should be zero initially")

	report.MarkResolved("Successfully retried")

	assert.True(t, report.IsResolved, "Should be marked as resolved")
	assert.NotEmpty(t, report.Resolution, "Resolution should be set")
	assert.Equal(t, "Successfully retried", report.Resolution, "Resolution message should match")
	assert.False(t, report.ResolvedAt.IsZero(), "ResolvedAt should be set")
}

func TestErrorReport_AddRetryAttempt(t *testing.T) {
	report := models.NewErrorReport(
		"msg-123",
		"test-topic",
		errors.New("test error"),
		producer.ErrorTypeTimeout,
		producer.ErrorSeverityMedium,
	)

	// Add first retry attempt
	retryError := errors.New("retry failed")
	report.AddRetryAttempt(retryError, 2*time.Second)

	assert.Equal(t, 2, report.AttemptNumber, "Attempt number should be incremented")
	assert.Len(t, report.RetryHistory, 1, "Should have one retry attempt")

	attempt := report.RetryHistory[0]
	assert.Equal(t, 2, attempt.AttemptNumber, "Retry attempt number should match")
	assert.Equal(t, retryError.Error(), attempt.ErrorMessage, "Retry error message should match")
	assert.Equal(t, 2*time.Second, attempt.Delay, "Retry delay should match")
	assert.NotZero(t, attempt.AttemptedAt, "AttemptedAt should be set")

	// Add second retry attempt
	secondRetryError := errors.New("second retry failed")
	report.AddRetryAttempt(secondRetryError, 4*time.Second)

	assert.Equal(t, 3, report.AttemptNumber, "Attempt number should be incremented again")
	assert.Len(t, report.RetryHistory, 2, "Should have two retry attempts")
}

func TestErrorReport_JSON_Serialization(t *testing.T) {
	report := models.NewErrorReport(
		"msg-456",
		"json-topic",
		errors.New("serialization test error"),
		producer.ErrorTypeSerialization,
		producer.ErrorSeverityHigh,
	)

	// Add some retry history
	report.AddRetryAttempt(errors.New("first retry"), 1*time.Second)
	report.AddRetryAttempt(errors.New("second retry"), 2*time.Second)
	report.MarkResolved("Eventually succeeded")

	// Test JSON serialization
	jsonData, err := report.ToJSON()
	require.NoError(t, err, "Should serialize to JSON successfully")
	assert.NotEmpty(t, jsonData, "JSON data should not be empty")

	// Test that we can unmarshal it back (basic validation)
	var result map[string]interface{}
	err = json.Unmarshal(jsonData, &result)
	require.NoError(t, err, "Should unmarshal JSON successfully")

	assert.Equal(t, "msg-456", result["message_id"], "Message ID should be preserved")
	assert.Equal(t, "json-topic", result["topic"], "Topic should be preserved")
	assert.Equal(t, true, result["is_resolved"], "Resolution status should be preserved")
}

func TestErrorCategorizer_CustomRules(t *testing.T) {
	categorizer := producer.NewErrorCategorizer()

	// Test adding custom categorization rules
	customRule := producer.CategoryRule{
		Pattern:     "custom error pattern",
		ErrorType:   producer.ErrorTypeCustom,
		Severity:    producer.ErrorSeverityLow,
		IsRetryable: false,
	}

	categorizer.AddRule(customRule)

	// Test that custom rule is applied
	customError := errors.New("custom error pattern detected")
	errorInfo := categorizer.CategorizeError(customError)

	assert.Equal(t, producer.ErrorTypeCustom, errorInfo.Type, "Should use custom error type")
	assert.Equal(t, producer.ErrorSeverityLow, errorInfo.Severity, "Should use custom severity")
	assert.False(t, errorInfo.IsRetryable, "Should respect custom retryability")
}

func TestErrorCategorizer_ConcurrentAccess(t *testing.T) {
	categorizer := producer.NewErrorCategorizer()

	// Test concurrent access safety
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 100; j++ {
				testError := fmt.Errorf("concurrent error %d-%d", id, j)
				errorInfo := categorizer.CategorizeError(testError)

				// Basic validation
				assert.NotEmpty(t, errorInfo.Category, "Category should not be empty")
				assert.NotZero(t, errorInfo.CategorizedAt, "CategorizedAt should be set")
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Concurrent access test timed out")
		}
	}
}
