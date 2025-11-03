package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// ErrorType represents the category of error that occurred
type ErrorType string

const (
	ErrorTypeNetwork        ErrorType = "network"
	ErrorTypeTimeout        ErrorType = "timeout"
	ErrorTypeBroker         ErrorType = "broker"
	ErrorTypeAuthentication ErrorType = "authentication"
	ErrorTypeAuthorization  ErrorType = "authorization"
	ErrorTypeValidation     ErrorType = "validation"
	ErrorTypeConfiguration  ErrorType = "configuration"
	ErrorTypeSerialization  ErrorType = "serialization"
	ErrorTypeUnknown        ErrorType = "unknown"
	ErrorTypeCustom         ErrorType = "custom"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity string

const (
	ErrorSeverityLow      ErrorSeverity = "low"
	ErrorSeverityMedium   ErrorSeverity = "medium"
	ErrorSeverityHigh     ErrorSeverity = "high"
	ErrorSeverityCritical ErrorSeverity = "critical"
)

// RetryAttempt represents a single retry attempt for an error
type RetryAttempt struct {
	AttemptNumber int           `json:"attempt_number"`
	AttemptedAt   time.Time     `json:"attempted_at"`
	ErrorMessage  string        `json:"error_message"`
	Delay         time.Duration `json:"delay"`
}

// ErrorReport represents a comprehensive error report for message processing failures
type ErrorReport struct {
	// Message identification
	MessageID string `json:"message_id"`
	Topic     string `json:"topic"`

	// Error details
	ErrorMessage string        `json:"error_message"`
	ErrorType    ErrorType     `json:"error_type"`
	Severity     ErrorSeverity `json:"severity"`

	// Timing information
	OccurredAt    time.Time `json:"occurred_at"`
	LastAttemptAt time.Time `json:"last_attempt_at"`

	// Retry tracking
	AttemptNumber int            `json:"attempt_number"`
	RetryHistory  []RetryAttempt `json:"retry_history,omitempty"`

	// Resolution tracking
	IsResolved bool      `json:"is_resolved"`
	ResolvedAt time.Time `json:"resolved_at,omitempty"`
	Resolution string    `json:"resolution,omitempty"`
}

// NewErrorReport creates a new ErrorReport instance
func NewErrorReport(messageID, topic string, err error, errorType ErrorType, severity ErrorSeverity) *ErrorReport {
	now := time.Now()
	return &ErrorReport{
		MessageID:     messageID,
		Topic:         topic,
		ErrorMessage:  err.Error(),
		ErrorType:     errorType,
		Severity:      severity,
		OccurredAt:    now,
		LastAttemptAt: now,
		AttemptNumber: 1,
		RetryHistory:  make([]RetryAttempt, 0),
		IsResolved:    false,
	}
}

// IncrementAttempt increments the attempt counter and updates timing
func (er *ErrorReport) IncrementAttempt() {
	er.AttemptNumber++
	er.LastAttemptAt = time.Now()
}

// AddRetryAttempt records a retry attempt with its details
func (er *ErrorReport) AddRetryAttempt(retryError error, delay time.Duration) {
	er.IncrementAttempt()

	attempt := RetryAttempt{
		AttemptNumber: er.AttemptNumber,
		AttemptedAt:   time.Now(),
		ErrorMessage:  retryError.Error(),
		Delay:         delay,
	}

	er.RetryHistory = append(er.RetryHistory, attempt)
}

// MarkResolved marks the error as resolved with a resolution message
func (er *ErrorReport) MarkResolved(resolution string) {
	er.IsResolved = true
	er.ResolvedAt = time.Now()
	er.Resolution = resolution
}

// ToJSON serializes the ErrorReport to JSON
func (er *ErrorReport) ToJSON() ([]byte, error) {
	return json.Marshal(er)
}

// FromJSON deserializes JSON data into an ErrorReport
func FromJSON(data []byte) (*ErrorReport, error) {
	var report ErrorReport
	err := json.Unmarshal(data, &report)
	if err != nil {
		return nil, err
	}
	return &report, nil
}

// GetSummary returns a brief summary of the error
func (er *ErrorReport) GetSummary() string {
	status := "unresolved"
	if er.IsResolved {
		status = "resolved"
	}

	return fmt.Sprintf("Message %s on topic %s: %s error (%s severity) - %d attempts, %s",
		er.MessageID, er.Topic, er.ErrorType, er.Severity, er.AttemptNumber, status)
}

// GetDuration returns how long this error has been active
func (er *ErrorReport) GetDuration() time.Duration {
	endTime := er.ResolvedAt
	if endTime.IsZero() {
		endTime = time.Now()
	}
	return endTime.Sub(er.OccurredAt)
}

// ShouldRetry determines if this error should be retried based on severity and attempt count
func (er *ErrorReport) ShouldRetry(maxAttempts int) bool {
	if er.IsResolved {
		return false
	}

	if er.AttemptNumber >= maxAttempts {
		return false
	}

	// Critical errors typically should not be retried
	if er.Severity == ErrorSeverityCritical {
		return false
	}

	// Validation and configuration errors should not be retried
	if er.ErrorType == ErrorTypeValidation ||
		er.ErrorType == ErrorTypeConfiguration ||
		er.ErrorType == ErrorTypeAuthentication ||
		er.ErrorType == ErrorTypeAuthorization {
		return false
	}

	return true
}
