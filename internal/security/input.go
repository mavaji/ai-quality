package security

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Input validation constants
const (
	MaxTopicNameLength    = 255
	MaxPartitionKeyLength = 1024
	MaxMessageSize        = 10 * 1024 * 1024 // 10MB
	MaxHeaderKeyLength    = 256
	MaxHeaderValueLength  = 1024
	MaxHeadersCount       = 50
	MaxMessageIDLength    = 128
)

// Regular expressions for validation
var (
	topicNameRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	messageIDRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
)

// InputValidator provides comprehensive input validation for Kafka messages
type InputValidator struct {
	maxMessageSize     int
	maxTopicLength     int
	maxPartitionLength int
	strictMode         bool
}

// NewInputValidator creates a new input validator
func NewInputValidator(strictMode bool) *InputValidator {
	return &InputValidator{
		maxMessageSize:     MaxMessageSize,
		maxTopicLength:     MaxTopicNameLength,
		maxPartitionLength: MaxPartitionKeyLength,
		strictMode:         strictMode,
	}
}

// ValidationError represents an input validation error
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
	Reason  string
	Hint    string
}

// Error implements the error interface
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error in field '%s': %s", e.Field, e.Message)
}

// ValidateTopicName validates a Kafka topic name
func (v *InputValidator) ValidateTopicName(topic string) error {
	if topic == "" {
		return &ValidationError{
			Field:   "topic",
			Value:   topic,
			Message: "topic name cannot be empty",
		}
	}

	if len(topic) > v.maxTopicLength {
		return &ValidationError{
			Field:   "topic",
			Value:   topic,
			Message: fmt.Sprintf("topic name exceeds maximum length of %d characters", v.maxTopicLength),
		}
	}

	if !topicNameRegex.MatchString(topic) {
		return &ValidationError{
			Field:   "topic",
			Value:   topic,
			Message: "topic name contains invalid characters (allowed: a-z, A-Z, 0-9, ., _, -)",
		}
	}

	// Additional strict mode validations
	if v.strictMode {
		if strings.HasPrefix(topic, "_") || strings.HasPrefix(topic, ".") {
			return &ValidationError{
				Field:   "topic",
				Value:   topic,
				Message: "topic name cannot start with underscore or period in strict mode",
			}
		}
	}

	return nil
}

// ValidatePartitionKey validates a partition key
func (v *InputValidator) ValidatePartitionKey(key string) error {
	if key == "" {
		return nil // Partition key is optional
	}

	if len(key) > v.maxPartitionLength {
		return &ValidationError{
			Field:   "key",
			Value:   key,
			Message: fmt.Sprintf("partition key exceeds maximum length of %d characters", v.maxPartitionLength),
		}
	}

	// Check for potentially dangerous characters in strict mode
	if v.strictMode {
		if strings.ContainsAny(key, "\x00\n\r\t") {
			return &ValidationError{
				Field:   "key",
				Value:   key,
				Message: "partition key contains control characters",
			}
		}
	}

	return nil
}

// ValidateMessagePayload validates a message payload
func (v *InputValidator) ValidateMessagePayload(payload []byte) error {
	if payload == nil {
		return &ValidationError{
			Field:   "value",
			Value:   nil,
			Message: "message payload cannot be nil",
		}
	}

	if len(payload) == 0 {
		return &ValidationError{
			Field:   "value",
			Value:   payload,
			Message: "message payload cannot be empty",
		}
	}

	if len(payload) > v.maxMessageSize {
		return &ValidationError{
			Field:   "value",
			Value:   len(payload),
			Message: fmt.Sprintf("message payload exceeds maximum size of %d bytes", v.maxMessageSize),
		}
	}

	// Validate UTF-8 encoding in strict mode
	if v.strictMode {
		if !utf8.Valid(payload) {
			return &ValidationError{
				Field:   "value",
				Value:   string(payload[:min(50, len(payload))]) + "...",
				Message: "message payload contains invalid UTF-8 sequences",
			}
		}
	}

	return nil
}

// ValidateHeaders validates message headers
func (v *InputValidator) ValidateHeaders(headers map[string]string) error {
	if headers == nil {
		return nil // Headers are optional
	}

	if len(headers) > MaxHeadersCount {
		return &ValidationError{
			Field:   "headers",
			Value:   len(headers),
			Message: fmt.Sprintf("number of headers (%d) exceeds maximum allowed (%d)", len(headers), MaxHeadersCount),
		}
	}

	for key, value := range headers {
		if err := v.validateHeaderKey(key); err != nil {
			return err
		}
		if err := v.validateHeaderValue(value); err != nil {
			return err
		}
	}

	return nil
}

// validateHeaderKey validates a single header key
func (v *InputValidator) validateHeaderKey(key string) error {
	if key == "" {
		return &ValidationError{
			Field:   "headers",
			Value:   key,
			Message: "header key cannot be empty",
		}
	}

	if len(key) > MaxHeaderKeyLength {
		return &ValidationError{
			Field:   "headers",
			Value:   key,
			Message: fmt.Sprintf("header key '%s' exceeds maximum length of %d characters", key, MaxHeaderKeyLength),
		}
	}

	return nil
}

// validateHeaderValue validates a single header value
func (v *InputValidator) validateHeaderValue(value string) error {
	if len(value) > MaxHeaderValueLength {
		return &ValidationError{
			Field:   "headers",
			Value:   value,
			Message: fmt.Sprintf("header value exceeds maximum length of %d characters", MaxHeaderValueLength),
		}
	}

	return nil
}

// ValidateMessageID validates a message ID
func (v *InputValidator) ValidateMessageID(id string) error {
	if id == "" {
		return &ValidationError{
			Field:   "message_id",
			Value:   id,
			Message: "message ID cannot be empty",
		}
	}

	if len(id) > MaxMessageIDLength {
		return &ValidationError{
			Field:   "message_id",
			Value:   id,
			Message: fmt.Sprintf("message ID exceeds maximum length of %d characters", MaxMessageIDLength),
		}
	}

	if !messageIDRegex.MatchString(id) {
		return &ValidationError{
			Field:   "message_id",
			Value:   id,
			Message: "message ID contains invalid characters (allowed: a-z, A-Z, 0-9, ., _, -)",
		}
	}

	return nil
}

// SanitizeInput sanitizes potentially dangerous input strings
func (v *InputValidator) SanitizeInput(input string) string {
	// Remove control characters
	sanitized := strings.Map(func(r rune) rune {
		if r < 32 && r != '\t' && r != '\n' && r != '\r' {
			return -1 // Remove control characters except tab, newline, carriage return
		}
		return r
	}, input)

	// Trim whitespace
	sanitized = strings.TrimSpace(sanitized)

	return sanitized
}

// Helper function for min calculation
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
