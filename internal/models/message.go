package models

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Message represents a Kafka message with all required and optional fields
type Message struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Key       string            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// Constants for message validation
const (
	MaxTopicLength    = 255
	MaxMessageSize    = 1024 * 1024 // 1MB
	MaxHeaderKeyLen   = 256
	MaxHeaderValueLen = 1024
)

// Topic name validation regex (Kafka topic naming rules)
// Allow only alphanumeric, underscores, and hyphens (no dots or special chars)
var topicNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Validate performs comprehensive validation of the message
func (m *Message) Validate() error {
	// Validate topic
	if err := m.validateTopic(); err != nil {
		return fmt.Errorf("topic validation failed: %w", err)
	}

	// Validate value
	if err := m.validateValue(); err != nil {
		return fmt.Errorf("value validation failed: %w", err)
	}

	// Validate timestamp
	if err := m.validateTimestamp(); err != nil {
		return fmt.Errorf("timestamp validation failed: %w", err)
	}

	// Validate headers
	if err := m.validateHeaders(); err != nil {
		return fmt.Errorf("headers validation failed: %w", err)
	}

	// Validate total message size
	if err := m.validateSize(); err != nil {
		return fmt.Errorf("size validation failed: %w", err)
	}

	return nil
}

// validateTopic validates the topic field
func (m *Message) validateTopic() error {
	if m.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	if len(m.Topic) > MaxTopicLength {
		return fmt.Errorf("topic length cannot exceed %d characters", MaxTopicLength)
	}

	// Check for invalid characters
	if !topicNameRegex.MatchString(m.Topic) {
		return fmt.Errorf("topic contains invalid characters (allowed: a-z, A-Z, 0-9, _, -)")
	}

	// Topic cannot start with underscore or period
	if strings.HasPrefix(m.Topic, "_") || strings.HasPrefix(m.Topic, ".") {
		return fmt.Errorf("topic cannot start with underscore or period")
	}

	return nil
}

// validateValue validates the message value
func (m *Message) validateValue() error {
	if m.Value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	if len(m.Value) == 0 {
		return fmt.Errorf("value cannot be empty")
	}

	return nil
}

// validateTimestamp validates the timestamp field
func (m *Message) validateTimestamp() error {
	if m.Timestamp.IsZero() {
		return fmt.Errorf("timestamp cannot be zero")
	}

	// Timestamp should not be too far in the future (more than 1 hour)
	if m.Timestamp.After(time.Now().Add(time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 hour in the future")
	}

	// Timestamp should not be too old (more than 1 year)
	if m.Timestamp.Before(time.Now().Add(-365 * 24 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 year in the past")
	}

	return nil
}

// validateHeaders validates message headers
func (m *Message) validateHeaders() error {
	if m.Headers == nil {
		return nil // Headers are optional
	}

	for key, value := range m.Headers {
		if len(key) > MaxHeaderKeyLen {
			return fmt.Errorf("header key '%s' exceeds maximum length of %d", key, MaxHeaderKeyLen)
		}

		if len(value) > MaxHeaderValueLen {
			return fmt.Errorf("header value for key '%s' exceeds maximum length of %d", key, MaxHeaderValueLen)
		}

		// Header keys cannot be empty
		if key == "" {
			return fmt.Errorf("header keys cannot be empty")
		}
	}

	return nil
}

// validateSize validates the total message size
func (m *Message) validateSize() error {
	totalSize := m.Size()
	if totalSize > MaxMessageSize {
		return fmt.Errorf("message size %d exceeds maximum allowed size of %d bytes", totalSize, MaxMessageSize)
	}

	return nil
}

// Size calculates the total size of the message in bytes
func (m *Message) Size() int {
	size := len(m.Topic) + len(m.Key) + len(m.Value)

	// Add headers size
	for key, value := range m.Headers {
		size += len(key) + len(value)
	}

	// Add overhead for metadata (approximate)
	size += 64 // Kafka message overhead

	return size
}

// GetPartitionKey returns the partition key and whether it exists
func (m *Message) GetPartitionKey() (string, bool) {
	if m.Key == "" {
		return "", false
	}
	return m.Key, true
}

// ToJSON serializes the message to JSON
func (m *Message) ToJSON() ([]byte, error) {
	// Create a JSON-friendly representation
	jsonMessage := struct {
		ID        string            `json:"id"`
		Topic     string            `json:"topic"`
		Key       string            `json:"key,omitempty"`
		Value     string            `json:"value"` // Convert bytes to string for JSON
		Headers   map[string]string `json:"headers,omitempty"`
		Timestamp time.Time         `json:"timestamp"`
	}{
		ID:        m.ID,
		Topic:     m.Topic,
		Key:       m.Key,
		Value:     string(m.Value), // Convert bytes to string
		Headers:   m.Headers,
		Timestamp: m.Timestamp,
	}

	return json.Marshal(jsonMessage)
}

// String returns a string representation of the message
func (m *Message) String() string {
	return fmt.Sprintf("Message{ID:%s, Topic:%s, Key:%s, ValueSize:%d, Headers:%d, Timestamp:%s}",
		m.ID,
		m.Topic,
		m.Key,
		len(m.Value),
		len(m.Headers),
		m.Timestamp.Format(time.RFC3339),
	)
}

// generateID generates a unique ID for the message
func generateID() string {
	bytes := make([]byte, 8) // 16 character hex string
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// NewMessage creates a new message with validation
func NewMessage(topic, key string, value []byte, headers map[string]string) (*Message, error) {
	msg := &Message{
		ID:        generateID(),
		Topic:     topic,
		Key:       key,
		Value:     value,
		Headers:   headers,
		Timestamp: time.Now(),
	}

	if err := msg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to create message: %w", err)
	}

	return msg, nil
}

// Clone creates a deep copy of the message
func (m *Message) Clone() *Message {
	// Clone headers
	headers := make(map[string]string)
	for k, v := range m.Headers {
		headers[k] = v
	}

	// Clone value
	value := make([]byte, len(m.Value))
	copy(value, m.Value)

	return &Message{
		ID:        m.ID,
		Topic:     m.Topic,
		Key:       m.Key,
		Value:     value,
		Headers:   headers,
		Timestamp: m.Timestamp,
	}
}

// SetTimestamp sets a custom timestamp (for testing or special use cases)
func (m *Message) SetTimestamp(timestamp time.Time) {
	m.Timestamp = timestamp
}

// AddHeader adds or updates a header
func (m *Message) AddHeader(key, value string) error {
	if len(key) > MaxHeaderKeyLen {
		return fmt.Errorf("header key exceeds maximum length of %d", MaxHeaderKeyLen)
	}

	if len(value) > MaxHeaderValueLen {
		return fmt.Errorf("header value exceeds maximum length of %d", MaxHeaderValueLen)
	}

	if key == "" {
		return fmt.Errorf("header key cannot be empty")
	}

	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}

	m.Headers[key] = value
	return nil
}

// GetHeader retrieves a header value
func (m *Message) GetHeader(key string) (string, bool) {
	if m.Headers == nil {
		return "", false
	}
	value, exists := m.Headers[key]
	return value, exists
}

// RemoveHeader removes a header
func (m *Message) RemoveHeader(key string) {
	if m.Headers != nil {
		delete(m.Headers, key)
	}
}
