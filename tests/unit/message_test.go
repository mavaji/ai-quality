package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	
	"sdd-kafka-producer/internal/models"
)

func TestMessage_Validate_ValidMessage(t *testing.T) {
	message := &models.Message{
		Topic:   "test-topic",
		Key:     "test-key",
		Value:   []byte("test message content"),
		Headers: map[string]string{
			"content-type": "application/json",
			"source":       "unit-test",
		},
		Timestamp: time.Now(),
	}

	err := message.Validate()
	assert.NoError(t, err, "Valid message should not return validation error")
}

func TestMessage_Validate_EmptyTopic(t *testing.T) {
	message := &models.Message{
		Topic:     "",
		Key:       "test-key",
		Value:     []byte("test content"),
		Timestamp: time.Now(),
	}

	err := message.Validate()
	require.Error(t, err, "Message with empty topic should fail validation")
	assert.Contains(t, err.Error(), "topic", "Error should mention topic field")
}

func TestMessage_Validate_EmptyValue(t *testing.T) {
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     nil,
		Timestamp: time.Now(),
	}

	err := message.Validate()
	require.Error(t, err, "Message with nil value should fail validation")
	assert.Contains(t, err.Error(), "value", "Error should mention value field")
}

func TestMessage_Validate_EmptyByteSliceValue(t *testing.T) {
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     []byte{},
		Timestamp: time.Now(),
	}

	err := message.Validate()
	require.Error(t, err, "Message with empty byte slice should fail validation")
	assert.Contains(t, err.Error(), "value", "Error should mention value field")
}

func TestMessage_Validate_ZeroTimestamp(t *testing.T) {
	message := &models.Message{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test content"),
		// Timestamp is zero value
	}

	err := message.Validate()
	require.Error(t, err, "Message with zero timestamp should fail validation")
	assert.Contains(t, err.Error(), "timestamp", "Error should mention timestamp field")
}

func TestMessage_Validate_InvalidTopicName(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{"topic with spaces", "invalid topic"},
		{"topic with special chars", "invalid/topic"},
		{"topic with dots", "invalid.topic"},
		{"topic starting with underscore", "_invalid"},
		{"topic starting with period", ".invalid"},
		{"very long topic", string(make([]byte, 256))}, // Topic too long
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := &models.Message{
				Topic:     tt.topic,
				Key:       "test-key",
				Value:     []byte("test content"),
				Timestamp: time.Now(),
			}

			err := message.Validate()
			require.Error(t, err, "Invalid topic name should fail validation")
			assert.Contains(t, err.Error(), "topic", "Error should mention topic field")
		})
	}
}

func TestMessage_Validate_OptionalKey(t *testing.T) {
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "", // Empty key should be allowed
		Value:     []byte("test content"),
		Timestamp: time.Now(),
	}

	err := message.Validate()
	assert.NoError(t, err, "Message with empty key should be valid")
}

func TestMessage_Validate_LargeMessage(t *testing.T) {
	// Test message size limit (assuming 1MB limit)
	largeValue := make([]byte, 2*1024*1024) // 2MB
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     largeValue,
		Timestamp: time.Now(),
	}

	err := message.Validate()
	require.Error(t, err, "Message exceeding size limit should fail validation")
	assert.Contains(t, err.Error(), "size", "Error should mention message size")
}

func TestMessage_Size(t *testing.T) {
	message := &models.Message{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("hello world"),
		Headers: map[string]string{
			"type": "test",
		},
	}

	size := message.Size()
	
	// Size should include topic, key, value, and headers
	expectedMinSize := len("test-topic") + len("test-key") + len("hello world") + len("type") + len("test")
	assert.GreaterOrEqual(t, size, expectedMinSize, "Message size calculation should include all fields")
}

func TestMessage_GetPartitionKey(t *testing.T) {
	tests := []struct {
		name           string
		message        *models.Message
		expectedKey    string
		expectedHasKey bool
	}{
		{
			name: "message with key",
			message: &models.Message{
				Topic: "test-topic",
				Key:   "partition-key",
				Value: []byte("content"),
			},
			expectedKey:    "partition-key",
			expectedHasKey: true,
		},
		{
			name: "message without key",
			message: &models.Message{
				Topic: "test-topic",
				Key:   "",
				Value: []byte("content"),
			},
			expectedKey:    "",
			expectedHasKey: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, hasKey := tt.message.GetPartitionKey()
			assert.Equal(t, tt.expectedKey, key, "Partition key should match expected")
			assert.Equal(t, tt.expectedHasKey, hasKey, "HasKey flag should match expected")
		})
	}
}

func TestMessage_ToJSON(t *testing.T) {
	message := &models.Message{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test content"),
		Headers: map[string]string{
			"content-type": "text/plain",
		},
		Timestamp: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	json, err := message.ToJSON()
	require.NoError(t, err, "Valid message should serialize to JSON")
	
	// Check that JSON contains expected fields
	assert.Contains(t, string(json), "test-topic", "JSON should contain topic")
	assert.Contains(t, string(json), "test-key", "JSON should contain key")
	assert.Contains(t, string(json), "content-type", "JSON should contain headers")
}

func TestMessage_String(t *testing.T) {
	message := &models.Message{
		Topic: "test-topic",
		Key:   "test-key",
		Value: []byte("test content"),
	}

	str := message.String()
	assert.Contains(t, str, "test-topic", "String representation should contain topic")
	assert.Contains(t, str, "test-key", "String representation should contain key")
}