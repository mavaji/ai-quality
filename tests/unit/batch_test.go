package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sdd-kafka-producer/internal/models"
)

func TestBatch_Creation(t *testing.T) {
	messages := []*models.Message{
		{
			ID:        "msg-1",
			Topic:     "test-topic",
			Key:       "key-1",
			Value:     []byte("message 1"),
			Timestamp: time.Now(),
		},
		{
			ID:        "msg-2",
			Topic:     "test-topic",
			Key:       "key-2",
			Value:     []byte("message 2"),
			Timestamp: time.Now(),
		},
	}

	batch := models.NewBatch("batch-1", "test-topic", messages)

	require.NotNil(t, batch, "Batch should not be nil")
	assert.Equal(t, "batch-1", batch.ID, "Batch ID should match")
	assert.Equal(t, "test-topic", batch.Topic, "Batch topic should match")
	assert.Len(t, batch.Messages, 2, "Should have 2 messages")
	assert.Equal(t, 2, batch.MessageCount(), "Message count should be 2")
	assert.Greater(t, batch.TotalBytes, 0, "Total bytes should be calculated")
	assert.NotZero(t, batch.CreatedAt, "CreatedAt should be set")
}

func TestBatch_AddMessage(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)

	message1 := &models.Message{
		ID:        "msg-1",
		Topic:     "test-topic",
		Value:     []byte("message 1"),
		Timestamp: time.Now(),
	}

	message2 := &models.Message{
		ID:        "msg-2",
		Topic:     "test-topic",
		Value:     []byte("message 2"),
		Timestamp: time.Now(),
	}

	err := batch.AddMessage(message1)
	assert.NoError(t, err, "Should add message successfully")
	assert.Equal(t, 1, batch.MessageCount(), "Should have 1 message")

	err = batch.AddMessage(message2)
	assert.NoError(t, err, "Should add second message successfully")
	assert.Equal(t, 2, batch.MessageCount(), "Should have 2 messages")
}

func TestBatch_AddMessage_DifferentTopic(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)

	message := &models.Message{
		ID:        "msg-1",
		Topic:     "different-topic",
		Value:     []byte("message 1"),
		Timestamp: time.Now(),
	}

	err := batch.AddMessage(message)
	assert.Error(t, err, "Should not add message with different topic")
	assert.Contains(t, err.Error(), "topic mismatch", "Error should mention topic mismatch")
	assert.Equal(t, 0, batch.MessageCount(), "Should have 0 messages")
}

func TestBatch_IsFull_ByCount(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)
	batch.MaxMessages = 2
	batch.MaxBytes = 1024

	message1 := &models.Message{
		ID:        "msg-1",
		Topic:     "test-topic",
		Value:     []byte("message 1"),
		Timestamp: time.Now(),
	}

	message2 := &models.Message{
		ID:        "msg-2",
		Topic:     "test-topic",
		Value:     []byte("message 2"),
		Timestamp: time.Now(),
	}

	assert.False(t, batch.IsFull(), "Empty batch should not be full")

	batch.AddMessage(message1)
	assert.False(t, batch.IsFull(), "Batch with 1 message should not be full")

	batch.AddMessage(message2)
	assert.True(t, batch.IsFull(), "Batch with 2 messages should be full")
}

func TestBatch_IsFull_BySize(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)
	batch.MaxMessages = 10
	batch.MaxBytes = 20 // Very small size limit

	message := &models.Message{
		ID:        "msg-1",
		Topic:     "test-topic",
		Value:     []byte("this is a long message that exceeds the size limit"),
		Timestamp: time.Now(),
	}

	batch.AddMessage(message)
	assert.True(t, batch.IsFull(), "Batch should be full due to size limit")
}

func TestBatch_CanAccept(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)
	batch.MaxMessages = 2
	batch.MaxBytes = 100

	smallMessage := &models.Message{
		ID:        "msg-1",
		Topic:     "test-topic",
		Value:     []byte("small"),
		Timestamp: time.Now(),
	}

	largeMessage := &models.Message{
		ID:        "msg-2",
		Topic:     "test-topic",
		Value:     []byte("this is a very long message that will exceed the batch size limit when added"),
		Timestamp: time.Now(),
	}

	wrongTopicMessage := &models.Message{
		ID:        "msg-3",
		Topic:     "wrong-topic",
		Value:     []byte("message"),
		Timestamp: time.Now(),
	}

	assert.True(t, batch.CanAccept(smallMessage), "Should accept small message")
	assert.False(t, batch.CanAccept(wrongTopicMessage), "Should not accept wrong topic")

	batch.AddMessage(smallMessage)
	assert.True(t, batch.CanAccept(smallMessage), "Should still accept another small message")
	assert.False(t, batch.CanAccept(largeMessage), "Should not accept large message that would exceed size")
}

func TestBatch_MarkSent(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)

	assert.True(t, batch.SentAt.IsZero(), "SentAt should be zero initially")

	batch.MarkSent()

	assert.False(t, batch.SentAt.IsZero(), "SentAt should be set after marking sent")
	assert.True(t, batch.SentAt.After(batch.CreatedAt), "SentAt should be after CreatedAt")
}

func TestBatch_MarkCompleted(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)

	assert.True(t, batch.CompletedAt.IsZero(), "CompletedAt should be zero initially")

	batch.MarkCompleted()

	assert.False(t, batch.CompletedAt.IsZero(), "CompletedAt should be set after marking completed")
	assert.True(t, batch.CompletedAt.After(batch.CreatedAt), "CompletedAt should be after CreatedAt")
}

func TestBatch_ProcessingDuration(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)

	// Should return zero duration if not completed
	duration := batch.ProcessingDuration()
	assert.Equal(t, time.Duration(0), duration, "Duration should be 0 if not completed")

	// Wait a bit and mark completed
	time.Sleep(1 * time.Millisecond)
	batch.MarkCompleted()

	duration = batch.ProcessingDuration()
	assert.Greater(t, duration, time.Duration(0), "Duration should be positive after completion")
}

func TestBatch_AverageMessageSize(t *testing.T) {
	messages := []*models.Message{
		{
			ID:        "msg-1",
			Topic:     "test-topic",
			Value:     []byte("12345"), // 5 bytes
			Timestamp: time.Now(),
		},
		{
			ID:        "msg-2",
			Topic:     "test-topic",
			Value:     []byte("1234567890"), // 10 bytes
			Timestamp: time.Now(),
		},
	}

	batch := models.NewBatch("batch-1", "test-topic", messages)

	avgSize := batch.AverageMessageSize()
	expectedAvg := (5 + 10) / 2
	assert.Equal(t, expectedAvg, avgSize, "Average message size should be 7.5 bytes")
}

func TestBatch_AverageMessageSize_EmptyBatch(t *testing.T) {
	batch := models.NewBatch("batch-1", "test-topic", nil)

	avgSize := batch.AverageMessageSize()
	assert.Equal(t, 0, avgSize, "Average size should be 0 for empty batch")
}

func TestBatch_GetMessages(t *testing.T) {
	messages := []*models.Message{
		{
			ID:        "msg-1",
			Topic:     "test-topic",
			Value:     []byte("message 1"),
			Timestamp: time.Now(),
		},
		{
			ID:        "msg-2",
			Topic:     "test-topic",
			Value:     []byte("message 2"),
			Timestamp: time.Now(),
		},
	}

	batch := models.NewBatch("batch-1", "test-topic", messages)

	retrievedMessages := batch.GetMessages()
	assert.Len(t, retrievedMessages, 2, "Should return all messages")
	assert.Equal(t, "msg-1", retrievedMessages[0].ID, "First message should match")
	assert.Equal(t, "msg-2", retrievedMessages[1].ID, "Second message should match")
}

func TestBatch_Reset(t *testing.T) {
	messages := []*models.Message{
		{
			ID:        "msg-1",
			Topic:     "test-topic",
			Value:     []byte("message 1"),
			Timestamp: time.Now(),
		},
	}

	batch := models.NewBatch("batch-1", "test-topic", messages)
	batch.MarkSent()
	batch.MarkCompleted()

	batch.Reset()

	assert.Equal(t, 0, batch.MessageCount(), "Should have 0 messages after reset")
	assert.Equal(t, 0, batch.TotalBytes, "Total bytes should be 0 after reset")
	assert.True(t, batch.SentAt.IsZero(), "SentAt should be reset")
	assert.True(t, batch.CompletedAt.IsZero(), "CompletedAt should be reset")
	assert.False(t, batch.CreatedAt.IsZero(), "CreatedAt should be updated")
}

func TestBatch_Validation(t *testing.T) {
	tests := []struct {
		name        string
		batch       *models.Batch
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid batch",
			batch: &models.Batch{
				ID:    "valid-batch",
				Topic: "valid-topic",
				Messages: []*models.Message{
					{
						ID:        "msg-1",
						Topic:     "valid-topic",
						Value:     []byte("test"),
						Timestamp: time.Now(),
					},
				},
				CreatedAt: time.Now(),
			},
			expectError: false,
		},
		{
			name: "empty batch ID",
			batch: &models.Batch{
				ID:       "",
				Topic:    "valid-topic",
				Messages: []*models.Message{},
			},
			expectError: true,
			errorMsg:    "batch ID cannot be empty",
		},
		{
			name: "empty topic",
			batch: &models.Batch{
				ID:       "valid-batch",
				Topic:    "",
				Messages: []*models.Message{},
			},
			expectError: true,
			errorMsg:    "batch topic cannot be empty",
		},
		{
			name: "topic mismatch in messages",
			batch: &models.Batch{
				ID:    "valid-batch",
				Topic: "batch-topic",
				Messages: []*models.Message{
					{
						ID:        "msg-1",
						Topic:     "different-topic",
						Value:     []byte("test"),
						Timestamp: time.Now(),
					},
				},
			},
			expectError: true,
			errorMsg:    "message topic does not match batch topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.batch.Validate()
			if tt.expectError {
				assert.Error(t, err, "Should return validation error")
				assert.Contains(t, err.Error(), tt.errorMsg, "Error message should match expected")
			} else {
				assert.NoError(t, err, "Should not return validation error")
			}
		})
	}
}

func TestBatch_Clone(t *testing.T) {
	original := models.NewBatch("batch-1", "test-topic", []*models.Message{
		{
			ID:        "msg-1",
			Topic:     "test-topic",
			Value:     []byte("message 1"),
			Timestamp: time.Now(),
		},
	})

	cloned := original.Clone()

	assert.Equal(t, original.ID, cloned.ID, "Cloned batch should have same ID")
	assert.Equal(t, original.Topic, cloned.Topic, "Cloned batch should have same topic")
	assert.Equal(t, original.MessageCount(), cloned.MessageCount(), "Cloned batch should have same message count")
	assert.Equal(t, original.TotalBytes, cloned.TotalBytes, "Cloned batch should have same total bytes")

	// Verify it's a deep copy by modifying original
	original.Messages[0].Value = []byte("modified")
	assert.NotEqual(t, string(original.Messages[0].Value), string(cloned.Messages[0].Value),
		"Cloned messages should be independent of original")
}
