package models

import (
	"fmt"
	"time"
)

// Batch represents a group of messages to be sent together for optimal throughput
type Batch struct {
	// Identification
	ID    string `json:"id"`
	Topic string `json:"topic"`

	// Messages
	Messages   []*Message `json:"messages"`
	TotalBytes int        `json:"total_bytes"`

	// Configuration limits
	MaxMessages int `json:"max_messages"`
	MaxBytes    int `json:"max_bytes"`

	// Timestamps
	CreatedAt   time.Time `json:"created_at"`
	SentAt      time.Time `json:"sent_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

// NewBatch creates a new batch with the specified parameters
func NewBatch(id, topic string, messages []*Message) *Batch {
	batch := &Batch{
		ID:          id,
		Topic:       topic,
		Messages:    make([]*Message, 0),
		TotalBytes:  0,
		MaxMessages: 100,   // Default max messages
		MaxBytes:    16384, // Default max bytes (16KB)
		CreatedAt:   time.Now(),
	}

	if messages != nil {
		for _, msg := range messages {
			batch.AddMessage(msg)
		}
	}

	return batch
}

// AddMessage adds a message to the batch if it can be accepted
func (b *Batch) AddMessage(message *Message) error {
	if message.Topic != b.Topic {
		return fmt.Errorf("topic mismatch: expected %s, got %s", b.Topic, message.Topic)
	}

	if !b.CanAccept(message) {
		return fmt.Errorf("batch cannot accept message: would exceed limits")
	}

	b.Messages = append(b.Messages, message)
	b.TotalBytes += b.calculateMessageSize(message)

	return nil
}

// CanAccept checks if the batch can accept the given message
func (b *Batch) CanAccept(message *Message) bool {
	// Check topic match
	if message.Topic != b.Topic {
		return false
	}

	// Check message count limit
	if len(b.Messages) >= b.MaxMessages {
		return false
	}

	// Check size limit
	messageSize := b.calculateMessageSize(message)
	if b.TotalBytes+messageSize > b.MaxBytes {
		return false
	}

	return true
}

// IsFull checks if the batch is full (has reached capacity limits)
func (b *Batch) IsFull() bool {
	return len(b.Messages) >= b.MaxMessages || b.TotalBytes >= b.MaxBytes
}

// MessageCount returns the number of messages in the batch
func (b *Batch) MessageCount() int {
	return len(b.Messages)
}

// AverageMessageSize returns the average size of messages in the batch
func (b *Batch) AverageMessageSize() int {
	if len(b.Messages) == 0 {
		return 0
	}
	return b.TotalBytes / len(b.Messages)
}

// GetMessages returns a copy of the messages in the batch
func (b *Batch) GetMessages() []*Message {
	messages := make([]*Message, len(b.Messages))
	copy(messages, b.Messages)
	return messages
}

// MarkSent marks the batch as sent
func (b *Batch) MarkSent() {
	b.SentAt = time.Now()
}

// MarkCompleted marks the batch as completed
func (b *Batch) MarkCompleted() {
	b.CompletedAt = time.Now()
}

// ProcessingDuration returns the time taken to process the batch
func (b *Batch) ProcessingDuration() time.Duration {
	if b.CompletedAt.IsZero() {
		return 0
	}
	return b.CompletedAt.Sub(b.CreatedAt)
}

// Reset clears the batch and resets it for reuse
func (b *Batch) Reset() {
	b.Messages = make([]*Message, 0)
	b.TotalBytes = 0
	b.SentAt = time.Time{}
	b.CompletedAt = time.Time{}
	b.CreatedAt = time.Now()
}

// Validate performs validation on the batch
func (b *Batch) Validate() error {
	if b.ID == "" {
		return fmt.Errorf("batch ID cannot be empty")
	}

	if b.Topic == "" {
		return fmt.Errorf("batch topic cannot be empty")
	}

	// Validate that all messages have the same topic as the batch
	for i, msg := range b.Messages {
		if msg.Topic != b.Topic {
			return fmt.Errorf("message %d topic (%s) does not match batch topic (%s)",
				i, msg.Topic, b.Topic)
		}
	}

	return nil
}

// Clone creates a deep copy of the batch
func (b *Batch) Clone() *Batch {
	cloned := &Batch{
		ID:          b.ID,
		Topic:       b.Topic,
		Messages:    make([]*Message, len(b.Messages)),
		TotalBytes:  b.TotalBytes,
		MaxMessages: b.MaxMessages,
		MaxBytes:    b.MaxBytes,
		CreatedAt:   b.CreatedAt,
		SentAt:      b.SentAt,
		CompletedAt: b.CompletedAt,
	}

	// Deep copy messages
	for i, msg := range b.Messages {
		cloned.Messages[i] = &Message{
			ID:        msg.ID,
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     make([]byte, len(msg.Value)),
			Headers:   make(map[string]string),
			Timestamp: msg.Timestamp,
		}
		copy(cloned.Messages[i].Value, msg.Value)
		for k, v := range msg.Headers {
			cloned.Messages[i].Headers[k] = v
		}
	}

	return cloned
}

// calculateMessageSize calculates the total size of a message including metadata
func (b *Batch) calculateMessageSize(message *Message) int {
	size := len(message.Value)
	size += len(message.Key)
	size += len(message.Topic)
	size += len(message.ID)

	// Add headers size
	for k, v := range message.Headers {
		size += len(k) + len(v)
	}

	// Add some overhead for metadata
	size += 64 // Approximate overhead for timestamps, etc.

	return size
}

// BatchConfig contains configuration options for batches
type BatchConfig struct {
	MaxMessages        int           `json:"max_messages" yaml:"max_messages"`
	MaxBytes           int           `json:"max_bytes" yaml:"max_bytes"`
	FlushInterval      time.Duration `json:"flush_interval" yaml:"flush_interval"`
	LingerTime         time.Duration `json:"linger_time" yaml:"linger_time"`
	CompressionEnabled bool          `json:"compression_enabled" yaml:"compression_enabled"`
}

// DefaultBatchConfig returns a batch configuration with sensible defaults
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxMessages:        100,
		MaxBytes:           16384, // 16KB
		FlushInterval:      10 * time.Millisecond,
		LingerTime:         5 * time.Millisecond,
		CompressionEnabled: true,
	}
}

// Validate validates the batch configuration
func (bc *BatchConfig) Validate() error {
	if bc.MaxMessages <= 0 {
		return fmt.Errorf("max_messages must be positive, got %d", bc.MaxMessages)
	}

	if bc.MaxMessages > 1000 {
		return fmt.Errorf("max_messages cannot exceed 1000, got %d", bc.MaxMessages)
	}

	if bc.MaxBytes <= 0 {
		return fmt.Errorf("max_bytes must be positive, got %d", bc.MaxBytes)
	}

	if bc.MaxBytes > 10*1024*1024 { // 10MB limit
		return fmt.Errorf("max_bytes cannot exceed 10MB, got %d", bc.MaxBytes)
	}

	if bc.FlushInterval < 0 {
		return fmt.Errorf("flush_interval cannot be negative, got %v", bc.FlushInterval)
	}

	if bc.FlushInterval > 1*time.Minute {
		return fmt.Errorf("flush_interval cannot exceed 1 minute, got %v", bc.FlushInterval)
	}

	if bc.LingerTime < 0 {
		return fmt.Errorf("linger_time cannot be negative, got %v", bc.LingerTime)
	}

	if bc.LingerTime > 1*time.Second {
		return fmt.Errorf("linger_time cannot exceed 1 second, got %v", bc.LingerTime)
	}

	return nil
}

// BatchMetrics contains metrics about batch processing
type BatchMetrics struct {
	TotalBatches       int64         `json:"total_batches"`
	TotalMessages      int64         `json:"total_messages"`
	AverageBatchSize   float64       `json:"average_batch_size"`
	AverageLatency     time.Duration `json:"average_latency"`
	ThroughputMsgSec   float64       `json:"throughput_msg_sec"`
	ThroughputBytesSec float64       `json:"throughput_bytes_sec"`
}

// BatchStatus represents the current status of a batch
type BatchStatus string

const (
	BatchStatusPending   BatchStatus = "pending"
	BatchStatusSending   BatchStatus = "sending"
	BatchStatusSent      BatchStatus = "sent"
	BatchStatusCompleted BatchStatus = "completed"
	BatchStatusFailed    BatchStatus = "failed"
	BatchStatusTimeout   BatchStatus = "timeout"
)

// GetStatus returns the current status of the batch based on its timestamps
func (b *Batch) GetStatus() BatchStatus {
	if !b.CompletedAt.IsZero() {
		return BatchStatusCompleted
	}
	if !b.SentAt.IsZero() {
		return BatchStatusSent
	}
	return BatchStatusPending
}
