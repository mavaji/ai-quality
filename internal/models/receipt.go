package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// DeliveryStatus represents the status of message delivery
type DeliveryStatus string

const (
	DeliveryStatusSuccess        DeliveryStatus = "success"
	DeliveryStatusPartialFailure DeliveryStatus = "partial_failure"
	DeliveryStatusFailed         DeliveryStatus = "failed"
	DeliveryStatusPending        DeliveryStatus = "pending"
	DeliveryStatusTimeout        DeliveryStatus = "timeout"
	DeliveryStatusCancelled      DeliveryStatus = "cancelled"
)

// DeliveryReceipt represents confirmation of message delivery to Kafka
type DeliveryReceipt struct {
	MessageID    string         `json:"message_id"`
	Topic        string         `json:"topic"`
	Partition    int32          `json:"partition"`
	Offset       int64          `json:"offset"`
	DeliveredAt  time.Time      `json:"delivered_at"`
	Status       DeliveryStatus `json:"status"`
	ErrorMessage string         `json:"error_message,omitempty"`
	Metadata     Metadata       `json:"metadata,omitempty"`
}

// Metadata contains additional delivery information
type Metadata struct {
	AttemptCount    int           `json:"attempt_count"`
	RetryDuration   time.Duration `json:"retry_duration,omitempty"`
	BrokerAddress   string        `json:"broker_address,omitempty"`
	CompressionType string        `json:"compression_type,omitempty"`
	SerializedSize  int           `json:"serialized_size,omitempty"`
}

// NewDeliveryReceipt creates a new delivery receipt
func NewDeliveryReceipt(messageID, topic string, partition int32, offset int64, status DeliveryStatus) *DeliveryReceipt {
	return &DeliveryReceipt{
		MessageID:   messageID,
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		DeliveredAt: time.Now(),
		Status:      status,
		Metadata: Metadata{
			AttemptCount: 1,
		},
	}
}

// NewSuccessReceipt creates a receipt for successful delivery
func NewSuccessReceipt(messageID, topic string, partition int32, offset int64) *DeliveryReceipt {
	return NewDeliveryReceipt(messageID, topic, partition, offset, DeliveryStatusSuccess)
}

// NewFailureReceipt creates a receipt for failed delivery
func NewFailureReceipt(messageID, topic string, errorMessage string) *DeliveryReceipt {
	receipt := &DeliveryReceipt{
		MessageID:    messageID,
		Topic:        topic,
		Partition:    -1, // Unknown partition on failure
		Offset:       -1, // Unknown offset on failure
		DeliveredAt:  time.Now(),
		Status:       DeliveryStatusFailed,
		ErrorMessage: errorMessage,
		Metadata: Metadata{
			AttemptCount: 1,
		},
	}
	return receipt
}

// NewPartialFailureReceipt creates a receipt for partial failure (message sent but ack failed)
func NewPartialFailureReceipt(messageID, topic string, partition int32, offset int64, errorMessage string) *DeliveryReceipt {
	receipt := NewDeliveryReceipt(messageID, topic, partition, offset, DeliveryStatusPartialFailure)
	receipt.ErrorMessage = errorMessage
	return receipt
}

// Validate validates the delivery receipt fields
func (r *DeliveryReceipt) Validate() error {
	if r.MessageID == "" {
		return fmt.Errorf("message ID cannot be empty")
	}
	
	if r.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	
	if !r.IsValidStatus() {
		return fmt.Errorf("invalid delivery status: %s", r.Status)
	}
	
	if r.DeliveredAt.IsZero() {
		return fmt.Errorf("delivered at timestamp cannot be zero")
	}
	
	// For successful deliveries, partition and offset should be valid
	if r.Status == DeliveryStatusSuccess {
		if r.Partition < 0 {
			return fmt.Errorf("successful delivery must have valid partition (>= 0), got %d", r.Partition)
		}
		
		if r.Offset < 0 {
			return fmt.Errorf("successful delivery must have valid offset (>= 0), got %d", r.Offset)
		}
	}
	
	return nil
}

// IsValidStatus checks if the status is valid
func (r *DeliveryReceipt) IsValidStatus() bool {
	switch r.Status {
	case DeliveryStatusSuccess, DeliveryStatusPartialFailure, DeliveryStatusFailed, 
		 DeliveryStatusPending, DeliveryStatusTimeout, DeliveryStatusCancelled:
		return true
	default:
		return false
	}
}

// IsSuccessful returns true if the message was successfully delivered
func (r *DeliveryReceipt) IsSuccessful() bool {
	return r.Status == DeliveryStatusSuccess
}

// HasError returns true if the delivery encountered an error
func (r *DeliveryReceipt) HasError() bool {
	return r.Status == DeliveryStatusFailed || 
		   r.Status == DeliveryStatusPartialFailure || 
		   r.Status == DeliveryStatusTimeout
}

// IsPending returns true if the delivery is still in progress
func (r *DeliveryReceipt) IsPending() bool {
	return r.Status == DeliveryStatusPending
}

// SetError sets an error message and updates status to failed
func (r *DeliveryReceipt) SetError(errorMessage string) {
	r.ErrorMessage = errorMessage
	r.Status = DeliveryStatusFailed
	r.Partition = -1
	r.Offset = -1
}

// SetTimeout marks the receipt as timed out
func (r *DeliveryReceipt) SetTimeout(errorMessage string) {
	r.ErrorMessage = errorMessage
	r.Status = DeliveryStatusTimeout
}

// SetCancelled marks the receipt as cancelled
func (r *DeliveryReceipt) SetCancelled(reason string) {
	r.ErrorMessage = reason
	r.Status = DeliveryStatusCancelled
}

// UpdateAttemptCount increments the attempt count
func (r *DeliveryReceipt) UpdateAttemptCount() {
	r.Metadata.AttemptCount++
}

// SetRetryDuration sets the total time spent on retries
func (r *DeliveryReceipt) SetRetryDuration(duration time.Duration) {
	r.Metadata.RetryDuration = duration
}

// SetBrokerInfo sets broker information in metadata
func (r *DeliveryReceipt) SetBrokerInfo(address, compressionType string, serializedSize int) {
	r.Metadata.BrokerAddress = address
	r.Metadata.CompressionType = compressionType
	r.Metadata.SerializedSize = serializedSize
}

// ToJSON serializes the receipt to JSON
func (r *DeliveryReceipt) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

// String returns a string representation of the receipt
func (r *DeliveryReceipt) String() string {
	if r.HasError() {
		return fmt.Sprintf("DeliveryReceipt{ID:%s, Topic:%s, Status:%s, Error:%s}",
			r.MessageID, r.Topic, r.Status, r.ErrorMessage)
	}
	
	return fmt.Sprintf("DeliveryReceipt{ID:%s, Topic:%s, Partition:%d, Offset:%d, Status:%s}",
		r.MessageID, r.Topic, r.Partition, r.Offset, r.Status)
}

// Clone creates a deep copy of the delivery receipt
func (r *DeliveryReceipt) Clone() *DeliveryReceipt {
	return &DeliveryReceipt{
		MessageID:    r.MessageID,
		Topic:        r.Topic,
		Partition:    r.Partition,
		Offset:       r.Offset,
		DeliveredAt:  r.DeliveredAt,
		Status:       r.Status,
		ErrorMessage: r.ErrorMessage,
		Metadata: Metadata{
			AttemptCount:    r.Metadata.AttemptCount,
			RetryDuration:   r.Metadata.RetryDuration,
			BrokerAddress:   r.Metadata.BrokerAddress,
			CompressionType: r.Metadata.CompressionType,
			SerializedSize:  r.Metadata.SerializedSize,
		},
	}
}

// GetLatencyMs returns the delivery latency in milliseconds (if available)
func (r *DeliveryReceipt) GetLatencyMs() int64 {
	if r.DeliveredAt.IsZero() {
		return 0
	}
	
	// If we don't have the original message timestamp, we can't calculate latency
	// This would typically be calculated when creating the receipt
	return 0
}

// StatusDescription returns a human-readable description of the status
func (r *DeliveryReceipt) StatusDescription() string {
	switch r.Status {
	case DeliveryStatusSuccess:
		return "Message successfully delivered to Kafka"
	case DeliveryStatusPartialFailure:
		return "Message was sent but acknowledgment failed"
	case DeliveryStatusFailed:
		return "Message delivery failed"
	case DeliveryStatusPending:
		return "Message delivery is in progress"
	case DeliveryStatusTimeout:
		return "Message delivery timed out"
	case DeliveryStatusCancelled:
		return "Message delivery was cancelled"
	default:
		return fmt.Sprintf("Unknown status: %s", r.Status)
	}
}

// GetSummary returns a summary of the receipt for logging
func (r *DeliveryReceipt) GetSummary() map[string]interface{} {
	summary := map[string]interface{}{
		"message_id":    r.MessageID,
		"topic":         r.Topic,
		"status":        r.Status,
		"delivered_at":  r.DeliveredAt,
		"attempt_count": r.Metadata.AttemptCount,
	}
	
	if r.IsSuccessful() {
		summary["partition"] = r.Partition
		summary["offset"] = r.Offset
	}
	
	if r.HasError() {
		summary["error"] = r.ErrorMessage
	}
	
	if r.Metadata.RetryDuration > 0 {
		summary["retry_duration_ms"] = r.Metadata.RetryDuration.Milliseconds()
	}
	
	return summary
}