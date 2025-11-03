package producer

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"sdd-kafka-producer/internal/models"
)

// MessageStatus represents the current status of a message
type MessageStatus struct {
	MessageID    string                  `json:"message_id"`
	Topic        string                  `json:"topic"`
	CurrentState models.DeliveryStatus   `json:"current_state"`
	Receipt      *models.DeliveryReceipt `json:"receipt,omitempty"`
	CreatedAt    time.Time               `json:"created_at"`
	UpdatedAt    time.Time               `json:"updated_at"`
	History      []StatusHistoryEntry    `json:"history"`
}

// StatusHistoryEntry represents a historical status change
type StatusHistoryEntry struct {
	State     models.DeliveryStatus `json:"state"`
	Timestamp time.Time             `json:"timestamp"`
	Details   string                `json:"details,omitempty"`
}

// MessageTracker tracks the status of messages throughout their lifecycle
type MessageTracker struct {
	statuses map[string]*MessageStatus
	mu       sync.RWMutex
	logger   *zap.Logger
	
	// Configuration
	maxHistorySize int
	retentionTime  time.Duration
	cleanupTicker  *time.Ticker
	stopCleanup    chan struct{}
}

// NewMessageTracker creates a new message tracker
func NewMessageTracker(logger *zap.Logger, maxHistorySize int, retentionTime time.Duration) *MessageTracker {
	tracker := &MessageTracker{
		statuses:       make(map[string]*MessageStatus),
		logger:         logger,
		maxHistorySize: maxHistorySize,
		retentionTime:  retentionTime,
		stopCleanup:    make(chan struct{}),
	}

	// Start cleanup routine
	tracker.startCleanupRoutine()

	return tracker
}

// TrackMessage starts tracking a new message
func (t *MessageTracker) TrackMessage(messageID, topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	status := &MessageStatus{
		MessageID:    messageID,
		Topic:        topic,
		CurrentState: models.DeliveryStatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
		History: []StatusHistoryEntry{
			{
				State:     models.DeliveryStatusPending,
				Timestamp: now,
				Details:   "Message queued for delivery",
			},
		},
	}

	t.statuses[messageID] = status

	t.logger.Debug("Started tracking message",
		zap.String("message_id", messageID),
		zap.String("topic", topic))
}

// UpdateStatus updates the status of a tracked message
func (t *MessageTracker) UpdateStatus(messageID string, newState models.DeliveryStatus, details string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	status, exists := t.statuses[messageID]
	if !exists {
		return fmt.Errorf("message %s not found in tracker", messageID)
	}

	now := time.Now()
	
	// Update current state
	status.CurrentState = newState
	status.UpdatedAt = now

	// Add to history
	historyEntry := StatusHistoryEntry{
		State:     newState,
		Timestamp: now,
		Details:   details,
	}

	status.History = append(status.History, historyEntry)

	// Trim history if it exceeds max size
	if len(status.History) > t.maxHistorySize {
		status.History = status.History[len(status.History)-t.maxHistorySize:]
	}

	t.logger.Debug("Updated message status",
		zap.String("message_id", messageID),
		zap.String("old_state", string(status.CurrentState)),
		zap.String("new_state", string(newState)),
		zap.String("details", details))

	return nil
}

// SetReceipt sets the delivery receipt for a message
func (t *MessageTracker) SetReceipt(messageID string, receipt *models.DeliveryReceipt) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	status, exists := t.statuses[messageID]
	if !exists {
		return fmt.Errorf("message %s not found in tracker", messageID)
	}

	status.Receipt = receipt
	status.CurrentState = receipt.Status
	status.UpdatedAt = time.Now()

	// Add receipt to history
	details := fmt.Sprintf("Receipt received: partition=%d, offset=%d", 
		receipt.Partition, receipt.Offset)
	if receipt.ErrorMessage != "" {
		details += fmt.Sprintf(", error=%s", receipt.ErrorMessage)
	}

	historyEntry := StatusHistoryEntry{
		State:     receipt.Status,
		Timestamp: receipt.DeliveredAt,
		Details:   details,
	}

	status.History = append(status.History, historyEntry)

	// Trim history if needed
	if len(status.History) > t.maxHistorySize {
		status.History = status.History[len(status.History)-t.maxHistorySize:]
	}

	return nil
}

// GetStatus retrieves the current status of a message
func (t *MessageTracker) GetStatus(messageID string) (*MessageStatus, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	status, exists := t.statuses[messageID]
	if !exists {
		return nil, fmt.Errorf("message %s not found", messageID)
	}

	// Return a copy to prevent external modifications
	return t.copyStatus(status), nil
}

// GetAllStatuses returns all tracked message statuses
func (t *MessageTracker) GetAllStatuses() map[string]*MessageStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]*MessageStatus, len(t.statuses))
	for id, status := range t.statuses {
		result[id] = t.copyStatus(status)
	}

	return result
}

// GetStatusesByTopic returns all statuses for messages in a specific topic
func (t *MessageTracker) GetStatusesByTopic(topic string) []*MessageStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []*MessageStatus
	for _, status := range t.statuses {
		if status.Topic == topic {
			result = append(result, t.copyStatus(status))
		}
	}

	return result
}

// GetStatusesByState returns all messages with a specific state
func (t *MessageTracker) GetStatusesByState(state models.DeliveryStatus) []*MessageStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []*MessageStatus
	for _, status := range t.statuses {
		if status.CurrentState == state {
			result = append(result, t.copyStatus(status))
		}
	}

	return result
}

// RemoveMessage removes a message from tracking
func (t *MessageTracker) RemoveMessage(messageID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.statuses, messageID)

	t.logger.Debug("Removed message from tracking",
		zap.String("message_id", messageID))
}

// GetStats returns tracking statistics
func (t *MessageTracker) GetStats() map[string]interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	stats := map[string]interface{}{
		"total_messages": len(t.statuses),
		"by_state":       make(map[string]int),
		"retention_time": t.retentionTime.String(),
		"max_history":    t.maxHistorySize,
	}

	// Count messages by state
	stateCount := make(map[string]int)
	for _, status := range t.statuses {
		stateCount[string(status.CurrentState)]++
	}
	stats["by_state"] = stateCount

	return stats
}

// copyStatus creates a deep copy of a MessageStatus
func (t *MessageTracker) copyStatus(original *MessageStatus) *MessageStatus {
	if original == nil {
		return nil
	}

	copy := &MessageStatus{
		MessageID:    original.MessageID,
		Topic:        original.Topic,
		CurrentState: original.CurrentState,
		CreatedAt:    original.CreatedAt,
		UpdatedAt:    original.UpdatedAt,
		History:      make([]StatusHistoryEntry, len(original.History)),
	}

	// Copy history
	for i, entry := range original.History {
		copy.History[i] = entry
	}

	// Copy receipt if present
	if original.Receipt != nil {
		copy.Receipt = original.Receipt.Clone()
	}

	return copy
}

// startCleanupRoutine starts a background goroutine to clean up old statuses
func (t *MessageTracker) startCleanupRoutine() {
	t.cleanupTicker = time.NewTicker(10 * time.Minute) // Cleanup every 10 minutes
	
	go func() {
		for {
			select {
			case <-t.cleanupTicker.C:
				t.cleanupOldStatuses()
			case <-t.stopCleanup:
				t.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanupOldStatuses removes statuses older than the retention time
func (t *MessageTracker) cleanupOldStatuses() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-t.retentionTime)
	
	var toDelete []string
	for messageID, status := range t.statuses {
		// Remove if the status is old and in a final state
		if status.UpdatedAt.Before(cutoff) && t.isFinalState(status.CurrentState) {
			toDelete = append(toDelete, messageID)
		}
	}

	for _, messageID := range toDelete {
		delete(t.statuses, messageID)
	}

	if len(toDelete) > 0 {
		t.logger.Info("Cleaned up old message statuses",
			zap.Int("removed_count", len(toDelete)),
			zap.Int("remaining_count", len(t.statuses)))
	}
}

// isFinalState returns true if the state represents a final state (no more changes expected)
func (t *MessageTracker) isFinalState(state models.DeliveryStatus) bool {
	switch state {
	case models.DeliveryStatusSuccess,
		 models.DeliveryStatusFailed,
		 models.DeliveryStatusCancelled:
		return true
	default:
		return false
	}
}

// Close stops the tracker and cleanup routine
func (t *MessageTracker) Close() error {
	close(t.stopCleanup)
	
	if t.cleanupTicker != nil {
		t.cleanupTicker.Stop()
	}

	t.logger.Info("Message tracker stopped")
	return nil
}

// Clear removes all tracked messages (for testing)
func (t *MessageTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.statuses = make(map[string]*MessageStatus)
	t.logger.Debug("Cleared all tracked messages")
}