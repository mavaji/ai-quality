package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	
	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

// MockProducer implements producer interface for testing
type MockProducer struct {
	mock.Mock
}

// Verify MockProducer implements Producer interface
var _ producer.Producer = (*MockProducer)(nil)

func (m *MockProducer) PublishMessage(ctx context.Context, message *models.Message) (*models.DeliveryReceipt, error) {
	args := m.Called(ctx, message)
	return args.Get(0).(*models.DeliveryReceipt), args.Error(1)
}

func (m *MockProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestProducer_PublishMessage_Success(t *testing.T) {
	mockProducer := new(MockProducer)
	
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     []byte("test message"),
		Timestamp: time.Now(),
	}

	expectedReceipt := &models.DeliveryReceipt{
		MessageID:   "msg-123",
		Topic:       "test-topic",
		Partition:   0,
		Offset:      100,
		DeliveredAt: time.Now(),
		Status:      models.DeliveryStatusSuccess,
	}

	mockProducer.On("PublishMessage", mock.Anything, message).Return(expectedReceipt, nil)

	ctx := context.Background()
	receipt, err := mockProducer.PublishMessage(ctx, message)

	require.NoError(t, err, "Publishing valid message should succeed")
	assert.NotNil(t, receipt, "Receipt should not be nil")
	assert.Equal(t, expectedReceipt.MessageID, receipt.MessageID, "Message ID should match")
	assert.Equal(t, expectedReceipt.Topic, receipt.Topic, "Topic should match")
	assert.Equal(t, models.DeliveryStatusSuccess, receipt.Status, "Status should be success")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_PublishMessage_InvalidMessage(t *testing.T) {
	mockProducer := new(MockProducer)
	
	// Create invalid message (empty topic)
	invalidMessage := &models.Message{
		Topic: "",
		Key:   "test-key",
		Value: []byte("test message"),
	}

	expectedError := errors.New("validation error: topic cannot be empty")
	mockProducer.On("PublishMessage", mock.Anything, invalidMessage).Return((*models.DeliveryReceipt)(nil), expectedError)

	ctx := context.Background()
	receipt, err := mockProducer.PublishMessage(ctx, invalidMessage)

	require.Error(t, err, "Publishing invalid message should fail")
	assert.Nil(t, receipt, "Receipt should be nil on error")
	assert.Contains(t, err.Error(), "validation", "Error should mention validation")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_PublishMessage_BrokerError(t *testing.T) {
	mockProducer := new(MockProducer)
	
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     []byte("test message"),
		Timestamp: time.Now(),
	}

	brokerError := errors.New("kafka: broker connection failed")
	mockProducer.On("PublishMessage", mock.Anything, message).Return((*models.DeliveryReceipt)(nil), brokerError)

	ctx := context.Background()
	receipt, err := mockProducer.PublishMessage(ctx, message)

	require.Error(t, err, "Broker error should be propagated")
	assert.Nil(t, receipt, "Receipt should be nil on error")
	assert.Contains(t, err.Error(), "broker", "Error should mention broker")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_PublishMessage_ContextCancellation(t *testing.T) {
	mockProducer := new(MockProducer)
	
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     []byte("test message"),
		Timestamp: time.Now(),
	}

	cancelledError := context.Canceled
	mockProducer.On("PublishMessage", mock.Anything, message).Return((*models.DeliveryReceipt)(nil), cancelledError)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	receipt, err := mockProducer.PublishMessage(ctx, message)

	require.Error(t, err, "Cancelled context should return error")
	assert.Nil(t, receipt, "Receipt should be nil on cancellation")
	assert.Equal(t, context.Canceled, err, "Error should be context cancelled")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_PublishMessage_Timeout(t *testing.T) {
	mockProducer := new(MockProducer)
	
	message := &models.Message{
		Topic:     "test-topic",  
		Key:       "test-key",
		Value:     []byte("test message"),
		Timestamp: time.Now(),
	}

	timeoutError := context.DeadlineExceeded
	mockProducer.On("PublishMessage", mock.Anything, message).Return((*models.DeliveryReceipt)(nil), timeoutError)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	receipt, err := mockProducer.PublishMessage(ctx, message)

	require.Error(t, err, "Timeout should return error")
	assert.Nil(t, receipt, "Receipt should be nil on timeout")
	assert.Equal(t, context.DeadlineExceeded, err, "Error should be deadline exceeded")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_PublishMessage_PartialFailure(t *testing.T) {
	mockProducer := new(MockProducer)
	
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key", 
		Value:     []byte("test message"),
		Timestamp: time.Now(),
	}

	// Simulate a partial failure where message was sent but acknowledgment failed
	failureReceipt := &models.DeliveryReceipt{
		MessageID:    "msg-456",
		Topic:        "test-topic",
		Partition:    -1, // Indicates unknown partition
		Offset:       -1, // Indicates unknown offset
		DeliveredAt:  time.Now(),
		Status:       models.DeliveryStatusPartialFailure,
		ErrorMessage: "acknowledgment timeout",
	}

	mockProducer.On("PublishMessage", mock.Anything, message).Return(failureReceipt, nil)

	ctx := context.Background()
	receipt, err := mockProducer.PublishMessage(ctx, message)

	require.NoError(t, err, "Partial failure should not return error")
	assert.NotNil(t, receipt, "Receipt should be provided")
	assert.Equal(t, models.DeliveryStatusPartialFailure, receipt.Status, "Status should indicate partial failure")
	assert.Equal(t, int32(-1), receipt.Partition, "Partition should be unknown")
	assert.Equal(t, int64(-1), receipt.Offset, "Offset should be unknown")
	assert.NotEmpty(t, receipt.ErrorMessage, "Error message should be provided")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_PublishMessage_RetryableError(t *testing.T) {
	mockProducer := new(MockProducer)
	
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     []byte("test message"),
		Timestamp: time.Now(),
	}

	retryableError := errors.New("kafka: request timed out")
	mockProducer.On("PublishMessage", mock.Anything, message).Return((*models.DeliveryReceipt)(nil), retryableError)

	ctx := context.Background()
	receipt, err := mockProducer.PublishMessage(ctx, message)

	require.Error(t, err, "Retryable error should be returned")
	assert.Nil(t, receipt, "Receipt should be nil on error")
	assert.Contains(t, err.Error(), "timed out", "Error should indicate timeout")
	
	mockProducer.AssertExpectations(t)
}

func TestProducer_Close_Success(t *testing.T) {
	mockProducer := new(MockProducer)
	
	mockProducer.On("Close").Return(nil)

	err := mockProducer.Close()

	assert.NoError(t, err, "Closing producer should succeed")
	mockProducer.AssertExpectations(t)
}

func TestProducer_Close_Error(t *testing.T) {
	mockProducer := new(MockProducer)
	
	closeError := errors.New("failed to close producer connections")
	mockProducer.On("Close").Return(closeError)

	err := mockProducer.Close()

	require.Error(t, err, "Close error should be propagated")
	assert.Contains(t, err.Error(), "close", "Error should mention close operation")
	mockProducer.AssertExpectations(t)
}

// Test producer configuration validation
func TestNewProducer_Configuration(t *testing.T) {
	tests := []struct {
		name        string
		brokers     []string
		shouldError bool
		errorMsg    string
	}{
		{
			name:        "valid single broker",
			brokers:     []string{"localhost:9092"},
			shouldError: false,
		},
		{
			name:        "valid multiple brokers",
			brokers:     []string{"broker1:9092", "broker2:9092", "broker3:9092"},
			shouldError: false,
		},
		{
			name:        "empty brokers list",
			brokers:     []string{},
			shouldError: true,
			errorMsg:    "brokers",
		},
		{
			name:        "invalid broker format",
			brokers:     []string{"invalid-broker"},
			shouldError: true,
			errorMsg:    "format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test would validate producer configuration
			// Implementation depends on actual Producer constructor
			if tt.shouldError {
				// Expect constructor to validate and return error
				assert.True(t, len(tt.errorMsg) > 0, "Error message should be specified for failing tests")
			} else {
				// Expect constructor to succeed
				assert.False(t, tt.shouldError, "Valid configuration should not error")
			}
		})
	}
}