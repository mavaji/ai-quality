package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	
	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

// PublishTestSuite groups integration tests for message publishing
type PublishTestSuite struct {
	suite.Suite
	producer producer.Producer
	config   *config.Config
	ctx      context.Context
	cancel   context.CancelFunc
}

// SetupSuite runs once before all tests in the suite
func (suite *PublishTestSuite) SetupSuite() {
	// Load test configuration
	cfg, err := config.Load("../../configs/kafka-producer.yaml")
	if err != nil {
		// Use default test configuration if file doesn't exist
		cfg = &config.Config{
			Kafka: config.KafkaConfig{
				Brokers: []string{"localhost:9092"}, // Default test broker
				Producer: config.ProducerConfig{
					BatchSize:        1,
					MaxMessageBytes:  1048576, // 1MB
					FlushFrequency:   100 * time.Millisecond,
					Compression:      "none",
				},
				Retry: config.RetryConfig{
					MaxAttempts:       3,
					InitialBackoff:    100 * time.Millisecond,
					MaxBackoff:        5 * time.Second,
					BackoffMultiplier: 2.0,
				},
				Timeouts: config.TimeoutConfig{
					Connection: 10 * time.Second,
					Request:    30 * time.Second,
					Delivery:   60 * time.Second,
				},
				Security: config.SecurityConfig{
					Protocol: "PLAINTEXT",
				},
			},
		}
	}
	
	suite.config = cfg
	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), 30*time.Second)
	
	// Initialize producer (this will fail until producer is implemented)
	// Uncomment when producer.New() is implemented:
	// suite.producer, err = producer.New(cfg)
	// suite.Require().NoError(err, "Failed to create producer for integration tests")
}

// TearDownSuite runs once after all tests in the suite
func (suite *PublishTestSuite) TearDownSuite() {
	if suite.producer != nil {
		err := suite.producer.Close()
		suite.Assert().NoError(err, "Failed to close producer")
	}
	suite.cancel()
}

// TestSingleMessageDelivery tests publishing a single message and verifying delivery
func (suite *PublishTestSuite) TestSingleMessageDelivery() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")
		return
	}
	
	message := &models.Message{
		Topic:     "integration-test-topic",
		Key:       fmt.Sprintf("test-key-%d", time.Now().Unix()),
		Value:     []byte(fmt.Sprintf("Integration test message at %s", time.Now().Format(time.RFC3339))),
		Headers:   map[string]string{
			"test-type": "integration",
			"timestamp": time.Now().Format(time.RFC3339),
		},
		Timestamp: time.Now(),
	}

	receipt, err := suite.producer.PublishMessage(suite.ctx, message)
	
	suite.Require().NoError(err, "Message publishing should succeed")
	suite.Require().NotNil(receipt, "Delivery receipt should be provided")
	
	// Verify receipt properties
	suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status, "Delivery status should be success")
	suite.Assert().Equal(message.Topic, receipt.Topic, "Receipt topic should match message topic")
	suite.Assert().NotEmpty(receipt.MessageID, "Message ID should be generated")
	suite.Assert().GreaterOrEqual(receipt.Partition, int32(0), "Partition should be valid")
	suite.Assert().GreaterOrEqual(receipt.Offset, int64(0), "Offset should be valid")
	suite.Assert().WithinDuration(time.Now(), receipt.DeliveredAt, 5*time.Second, "Delivery time should be recent")
}

// TestMessageWithoutKey tests publishing a message without a partition key
func (suite *PublishTestSuite) TestMessageWithoutKey() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")
		return
	}
	
	message := &models.Message{
		Topic:     "integration-test-topic",
		Key:       "", // No partition key
		Value:     []byte("Message without partition key"),
		Timestamp: time.Now(),
	}

	receipt, err := suite.producer.PublishMessage(suite.ctx, message)
	
	suite.Require().NoError(err, "Message without key should be publishable")
	suite.Require().NotNil(receipt, "Receipt should be provided")
	suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status, "Delivery should succeed")
}

// TestLargeMessage tests publishing a message near the size limit
func (suite *PublishTestSuite) TestLargeMessage() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")
		return
	}
	
	// Create a message close to 1MB (but under the limit)
	largePayload := make([]byte, 512*1024) // 512KB
	for i := range largePayload {
		largePayload[i] = byte('A' + (i % 26)) // Fill with letters
	}
	
	message := &models.Message{
		Topic:     "integration-test-topic",
		Key:       "large-message-key",
		Value:     largePayload,
		Timestamp: time.Now(),
	}

	receipt, err := suite.producer.PublishMessage(suite.ctx, message)
	
	suite.Require().NoError(err, "Large message should be publishable")
	suite.Require().NotNil(receipt, "Receipt should be provided")
	suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status, "Large message delivery should succeed")
}

// TestConcurrentPublishing tests publishing multiple messages concurrently
func (suite *PublishTestSuite) TestConcurrentPublishing() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")
		return
	}
	
	const numMessages = 10
	receipts := make([]*models.DeliveryReceipt, numMessages)
	errors := make([]error, numMessages)
	
	// Create channel to coordinate goroutines
	done := make(chan bool, numMessages)
	
	// Launch concurrent publishing
	for i := 0; i < numMessages; i++ {
		go func(index int) {
			defer func() { done <- true }()
			
			message := &models.Message{
				Topic:     "integration-test-topic",
				Key:       fmt.Sprintf("concurrent-key-%d", index),
				Value:     []byte(fmt.Sprintf("Concurrent message %d", index)),
				Headers:   map[string]string{
					"message-index": fmt.Sprintf("%d", index),
				},
				Timestamp: time.Now(),
			}
			
			receipt, err := suite.producer.PublishMessage(suite.ctx, message)
			receipts[index] = receipt
			errors[index] = err
		}(i)
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < numMessages; i++ {
		select {
		case <-done:
			// Goroutine completed
		case <-time.After(10 * time.Second):
			suite.T().Fatal("Concurrent publishing timed out")
		}
	}
	
	// Verify all messages were published successfully
	for i := 0; i < numMessages; i++ {
		suite.Assert().NoError(errors[i], "Message %d should publish successfully", i)
		if receipts[i] != nil {
			suite.Assert().Equal(models.DeliveryStatusSuccess, receipts[i].Status, "Message %d should have success status", i)
		}
	}
}

// TestPublishingTimeout tests behavior when publishing times out
func (suite *PublishTestSuite) TestPublishingTimeout() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")  
		return
	}
	
	// Create a short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	
	message := &models.Message{
		Topic:     "integration-test-topic",
		Key:       "timeout-test-key",
		Value:     []byte("This message should timeout"),
		Timestamp: time.Now(),
	}

	receipt, err := suite.producer.PublishMessage(ctx, message)
	
	// Expect timeout error
	suite.Require().Error(err, "Short timeout should cause error")
	suite.Assert().Nil(receipt, "Receipt should be nil on timeout")
	suite.Assert().Equal(context.DeadlineExceeded, err, "Error should be deadline exceeded")
}

// TestInvalidTopicName tests publishing to an invalid topic
func (suite *PublishTestSuite) TestInvalidTopicName() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")
		return
	}
	
	message := &models.Message{
		Topic:     "invalid/topic/name", // Invalid characters
		Key:       "test-key",
		Value:     []byte("Message to invalid topic"),
		Timestamp: time.Now(),
	}

	receipt, err := suite.producer.PublishMessage(suite.ctx, message)
	
	suite.Require().Error(err, "Invalid topic name should cause error")
	suite.Assert().Nil(receipt, "Receipt should be nil for invalid topic")
}

// TestMessageHeaders tests that message headers are preserved
func (suite *PublishTestSuite) TestMessageHeaders() {
	if suite.producer == nil {
		suite.T().Skip("Producer not available - implementation pending")
		return
	}
	
	headers := map[string]string{
		"content-type":    "application/json",
		"correlation-id":  "test-correlation-123",
		"source-service":  "integration-test",
		"message-version": "1.0",
	}
	
	message := &models.Message{
		Topic:     "integration-test-topic",
		Key:       "header-test-key",
		Value:     []byte(`{"test": "message with headers"}`),
		Headers:   headers,
		Timestamp: time.Now(),
	}

	receipt, err := suite.producer.PublishMessage(suite.ctx, message)
	
	suite.Require().NoError(err, "Message with headers should publish successfully")
	suite.Require().NotNil(receipt, "Receipt should be provided")
	suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status, "Message with headers should succeed")
	
	// Note: In a full integration test, you might want to consume the message
	// back from Kafka to verify headers were preserved correctly
}

// Run the test suite
func TestPublishIntegration(t *testing.T) {
	// Skip integration tests if not running in integration environment
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	
	suite.Run(t, new(PublishTestSuite))
}

// Benchmark test for single message publishing performance
func BenchmarkSingleMessagePublish(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}
	
	// This benchmark will be implemented once the producer is available
	b.Skip("Producer not available - implementation pending")
	
	// Uncomment when producer is implemented:
	// cfg := getTestConfig()
	// producer, err := producer.New(cfg)
	// require.NoError(b, err)
	// defer producer.Close()
	//
	// message := &models.Message{
	// 	Topic:     "benchmark-topic",
	// 	Key:       "benchmark-key",
	// 	Value:     []byte("benchmark message payload"),
	// 	Timestamp: time.Now(),
	// }
	//
	// ctx := context.Background()
	// b.ResetTimer()
	//
	// for i := 0; i < b.N; i++ {
	// 	_, err := producer.PublishMessage(ctx, message)
	// 	if err != nil {
	// 		b.Fatal(err)
	// 	}
	// }
}

// Helper function to get test configuration
func getTestConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Producer: config.ProducerConfig{
				BatchSize:       1,
				MaxMessageBytes: 1048576,
				FlushFrequency:  100 * time.Millisecond,
				Compression:     "none",
			},
			Retry: config.RetryConfig{
				MaxAttempts:       3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        5 * time.Second,
				BackoffMultiplier: 2.0,
			},
			Timeouts: config.TimeoutConfig{
				Connection: 10 * time.Second,
				Request:    30 * time.Second,
				Delivery:   60 * time.Second,
			},
			Security: config.SecurityConfig{
				Protocol: "PLAINTEXT",
			},
		},
	}
}