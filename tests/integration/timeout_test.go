package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

// TimeoutTestSuite groups integration tests for network timeout scenarios
type TimeoutTestSuite struct {
	suite.Suite
	config *config.Config
	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// SetupSuite runs once before all tests in the suite
func (suite *TimeoutTestSuite) SetupSuite() {
	// Create test configuration optimized for timeout testing
	suite.config = &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{
				"localhost:9092", // Primary broker
			},
			Producer: config.ProducerConfig{
				BatchSize:       1,
				MaxMessageBytes: 1048576, // 1MB
				FlushFrequency:  50 * time.Millisecond,
				Compression:     "none",
			},
			Retry: config.RetryConfig{
				MaxAttempts:       3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        5 * time.Second,
				BackoffMultiplier: 2.0,
			},
			Timeouts: config.TimeoutConfig{
				Connection: 2 * time.Second,  // Short connection timeout
				Request:    3 * time.Second,  // Short request timeout
				Delivery:   10 * time.Second, // Moderate delivery timeout
			},
			Security: config.SecurityConfig{
				Protocol: "PLAINTEXT",
			},
		},
	}

	// Create logger
	logger, err := zap.NewDevelopment()
	suite.Require().NoError(err, "Failed to create logger")
	suite.logger = logger

	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), 2*time.Minute)
}

// TearDownSuite runs once after all tests in the suite
func (suite *TimeoutTestSuite) TearDownSuite() {
	suite.cancel()
}

// TestConnectionTimeout tests producer behavior when connection timeout occurs
func (suite *TimeoutTestSuite) TestConnectionTimeout() {
	if testing.Short() {
		suite.T().Skip("Skipping connection timeout test in short mode")
	}

	// Create configuration with very short connection timeout and unreachable broker
	timeoutConfig := *suite.config
	timeoutConfig.Kafka.Timeouts.Connection = 100 * time.Millisecond // Very short
	timeoutConfig.Kafka.Brokers = []string{
		"192.0.2.1:9092", // RFC 5737 test address (non-routable)
	}

	start := time.Now()
	prod, err := producer.New(&timeoutConfig, suite.logger)
	elapsed := time.Since(start)

	if err != nil {
		// Connection timeout during producer creation is expected
		suite.T().Logf("Expected connection timeout during producer creation: %v", err)
		suite.Assert().Contains(err.Error(), "timeout", "Error should mention timeout")

		// Verify timing - should be close to configured timeout
		expectedTimeout := timeoutConfig.Kafka.Timeouts.Connection
		tolerance := 500 * time.Millisecond // Allow some variance

		suite.Assert().InDelta(expectedTimeout.Milliseconds(), elapsed.Milliseconds(),
			float64(tolerance.Milliseconds()),
			"Connection timeout should occur near configured timeout")
		return
	}

	// If producer creation succeeds, test message sending
	if prod != nil {
		defer prod.Close()

		message := &models.Message{
			Topic:     "timeout-test-topic",
			Key:       "connection-timeout-test",
			Value:     []byte("Test message for connection timeout"),
			Timestamp: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		start = time.Now()
		receipt, err := prod.PublishMessage(ctx, message)
		elapsed = time.Since(start)

		// Should timeout during message sending
		suite.Require().Error(err, "Should timeout when sending to unreachable broker")
		suite.Assert().Nil(receipt, "Receipt should be nil on timeout")

		suite.T().Logf("Message sending timed out in %v: %v", elapsed, err)
	}
}

// TestRequestTimeout tests producer behavior when request timeout occurs
func (suite *TimeoutTestSuite) TestRequestTimeout() {
	if testing.Short() {
		suite.T().Skip("Skipping request timeout test in short mode")
	}

	// Configure very short request timeout
	timeoutConfig := *suite.config
	timeoutConfig.Kafka.Timeouts.Request = 10 * time.Millisecond // Extremely short
	timeoutConfig.Kafka.Timeouts.Connection = 5 * time.Second    // Longer connection timeout

	// Use localhost to establish connection but trigger request timeout
	prod, err := producer.New(&timeoutConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Could not create producer (Kafka may not be running): %v", err)
		return // Skip test if Kafka is not available
	}
	defer prod.Close()

	message := &models.Message{
		Topic:     "timeout-test-topic",
		Key:       fmt.Sprintf("request-timeout-%d", time.Now().Unix()),
		Value:     []byte("Test message for request timeout"),
		Timestamp: time.Now(),
	}

	// Set context timeout longer than request timeout to isolate the behavior
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	receipt, err := prod.PublishMessage(ctx, message)
	elapsed := time.Since(start)

	if err != nil {
		suite.T().Logf("Request timed out as expected in %v: %v", elapsed, err)
		suite.Assert().Nil(receipt, "Receipt should be nil on timeout")

		// The timeout should be related to the request timeout configuration
		// (though exact timing may vary due to retries and Kafka behavior)
		suite.Assert().LessOrEqual(elapsed, 5*time.Second,
			"Should timeout relatively quickly with short request timeout")
	} else {
		suite.T().Logf("Request succeeded unexpectedly (Kafka may be very responsive)")
	}
}

// TestContextTimeout tests producer behavior with context-based timeouts
func (suite *TimeoutTestSuite) TestContextTimeout() {
	if testing.Short() {
		suite.T().Skip("Skipping context timeout test in short mode")
	}

	// Use reasonable producer timeouts but short context timeout
	prod, err := producer.New(suite.config, suite.logger)
	if err != nil {
		suite.T().Logf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	message := &models.Message{
		Topic:     "timeout-test-topic",
		Key:       fmt.Sprintf("context-timeout-%d", time.Now().Unix()),
		Value:     []byte("Test message for context timeout"),
		Timestamp: time.Now(),
	}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	start := time.Now()
	receipt, err := prod.PublishMessage(ctx, message)
	elapsed := time.Since(start)

	// Should get context deadline exceeded
	suite.Require().Error(err, "Should get error due to context timeout")
	suite.Assert().Equal(context.DeadlineExceeded, err, "Should get context deadline exceeded error")
	suite.Assert().Nil(receipt, "Receipt should be nil on context timeout")

	// Should timeout very quickly
	suite.Assert().LessOrEqual(elapsed, 100*time.Millisecond,
		"Context timeout should be very fast")

	suite.T().Logf("Context timeout occurred in %v as expected", elapsed)
}

// TestTimeoutWithRetries tests timeout behavior with retry mechanisms
func (suite *TimeoutTestSuite) TestTimeoutWithRetries() {
	if testing.Short() {
		suite.T().Skip("Skipping timeout with retries test in short mode")
	}

	// Configure for multiple retries with short timeouts
	retryConfig := *suite.config
	retryConfig.Kafka.Retry.MaxAttempts = 3
	retryConfig.Kafka.Retry.InitialBackoff = 100 * time.Millisecond
	retryConfig.Kafka.Timeouts.Request = 200 * time.Millisecond

	// Use non-existent broker to trigger timeouts
	retryConfig.Kafka.Brokers = []string{"localhost:19999"}

	prod, err := producer.New(&retryConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Expected error creating producer with unavailable broker: %v", err)
		return
	}
	defer prod.Close()

	message := &models.Message{
		Topic:     "timeout-retry-topic",
		Key:       fmt.Sprintf("timeout-retry-%d", time.Now().Unix()),
		Value:     []byte("Test message for timeout with retries"),
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	receipt, err := prod.PublishMessage(ctx, message)
	elapsed := time.Since(start)

	// Should eventually fail after retries
	suite.Require().Error(err, "Should fail after retry timeouts")
	suite.Assert().Nil(receipt, "Receipt should be nil after timeout failures")

	// Should take time proportional to retry attempts
	expectedMinTime := time.Duration(retryConfig.Kafka.Retry.MaxAttempts) * retryConfig.Kafka.Timeouts.Request
	suite.Assert().GreaterOrEqual(elapsed, expectedMinTime/2, // Allow some variance
		"Should take time for multiple retry attempts")

	suite.T().Logf("Timeout with retries took %v (expected >= %v)", elapsed, expectedMinTime/2)
}

// TestDeliveryTimeout tests end-to-end delivery timeout
func (suite *TimeoutTestSuite) TestDeliveryTimeout() {
	if testing.Short() {
		suite.T().Skip("Skipping delivery timeout test in short mode")
	}

	// Configure short delivery timeout
	deliveryConfig := *suite.config
	deliveryConfig.Kafka.Timeouts.Delivery = 1 * time.Second // Very short delivery timeout
	deliveryConfig.Kafka.Timeouts.Request = 500 * time.Millisecond
	deliveryConfig.Kafka.Retry.MaxAttempts = 2

	prod, err := producer.New(&deliveryConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	// Create a large message that might take time to deliver
	largePayload := make([]byte, 500*1024) // 500KB
	for i := range largePayload {
		largePayload[i] = byte('A' + (i % 26))
	}

	message := &models.Message{
		Topic: "timeout-test-topic",
		Key:   fmt.Sprintf("delivery-timeout-%d", time.Now().Unix()),
		Value: largePayload,
		Headers: map[string]string{
			"test-type": "delivery-timeout",
		},
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	receipt, err := prod.PublishMessage(ctx, message)
	elapsed := time.Since(start)

	if err != nil {
		suite.T().Logf("Delivery timed out as expected in %v: %v", elapsed, err)

		// Should be related to delivery timeout
		suite.Assert().LessOrEqual(elapsed, 5*time.Second,
			"Should timeout within reasonable time")
	} else {
		// If it succeeds, verify it's a proper success
		suite.Require().NotNil(receipt, "Receipt should be provided on success")
		suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status)
		suite.T().Logf("Large message delivered successfully in %v", elapsed)
	}
}

// TestConcurrentTimeouts tests timeout behavior with concurrent operations
func (suite *TimeoutTestSuite) TestConcurrentTimeouts() {
	if testing.Short() {
		suite.T().Skip("Skipping concurrent timeout test in short mode")
	}

	// Use configuration that may cause some timeouts
	concurrentConfig := *suite.config
	concurrentConfig.Kafka.Timeouts.Request = 500 * time.Millisecond
	concurrentConfig.Kafka.Retry.MaxAttempts = 2

	prod, err := producer.New(&concurrentConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	const numConcurrent = 5
	results := make([]error, numConcurrent)
	done := make(chan bool, numConcurrent)

	start := time.Now()

	// Launch concurrent operations with varying timeouts
	for i := 0; i < numConcurrent; i++ {
		go func(index int) {
			defer func() { done <- true }()

			message := &models.Message{
				Topic:     "concurrent-timeout-topic",
				Key:       fmt.Sprintf("concurrent-%d-%d", index, time.Now().Unix()),
				Value:     []byte(fmt.Sprintf("Concurrent timeout test message %d", index)),
				Timestamp: time.Now(),
			}

			// Vary context timeouts
			timeoutDuration := time.Duration(100+index*200) * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
			defer cancel()

			_, err := prod.PublishMessage(ctx, message)
			results[index] = err
		}(i)
	}

	// Wait for all operations
	for i := 0; i < numConcurrent; i++ {
		select {
		case <-done:
			// Operation completed
		case <-time.After(30 * time.Second):
			suite.T().Fatal("Concurrent timeout test took too long")
		}
	}

	elapsed := time.Since(start)

	// Analyze results
	timeouts := 0
	successes := 0
	for i, err := range results {
		if err != nil {
			if err == context.DeadlineExceeded {
				timeouts++
				suite.T().Logf("Operation %d timed out as expected", i)
			} else {
				suite.T().Logf("Operation %d failed with error: %v", i, err)
			}
		} else {
			successes++
			suite.T().Logf("Operation %d succeeded", i)
		}
	}

	suite.T().Logf("Concurrent operations completed in %v: %d timeouts, %d successes",
		elapsed, timeouts, successes)

	// We expect at least some variation in results due to different timeout values
	suite.Assert().GreaterOrEqual(timeouts+successes, numConcurrent,
		"All operations should complete (either timeout or succeed)")
}

// TestTimeoutRecovery tests recovery after timeout scenarios
func (suite *TimeoutTestSuite) TestTimeoutRecovery() {
	if testing.Short() {
		suite.T().Skip("Skipping timeout recovery test in short mode")
	}

	prod, err := producer.New(suite.config, suite.logger)
	if err != nil {
		suite.T().Logf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	// Phase 1: Cause timeout with very short context
	timeoutMessage := &models.Message{
		Topic:     "recovery-test-topic",
		Key:       "timeout-phase",
		Value:     []byte("Message that should timeout"),
		Timestamp: time.Now(),
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	_, err = prod.PublishMessage(shortCtx, timeoutMessage)
	cancel()

	suite.Require().Error(err, "Should timeout with very short context")
	suite.Assert().Equal(context.DeadlineExceeded, err, "Should be context deadline exceeded")

	// Phase 2: Verify recovery with normal timeout
	time.Sleep(100 * time.Millisecond) // Brief pause

	recoveryMessage := &models.Message{
		Topic:     "recovery-test-topic",
		Key:       "recovery-phase",
		Value:     []byte("Message after timeout recovery"),
		Timestamp: time.Now(),
	}

	normalCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	receipt, err := prod.PublishMessage(normalCtx, recoveryMessage)

	if err != nil {
		suite.T().Logf("Recovery message failed (may indicate Kafka unavailability): %v", err)
	} else {
		suite.Assert().NotNil(receipt, "Should get receipt after recovery")
		suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status,
			"Should succeed after timeout recovery")
		suite.T().Logf("Successfully recovered from timeout scenario")
	}
}

// Run the test suite
func TestNetworkTimeouts(t *testing.T) {
	// Skip integration tests if not running in integration environment
	if testing.Short() {
		t.Skip("Skipping network timeout integration tests in short mode")
	}

	suite.Run(t, new(TimeoutTestSuite))
}

// Benchmark timeout handling performance
func BenchmarkTimeoutHandling(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping timeout benchmark in short mode")
	}

	config := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Producer: config.ProducerConfig{
				BatchSize:       1,
				MaxMessageBytes: 1048576,
				FlushFrequency:  50 * time.Millisecond,
				Compression:     "none",
			},
			Timeouts: config.TimeoutConfig{
				Connection: 1 * time.Second,
				Request:    2 * time.Second,
				Delivery:   5 * time.Second,
			},
		},
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		b.Fatal(err)
	}

	prod, err := producer.New(config, logger)
	if err != nil {
		b.Skip("Could not create producer for benchmark")
	}
	defer prod.Close()

	message := &models.Message{
		Topic:     "benchmark-timeout-topic",
		Key:       "benchmark-key",
		Value:     []byte("Benchmark timeout message"),
		Timestamp: time.Now(),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		prod.PublishMessage(ctx, message)
		cancel()
	}
}
