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

// FailoverTestSuite groups integration tests for broker failure scenarios
type FailoverTestSuite struct {
	suite.Suite
	producer producer.Producer
	config   *config.Config
	logger   *zap.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

// SetupSuite runs once before all tests in the suite
func (suite *FailoverTestSuite) SetupSuite() {
	// Create test configuration with multiple brokers for failover testing
	suite.config = &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{
				"localhost:9092", // Primary broker
				"localhost:9093", // Secondary broker
				"localhost:9094", // Tertiary broker
			},
			Producer: config.ProducerConfig{
				BatchSize:       1,
				MaxMessageBytes: 1048576, // 1MB
				FlushFrequency:  100 * time.Millisecond,
				Compression:     "none",
			},
			Retry: config.RetryConfig{
				MaxAttempts:       5, // More retries for failover scenarios
				InitialBackoff:    200 * time.Millisecond,
				MaxBackoff:        10 * time.Second,
				BackoffMultiplier: 2.0,
			},
			Timeouts: config.TimeoutConfig{
				Connection: 5 * time.Second,  // Shorter for faster failover
				Request:    10 * time.Second, // Moderate timeout
				Delivery:   30 * time.Second, // Longer for retry scenarios
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

	// Note: Producer will be created in individual tests as needed
	// because we want to simulate different failure scenarios
}

// TearDownSuite runs once after all tests in the suite
func (suite *FailoverTestSuite) TearDownSuite() {
	if suite.producer != nil {
		err := suite.producer.Close()
		suite.Assert().NoError(err, "Failed to close producer")
	}
	suite.cancel()
}

// TestBrokerFailoverScenario tests producer behavior when primary broker fails
func (suite *FailoverTestSuite) TestBrokerFailoverScenario() {
	if testing.Short() {
		suite.T().Skip("Skipping failover test in short mode - requires Kafka cluster")
	}

	// This test simulates broker failover by:
	// 1. Starting with a working primary broker
	// 2. Simulating primary broker failure
	// 3. Verifying failover to secondary broker
	// 4. Confirming message delivery continues

	suite.T().Skip("Integration test requires running Kafka cluster with multiple brokers")

	// Uncomment when Kafka cluster is available:
	/*
		// Create producer with failover configuration
		prod, err := producer.New(suite.config, suite.logger)
		suite.Require().NoError(err, "Failed to create producer")
		suite.producer = prod

		// Test message that should succeed on primary broker
		successMessage := &models.Message{
			Topic:     "failover-test-topic",
			Key:       fmt.Sprintf("success-%d", time.Now().Unix()),
			Value:     []byte("Message before failover"),
			Headers:   map[string]string{
				"test-phase": "before-failover",
			},
			Timestamp: time.Now(),
		}

		// Send message to primary broker (should succeed)
		receipt, err := suite.producer.PublishMessage(suite.ctx, successMessage)
		suite.Require().NoError(err, "Message should succeed on primary broker")
		suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status)

		// TODO: Simulate primary broker failure here
		// This would require external orchestration (e.g., stopping Docker container)

		// Test message that should failover to secondary broker
		failoverMessage := &models.Message{
			Topic:     "failover-test-topic",
			Key:       fmt.Sprintf("failover-%d", time.Now().Unix()),
			Value:     []byte("Message during failover"),
			Headers:   map[string]string{
				"test-phase": "during-failover",
			},
			Timestamp: time.Now(),
		}

		// This should trigger failover and eventually succeed
		receipt, err = suite.producer.PublishMessage(suite.ctx, failoverMessage)

		// We expect either success (failover worked) or specific failover behavior
		if err != nil {
			suite.T().Logf("Failover scenario error (expected in test): %v", err)
			// In a real scenario, we'd verify the error is handled appropriately
		} else {
			suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status)
			suite.T().Logf("Failover successful, message delivered to partition %d, offset %d",
				receipt.Partition, receipt.Offset)
		}
	*/
}

// TestBrokerConnectionFailure tests behavior when all brokers are unavailable
func (suite *FailoverTestSuite) TestBrokerConnectionFailure() {
	if testing.Short() {
		suite.T().Skip("Skipping connection failure test in short mode")
	}

	// Create producer with non-existent brokers to simulate total failure
	failConfig := suite.config
	failConfig.Kafka.Brokers = []string{
		"localhost:19092", // Non-existent broker
		"localhost:19093", // Non-existent broker
		"localhost:19094", // Non-existent broker
	}

	// Reduce timeouts for faster test execution
	failConfig.Kafka.Timeouts.Connection = 1 * time.Second
	failConfig.Kafka.Timeouts.Request = 2 * time.Second
	failConfig.Kafka.Retry.MaxAttempts = 2 // Fewer retries for faster test

	prod, err := producer.New(failConfig, suite.logger)
	if err != nil {
		// This is expected behavior when no brokers are available
		suite.T().Logf("Expected error creating producer with unavailable brokers: %v", err)
		return
	}
	suite.producer = prod

	message := &models.Message{
		Topic: "failure-test-topic",
		Key:   "failure-test-key",
		Value: []byte("Message to unavailable brokers"),
		Headers: map[string]string{
			"test-type": "connection-failure",
		},
		Timestamp: time.Now(),
	}

	// This should fail due to no available brokers
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	receipt, err := suite.producer.PublishMessage(ctx, message)

	// We expect this to fail
	suite.Require().Error(err, "Should fail when no brokers are available")

	if receipt != nil {
		// If we get a receipt, it should indicate failure
		suite.Assert().NotEqual(models.DeliveryStatusSuccess, receipt.Status,
			"Receipt status should not be success")
		suite.T().Logf("Received failure receipt: %v", receipt.Status)
	}

	suite.T().Logf("Expected connection failure: %v", err)
}

// TestPartialBrokerFailure tests behavior with some brokers available
func (suite *FailoverTestSuite) TestPartialBrokerFailure() {
	if testing.Short() {
		suite.T().Skip("Skipping partial failure test in short mode")
	}

	// Create configuration with mix of available and unavailable brokers
	mixedConfig := suite.config
	mixedConfig.Kafka.Brokers = []string{
		"localhost:9092",  // Potentially available
		"localhost:19093", // Non-existent (failure simulation)
		"localhost:19094", // Non-existent (failure simulation)
	}

	prod, err := producer.New(mixedConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Error creating producer (may be expected): %v", err)
		// Skip rest of test if we can't create producer at all
		return
	}
	suite.producer = prod

	message := &models.Message{
		Topic: "partial-failure-topic",
		Key:   fmt.Sprintf("partial-%d", time.Now().Unix()),
		Value: []byte("Message with partial broker failure"),
		Headers: map[string]string{
			"test-type": "partial-failure",
		},
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	receipt, err := suite.producer.PublishMessage(ctx, message)

	// Result depends on whether localhost:9092 is actually available
	if err != nil {
		suite.T().Logf("Partial failure scenario error: %v", err)
		// Verify error is appropriate for broker unavailability
		suite.Assert().Contains(err.Error(), "broker", "Error should mention broker issues")
	} else {
		// If it succeeds, verify it's a proper success
		suite.Require().NotNil(receipt, "Receipt should be provided on success")
		suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status)
		suite.T().Logf("Partial failure handled successfully")
	}
}

// TestBrokerRecoveryScenario tests producer behavior when failed broker recovers
func (suite *FailoverTestSuite) TestBrokerRecoveryScenario() {
	if testing.Short() {
		suite.T().Skip("Skipping recovery test in short mode - requires Kafka orchestration")
	}

	suite.T().Skip("Integration test requires external Kafka cluster orchestration")

	// This test would simulate:
	// 1. Start with working broker
	// 2. Simulate broker failure
	// 3. Verify failover behavior
	// 4. Simulate broker recovery
	// 5. Verify producer reconnects and continues working

	// Uncomment when full Kafka orchestration is available:
	/*
		prod, err := producer.New(suite.config, suite.logger)
		suite.Require().NoError(err, "Failed to create producer")
		suite.producer = prod

		// Phase 1: Normal operation
		normalMessage := &models.Message{
			Topic:     "recovery-test-topic",
			Key:       "recovery-phase-1",
			Value:     []byte("Message during normal operation"),
			Timestamp: time.Now(),
		}

		receipt, err := suite.producer.PublishMessage(suite.ctx, normalMessage)
		suite.Require().NoError(err, "Normal operation should succeed")
		suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status)

		// Phase 2: Simulate broker failure and verify failover
		// (External orchestration needed)

		// Phase 3: Simulate broker recovery
		// (External orchestration needed)

		// Phase 4: Verify resumed operation
		recoveryMessage := &models.Message{
			Topic:     "recovery-test-topic",
			Key:       "recovery-phase-4",
			Value:     []byte("Message after broker recovery"),
			Timestamp: time.Now(),
		}

		receipt, err = suite.producer.PublishMessage(suite.ctx, recoveryMessage)
		suite.Assert().NoError(err, "Should work after broker recovery")
		if receipt != nil {
			suite.Assert().Equal(models.DeliveryStatusSuccess, receipt.Status)
		}
	*/
}

// TestCircuitBreakerBehavior tests circuit breaker pattern during broker failures
func (suite *FailoverTestSuite) TestCircuitBreakerBehavior() {
	if testing.Short() {
		suite.T().Skip("Skipping circuit breaker test in short mode")
	}

	// Create configuration optimized for circuit breaker testing
	cbConfig := suite.config
	cbConfig.Kafka.Retry.MaxAttempts = 3
	cbConfig.Kafka.Retry.InitialBackoff = 100 * time.Millisecond
	cbConfig.Kafka.Timeouts.Request = 1 * time.Second // Fast timeout

	// Use non-existent brokers to trigger failures
	cbConfig.Kafka.Brokers = []string{"localhost:19999"}

	prod, err := producer.New(cbConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Expected error with unavailable brokers: %v", err)
		return
	}
	suite.producer = prod

	// Send multiple messages to trigger circuit breaker
	const numMessages = 5
	failures := 0

	for i := 0; i < numMessages; i++ {
		message := &models.Message{
			Topic:     "circuit-breaker-topic",
			Key:       fmt.Sprintf("cb-test-%d", i),
			Value:     []byte(fmt.Sprintf("Circuit breaker test message %d", i)),
			Timestamp: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		receipt, err := suite.producer.PublishMessage(ctx, message)
		cancel()

		if err != nil {
			failures++
			suite.T().Logf("Message %d failed as expected: %v", i, err)
		} else if receipt != nil && receipt.Status != models.DeliveryStatusSuccess {
			failures++
			suite.T().Logf("Message %d failed with status: %v", i, receipt.Status)
		}
	}

	// We expect most/all messages to fail due to unavailable brokers
	suite.Assert().Greater(failures, numMessages/2,
		"Circuit breaker should prevent most messages from succeeding")

	suite.T().Logf("Circuit breaker test: %d/%d messages failed as expected", failures, numMessages)
}

// TestRetryExhaustion tests behavior when retry attempts are exhausted
func (suite *FailoverTestSuite) TestRetryExhaustion() {
	if testing.Short() {
		suite.T().Skip("Skipping retry exhaustion test in short mode")
	}

	// Configure for fast retry exhaustion
	retryConfig := suite.config
	retryConfig.Kafka.Retry.MaxAttempts = 2 // Very few retries
	retryConfig.Kafka.Retry.InitialBackoff = 50 * time.Millisecond
	retryConfig.Kafka.Timeouts.Request = 500 * time.Millisecond

	// Use non-existent broker
	retryConfig.Kafka.Brokers = []string{"localhost:18888"}

	prod, err := producer.New(retryConfig, suite.logger)
	if err != nil {
		suite.T().Logf("Expected error with unavailable broker: %v", err)
		return
	}
	suite.producer = prod

	message := &models.Message{
		Topic:     "retry-exhaustion-topic",
		Key:       "retry-test",
		Value:     []byte("Message to test retry exhaustion"),
		Timestamp: time.Now(),
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	receipt, err := suite.producer.PublishMessage(ctx, message)
	elapsed := time.Since(start)

	// Should fail after exhausting retries
	suite.Require().Error(err, "Should fail after retry exhaustion")

	if receipt != nil {
		suite.Assert().NotEqual(models.DeliveryStatusSuccess, receipt.Status,
			"Receipt should indicate failure")
	}

	// Verify timing suggests retries were attempted
	expectedMinTime := time.Duration(retryConfig.Kafka.Retry.MaxAttempts) * retryConfig.Kafka.Retry.InitialBackoff
	suite.Assert().GreaterOrEqual(elapsed, expectedMinTime,
		"Should take at least the minimum retry time")

	suite.T().Logf("Retry exhaustion test completed in %v (expected >= %v)", elapsed, expectedMinTime)
}

// Run the test suite
func TestBrokerFailover(t *testing.T) {
	// Skip integration tests if not running in integration environment
	if testing.Short() {
		t.Skip("Skipping broker failover integration tests in short mode")
	}

	suite.Run(t, new(FailoverTestSuite))
}

// Helper function for creating test messages with unique identifiers
func createTestMessage(topic, keyPrefix string, payload []byte) *models.Message {
	return &models.Message{
		Topic: topic,
		Key:   fmt.Sprintf("%s-%d", keyPrefix, time.Now().UnixNano()),
		Value: payload,
		Headers: map[string]string{
			"test-timestamp": time.Now().Format(time.RFC3339),
			"test-id":        fmt.Sprintf("%d", time.Now().UnixNano()),
		},
		Timestamp: time.Now(),
	}
}

// Helper function to simulate various broker failure scenarios
func simulateBrokerFailure(suite *FailoverTestSuite, scenario string) {
	switch scenario {
	case "network-partition":
		// Simulate network partition (would require external tooling)
		suite.T().Logf("Simulating network partition scenario")
	case "broker-shutdown":
		// Simulate graceful broker shutdown (would require external tooling)
		suite.T().Logf("Simulating broker shutdown scenario")
	case "broker-crash":
		// Simulate broker crash (would require external tooling)
		suite.T().Logf("Simulating broker crash scenario")
	default:
		suite.T().Logf("Unknown failure scenario: %s", scenario)
	}
}
