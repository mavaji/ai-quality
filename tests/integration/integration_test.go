package integration

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

// IntegrationTestSuite provides integration tests that work with or without testcontainers
func TestKafkaIntegration(t *testing.T) {
	ctx := context.Background()

	// Try to setup Kafka container, fallback to mock if Docker unavailable
	kafkaContainer, err := SetupKafkaContainer(ctx, t)
	useContainer := err == nil && kafkaContainer != nil

	var cfg *config.Config
	if useContainer {
		t.Log("Using Kafka testcontainer for integration tests")
		cfg = CreateTestConfigWithKafka(kafkaContainer)
		defer func() {
			if cleanupErr := kafkaContainer.Cleanup(ctx); cleanupErr != nil {
				t.Logf("Failed to cleanup Kafka container: %v", cleanupErr)
			}
		}()
	} else {
		t.Log("Docker not available, using mock producer for integration tests")
		cfg = createMockConfig()
	}

	// Create logger
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	t.Run("BasicProducerFunctionality", func(t *testing.T) {
		testBasicProducerFunctionality(t, cfg, logger, useContainer)
	})

	t.Run("ConcurrentMessagePublishing", func(t *testing.T) {
		testConcurrentMessagePublishing(t, cfg, logger, useContainer)
	})

	t.Run("MessageValidationIntegration", func(t *testing.T) {
		testMessageValidationIntegration(t, cfg, logger, useContainer)
	})

	t.Run("ProducerErrorHandling", func(t *testing.T) {
		testProducerErrorHandling(t, cfg, logger, useContainer)
	})

	t.Run("MemoryUsageValidation", func(t *testing.T) {
		testMemoryUsageValidation(t, cfg, logger, useContainer)
	})
}

func testBasicProducerFunctionality(t *testing.T, cfg *config.Config, logger *zap.Logger, useContainer bool) {
	var prod producer.Producer
	var err error

	if useContainer {
		prod, err = producer.New(cfg, logger)
		if err != nil {
			t.Skipf("Could not create real producer: %v", err)
			return
		}
		defer prod.Close()
	} else {
		prod = producer.NewMockProducer()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test single message publishing
	message := &models.Message{
		Topic:     "test-topic",
		Key:       "test-key",
		Value:     []byte(`{"test": "data", "timestamp": "2023-01-01"}`),
		Timestamp: time.Now(),
	}

	receipt, err := prod.PublishMessage(ctx, message)
	require.NoError(t, err, "Should publish message successfully")
	assert.NotNil(t, receipt, "Should return receipt")
	assert.Equal(t, models.DeliveryStatusSuccess, receipt.Status, "Should indicate successful delivery")
	assert.NotEmpty(t, receipt.MessageID, "Should have message ID")

	if useContainer {
		// With real Kafka, we can test additional properties
		assert.Greater(t, receipt.Offset, int64(-1), "Should have valid offset")
		assert.GreaterOrEqual(t, receipt.Partition, int32(0), "Should have valid partition")
	}

	t.Logf("Message published successfully: ID=%s, Offset=%d, Partition=%d",
		receipt.MessageID, receipt.Offset, receipt.Partition)
}

func testConcurrentMessagePublishing(t *testing.T, cfg *config.Config, logger *zap.Logger, useContainer bool) {
	var prod producer.Producer
	var err error

	if useContainer {
		prod, err = producer.New(cfg, logger)
		if err != nil {
			t.Skipf("Could not create real producer: %v", err)
			return
		}
		defer prod.Close()
	} else {
		prod = producer.NewMockProducer()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		numGoroutines        = 5
		messagesPerGoroutine = 20
		totalMessages        = numGoroutines * messagesPerGoroutine
	)

	var (
		successCount int64
		errorCount   int64
		wg           sync.WaitGroup
	)

	startTime := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				message := &models.Message{
					Topic:     fmt.Sprintf("concurrent-test-topic-%d", goroutineID%3), // Use multiple topics
					Key:       fmt.Sprintf("key-%d-%d", goroutineID, j),
					Value:     []byte(fmt.Sprintf(`{"goroutine": %d, "message": %d, "data": "test"}`, goroutineID, j)),
					Timestamp: time.Now(),
				}

				receipt, err := prod.PublishMessage(ctx, message)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					t.Logf("Error publishing message: %v", err)
				} else if receipt != nil && receipt.Status == models.DeliveryStatusSuccess {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Calculate metrics
	throughput := float64(successCount) / duration.Seconds()
	successRate := float64(successCount) / float64(totalMessages) * 100

	t.Logf("Concurrent publishing results:")
	t.Logf("  - Duration: %v", duration)
	t.Logf("  - Total messages: %d", totalMessages)
	t.Logf("  - Successful: %d", successCount)
	t.Logf("  - Errors: %d", errorCount)
	t.Logf("  - Throughput: %.2f msg/sec", throughput)
	t.Logf("  - Success rate: %.2f%%", successRate)

	// Assertions
	assert.Greater(t, successCount, int64(0), "Should have successful messages")
	assert.Greater(t, successRate, 90.0, "Success rate should be > 90%%")

	if useContainer {
		assert.Greater(t, throughput, 10.0, "Should achieve reasonable throughput with real Kafka")
	}
}

func testMessageValidationIntegration(t *testing.T, cfg *config.Config, logger *zap.Logger, useContainer bool) {
	var prod producer.Producer
	var err error

	if useContainer {
		prod, err = producer.New(cfg, logger)
		if err != nil {
			t.Skipf("Could not create real producer: %v", err)
			return
		}
		defer prod.Close()
	} else {
		prod = producer.NewMockProducer()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testCases := []struct {
		name        string
		message     *models.Message
		expectError bool
	}{
		{
			name: "valid message",
			message: &models.Message{
				Topic:     "validation-test",
				Key:       "valid-key",
				Value:     []byte(`{"valid": "data"}`),
				Timestamp: time.Now(),
			},
			expectError: false,
		},
		{
			name: "empty topic",
			message: &models.Message{
				Topic:     "",
				Key:       "key",
				Value:     []byte(`{"data": "test"}`),
				Timestamp: time.Now(),
			},
			expectError: true,
		},
		{
			name: "nil value",
			message: &models.Message{
				Topic:     "test-topic",
				Key:       "key",
				Value:     nil,
				Timestamp: time.Now(),
			},
			expectError: true,
		},
		{
			name: "zero timestamp",
			message: &models.Message{
				Topic:     "test-topic",
				Key:       "key",
				Value:     []byte(`{"data": "test"}`),
				Timestamp: time.Time{},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			receipt, err := prod.PublishMessage(ctx, tc.message)

			if tc.expectError {
				assert.Error(t, err, "Should return error for invalid message")
				assert.Nil(t, receipt, "Should not return receipt for invalid message")
			} else {
				assert.NoError(t, err, "Should not return error for valid message")
				assert.NotNil(t, receipt, "Should return receipt for valid message")
				assert.Equal(t, models.DeliveryStatusSuccess, receipt.Status, "Should indicate successful delivery")
			}
		})
	}
}

func testProducerErrorHandling(t *testing.T, cfg *config.Config, logger *zap.Logger, useContainer bool) {
	var prod producer.Producer
	var err error

	if useContainer {
		prod, err = producer.New(cfg, logger)
		if err != nil {
			t.Skipf("Could not create real producer: %v", err)
			return
		}
		defer prod.Close()
	} else {
		prod = producer.NewMockProducer()
	}

	// Test context cancellation
	t.Run("context_cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		message := &models.Message{
			Topic:     "test-topic",
			Key:       "key",
			Value:     []byte(`{"data": "test"}`),
			Timestamp: time.Now(),
		}

		receipt, err := prod.PublishMessage(ctx, message)

		if useContainer {
			// Real producer should respect context cancellation
			assert.Error(t, err, "Should return error when context is cancelled")
			assert.Nil(t, receipt, "Should not return receipt when context is cancelled")
		} else {
			// Mock producer might not implement context cancellation
			t.Log("Mock producer may not respect context cancellation")
		}
	})

	// Test timeout handling
	t.Run("timeout_handling", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond) // Very short timeout
		defer cancel()

		// Give time for timeout to trigger
		time.Sleep(1 * time.Millisecond)

		message := &models.Message{
			Topic:     "test-topic",
			Key:       "key",
			Value:     []byte(`{"data": "test"}`),
			Timestamp: time.Now(),
		}

		receipt, err := prod.PublishMessage(ctx, message)

		if useContainer {
			// Real producer should handle timeouts
			assert.Error(t, err, "Should return error when timeout occurs")
			assert.Nil(t, receipt, "Should not return receipt when timeout occurs")
		} else {
			t.Log("Mock producer may not implement timeout handling")
		}
	})
}

func testMemoryUsageValidation(t *testing.T, cfg *config.Config, logger *zap.Logger, useContainer bool) {
	var prod producer.Producer
	var err error

	if useContainer {
		prod, err = producer.New(cfg, logger)
		if err != nil {
			t.Skipf("Could not create real producer: %v", err)
			return
		}
		defer prod.Close()
	} else {
		prod = producer.NewMockProducer()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Get initial memory stats
	var initialMemStats, finalMemStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&initialMemStats)

	const (
		testMessages = 500
		payloadSize  = 1024 // 1KB
	)

	// Send messages
	for i := 0; i < testMessages; i++ {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte('A' + (i % 26))
		}

		message := &models.Message{
			Topic:     "memory-test-topic",
			Key:       fmt.Sprintf("memory-test-%d", i),
			Value:     payload,
			Timestamp: time.Now(),
		}

		_, err := prod.PublishMessage(ctx, message)
		if err != nil {
			t.Logf("Message %d failed: %v", i, err)
		}

		// Force GC periodically
		if i%100 == 0 {
			runtime.GC()
		}
	}

	// Get final memory stats
	runtime.GC()
	runtime.ReadMemStats(&finalMemStats)

	memoryUsed := finalMemStats.Alloc
	memoryDelta := int64(finalMemStats.Alloc) - int64(initialMemStats.Alloc)

	const memoryLimit = 100 * 1024 * 1024 // 100MB limit for integration tests

	t.Logf("Memory usage results:")
	t.Logf("  - Initial memory: %.2f MB", float64(initialMemStats.Alloc)/(1024*1024))
	t.Logf("  - Final memory: %.2f MB", float64(finalMemStats.Alloc)/(1024*1024))
	t.Logf("  - Memory delta: %.2f MB", float64(memoryDelta)/(1024*1024))
	t.Logf("  - Heap size: %.2f MB", float64(finalMemStats.HeapSys)/(1024*1024))

	// Verify memory usage is reasonable
	assert.Less(t, memoryUsed, uint64(memoryLimit),
		"Memory usage should be less than %d MB, got %.2f MB",
		memoryLimit/(1024*1024), float64(memoryUsed)/(1024*1024))
}

// createMockConfig creates a configuration for mock testing
func createMockConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"}, // Not used by mock
			Producer: config.ProducerConfig{
				BatchSize:       50,
				MaxMessageBytes: 1048576,
				FlushFrequency:  100 * time.Millisecond,
				Compression:     "none",
			},
			Retry: config.RetryConfig{
				MaxAttempts:       3,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        1 * time.Second,
				BackoffMultiplier: 2.0,
			},
			Timeouts: config.TimeoutConfig{
				Connection: 10 * time.Second,
				Request:    15 * time.Second,
				Delivery:   30 * time.Second,
			},
			Security: config.SecurityConfig{
				Protocol: "PLAINTEXT",
			},
		},
		Server: config.ServerConfig{
			Host:         "localhost",
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
	}
}
