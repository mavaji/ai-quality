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
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

// PerformanceTestSuite groups performance tests for throughput targets
type PerformanceTestSuite struct {
	suite.Suite
	config *config.Config
	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// SetupSuite runs once before all tests in the suite
func (suite *PerformanceTestSuite) SetupSuite() {
	// Create optimized configuration for performance testing
	suite.config = &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{
				"localhost:9092", // Primary broker
			},
			Producer: config.ProducerConfig{
				BatchSize:       100,                   // Large batches for throughput
				MaxMessageBytes: 1048576,               // 1MB
				FlushFrequency:  10 * time.Millisecond, // Fast flush
				Compression:     "lz4",                 // Fast compression
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
	}

	// Create logger
	logger, err := zap.NewProduction() // Use production logger for performance
	suite.Require().NoError(err, "Failed to create logger")
	suite.logger = logger

	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), 10*time.Minute)
}

// TearDownSuite runs once after all tests in the suite
func (suite *PerformanceTestSuite) TearDownSuite() {
	suite.cancel()
}

// TestThroughputTarget tests that we can achieve 10,000+ msg/sec throughput
func (suite *PerformanceTestSuite) TestThroughputTarget() {
	if testing.Short() {
		suite.T().Skip("Skipping throughput test in short mode")
	}

	prod, err := producer.New(suite.config, suite.logger)
	if err != nil {
		suite.T().Skipf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	const (
		targetThroughput = 10000 // messages per second
		testDuration     = 10    // seconds
		totalMessages    = targetThroughput * testDuration
	)

	suite.T().Logf("Starting throughput test: %d messages over %d seconds (target: %d msg/sec)",
		totalMessages, testDuration, targetThroughput)

	var (
		sentCount    int64
		successCount int64
		errorCount   int64
		wg           sync.WaitGroup
		startTime    = time.Now()
	)

	// Create test context with timeout
	testCtx, cancel := context.WithTimeout(context.Background(), time.Duration(testDuration+30)*time.Second)
	defer cancel()

	// Launch message sending goroutines
	numGoroutines := 10
	messagesPerGoroutine := totalMessages / numGoroutines

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				message := &models.Message{
					Topic:     "performance-test-topic",
					Key:       fmt.Sprintf("perf-%d-%d", goroutineID, j),
					Value:     generateTestPayload(256), // 256 byte payload
					Timestamp: time.Now(),
				}

				receipt, err := prod.PublishMessage(testCtx, message)
				atomic.AddInt64(&sentCount, 1)

				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					if atomic.LoadInt64(&errorCount) <= 10 { // Log first 10 errors
						suite.T().Logf("Send error: %v", err)
					}
				} else if receipt != nil && receipt.Status == models.DeliveryStatusSuccess {
					atomic.AddInt64(&successCount, 1)
				}

				// Check if we should stop due to timeout
				if testCtx.Err() != nil {
					break
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	elapsed := time.Since(startTime)

	// Calculate metrics
	actualThroughput := float64(sentCount) / elapsed.Seconds()
	successRate := float64(successCount) / float64(sentCount) * 100
	errorRate := float64(errorCount) / float64(sentCount) * 100

	suite.T().Logf("Performance test results:")
	suite.T().Logf("  - Duration: %v", elapsed)
	suite.T().Logf("  - Messages sent: %d", sentCount)
	suite.T().Logf("  - Messages successful: %d", successCount)
	suite.T().Logf("  - Messages failed: %d", errorCount)
	suite.T().Logf("  - Throughput: %.2f msg/sec", actualThroughput)
	suite.T().Logf("  - Success rate: %.2f%%", successRate)
	suite.T().Logf("  - Error rate: %.2f%%", errorRate)

	// Verify throughput target (allow some tolerance for test environment)
	minThroughput := float64(targetThroughput) * 0.8 // Allow 20% tolerance
	suite.Assert().Greater(actualThroughput, minThroughput,
		"Throughput should be at least %.0f msg/sec (80%% of target), got %.2f", minThroughput, actualThroughput)

	// Verify error rate is acceptable (< 1%)
	suite.Assert().Less(errorRate, 1.0, "Error rate should be less than 1%%")
}

// TestLatencyTarget tests that p95 latency is under 100ms
func (suite *PerformanceTestSuite) TestLatencyTarget() {
	if testing.Short() {
		suite.T().Skip("Skipping latency test in short mode")
	}

	prod, err := producer.New(suite.config, suite.logger)
	if err != nil {
		suite.T().Skipf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	const (
		numMessages      = 1000
		targetP95Latency = 100 * time.Millisecond
	)

	suite.T().Logf("Starting latency test: %d messages (target p95 < %v)", numMessages, targetP95Latency)

	latencies := make([]time.Duration, 0, numMessages)
	var mu sync.Mutex

	// Send messages and measure latency
	var wg sync.WaitGroup
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(messageID int) {
			defer wg.Done()

			message := &models.Message{
				Topic:     "latency-test-topic",
				Key:       fmt.Sprintf("latency-%d", messageID),
				Value:     generateTestPayload(128), // 128 byte payload
				Timestamp: time.Now(),
			}

			start := time.Now()
			receipt, err := prod.PublishMessage(suite.ctx, message)
			latency := time.Since(start)

			if err == nil && receipt != nil && receipt.Status == models.DeliveryStatusSuccess {
				mu.Lock()
				latencies = append(latencies, latency)
				mu.Unlock()
			} else if err != nil {
				suite.T().Logf("Message %d failed: %v", messageID, err)
			}
		}(i)

		// Pace the sending to avoid overwhelming
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	wg.Wait()

	// Calculate latency percentiles
	suite.Require().Greater(len(latencies), numMessages/2, "At least 50%% of messages should succeed")

	// Sort latencies for percentile calculation
	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)

	// Simple selection sort for test purposes
	for i := 0; i < len(sortedLatencies); i++ {
		minIdx := i
		for j := i + 1; j < len(sortedLatencies); j++ {
			if sortedLatencies[j] < sortedLatencies[minIdx] {
				minIdx = j
			}
		}
		sortedLatencies[i], sortedLatencies[minIdx] = sortedLatencies[minIdx], sortedLatencies[i]
	}

	// Calculate percentiles
	p50Index := int(float64(len(sortedLatencies)) * 0.50)
	p95Index := int(float64(len(sortedLatencies)) * 0.95)
	p99Index := int(float64(len(sortedLatencies)) * 0.99)

	p50Latency := sortedLatencies[p50Index]
	p95Latency := sortedLatencies[p95Index]
	p99Latency := sortedLatencies[p99Index]

	suite.T().Logf("Latency test results:")
	suite.T().Logf("  - Successful messages: %d", len(latencies))
	suite.T().Logf("  - p50 latency: %v", p50Latency)
	suite.T().Logf("  - p95 latency: %v", p95Latency)
	suite.T().Logf("  - p99 latency: %v", p99Latency)

	// Verify p95 latency target
	suite.Assert().Less(p95Latency, targetP95Latency,
		"p95 latency should be less than %v, got %v", targetP95Latency, p95Latency)
}

// TestBurstThroughput tests burst capacity of 50,000 msg/sec
func (suite *PerformanceTestSuite) TestBurstThroughput() {
	if testing.Short() {
		suite.T().Skip("Skipping burst test in short mode")
	}

	prod, err := producer.New(suite.config, suite.logger)
	if err != nil {
		suite.T().Skipf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	const (
		burstTarget   = 50000 // messages per second
		burstDuration = 2     // seconds
		totalMessages = burstTarget * burstDuration
	)

	suite.T().Logf("Starting burst test: %d messages over %d seconds (target: %d msg/sec)",
		totalMessages, burstDuration, burstTarget)

	var (
		sentCount    int64
		successCount int64
		startTime    = time.Now()
	)

	// Create test context
	testCtx, cancel := context.WithTimeout(context.Background(), time.Duration(burstDuration+10)*time.Second)
	defer cancel()

	// Use more goroutines for burst test
	numGoroutines := 50
	messagesPerGoroutine := totalMessages / numGoroutines

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				message := &models.Message{
					Topic:     "burst-test-topic",
					Key:       fmt.Sprintf("burst-%d-%d", goroutineID, j),
					Value:     generateTestPayload(64), // Smaller payload for burst
					Timestamp: time.Now(),
				}

				receipt, err := prod.PublishMessage(testCtx, message)
				atomic.AddInt64(&sentCount, 1)

				if err == nil && receipt != nil && receipt.Status == models.DeliveryStatusSuccess {
					atomic.AddInt64(&successCount, 1)
				}

				if testCtx.Err() != nil {
					break
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	// Calculate burst metrics
	actualThroughput := float64(sentCount) / elapsed.Seconds()
	successRate := float64(successCount) / float64(sentCount) * 100

	suite.T().Logf("Burst test results:")
	suite.T().Logf("  - Duration: %v", elapsed)
	suite.T().Logf("  - Messages sent: %d", sentCount)
	suite.T().Logf("  - Messages successful: %d", successCount)
	suite.T().Logf("  - Throughput: %.2f msg/sec", actualThroughput)
	suite.T().Logf("  - Success rate: %.2f%%", successRate)

	// Verify burst throughput (allow more tolerance for burst)
	minBurstThroughput := float64(burstTarget) * 0.6 // Allow 40% tolerance for burst
	suite.Assert().Greater(actualThroughput, minBurstThroughput,
		"Burst throughput should be at least %.0f msg/sec, got %.2f", minBurstThroughput, actualThroughput)
}

// TestConcurrentProducers tests performance with multiple producer instances
func (suite *PerformanceTestSuite) TestConcurrentProducers() {
	if testing.Short() {
		suite.T().Skip("Skipping concurrent producers test in short mode")
	}

	const (
		numProducers        = 5
		messagesPerProducer = 1000
		totalMessages       = numProducers * messagesPerProducer
	)

	suite.T().Logf("Starting concurrent producers test: %d producers, %d messages each",
		numProducers, messagesPerProducer)

	var (
		totalSent    int64
		totalSuccess int64
		startTime    = time.Now()
	)

	var wg sync.WaitGroup
	for producerID := 0; producerID < numProducers; producerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine creates its own producer
			prod, err := producer.New(suite.config, suite.logger)
			if err != nil {
				suite.T().Logf("Producer %d failed to start: %v", id, err)
				return
			}
			defer prod.Close()

			var localSent, localSuccess int64
			for msgID := 0; msgID < messagesPerProducer; msgID++ {
				message := &models.Message{
					Topic:     "concurrent-test-topic",
					Key:       fmt.Sprintf("producer-%d-msg-%d", id, msgID),
					Value:     generateTestPayload(128),
					Timestamp: time.Now(),
				}

				receipt, err := prod.PublishMessage(suite.ctx, message)
				localSent++

				if err == nil && receipt != nil && receipt.Status == models.DeliveryStatusSuccess {
					localSuccess++
				}
			}

			atomic.AddInt64(&totalSent, localSent)
			atomic.AddInt64(&totalSuccess, localSuccess)

			suite.T().Logf("Producer %d completed: %d sent, %d successful", id, localSent, localSuccess)
		}(producerID)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	// Calculate metrics
	throughput := float64(totalSent) / elapsed.Seconds()
	successRate := float64(totalSuccess) / float64(totalSent) * 100

	suite.T().Logf("Concurrent producers test results:")
	suite.T().Logf("  - Duration: %v", elapsed)
	suite.T().Logf("  - Total messages sent: %d", totalSent)
	suite.T().Logf("  - Total successful: %d", totalSuccess)
	suite.T().Logf("  - Combined throughput: %.2f msg/sec", throughput)
	suite.T().Logf("  - Success rate: %.2f%%", successRate)

	// Verify that concurrent producers don't interfere with each other
	suite.Assert().Equal(totalMessages, int(totalSent), "Should send all expected messages")
	suite.Assert().Greater(successRate, 90.0, "Success rate should be > 90%% with concurrent producers")
}

// TestMemoryUsage tests that memory usage stays within 512MB limit
func (suite *PerformanceTestSuite) TestMemoryUsage() {
	if testing.Short() {
		suite.T().Skip("Skipping memory test in short mode")
	}

	prod, err := producer.New(suite.config, suite.logger)
	if err != nil {
		suite.T().Skipf("Could not create producer (Kafka may not be running): %v", err)
		return
	}
	defer prod.Close()

	const (
		memoryLimit  = 512 * 1024 * 1024 // 512MB in bytes
		testMessages = 10000
		payloadSize  = 1024 // 1KB payloads
	)

	suite.T().Logf("Starting memory usage test: %d messages with %d byte payloads", testMessages, payloadSize)

	// Get initial memory usage
	var initialMemStats, finalMemStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&initialMemStats)

	// Send messages
	for i := 0; i < testMessages; i++ {
		message := &models.Message{
			Topic:     "memory-test-topic",
			Key:       fmt.Sprintf("memory-%d", i),
			Value:     generateTestPayload(payloadSize),
			Timestamp: time.Now(),
		}

		_, err := prod.PublishMessage(suite.ctx, message)
		if err != nil {
			suite.T().Logf("Message %d failed: %v", i, err)
		}

		// Force GC periodically
		if i%1000 == 0 {
			runtime.GC()
		}
	}

	// Get final memory usage
	runtime.GC()
	runtime.ReadMemStats(&finalMemStats)

	memoryUsed := finalMemStats.Alloc
	memoryDelta := int64(finalMemStats.Alloc) - int64(initialMemStats.Alloc)

	suite.T().Logf("Memory usage test results:")
	suite.T().Logf("  - Initial memory: %d bytes (%.2f MB)", initialMemStats.Alloc, float64(initialMemStats.Alloc)/(1024*1024))
	suite.T().Logf("  - Final memory: %d bytes (%.2f MB)", finalMemStats.Alloc, float64(finalMemStats.Alloc)/(1024*1024))
	suite.T().Logf("  - Memory delta: %d bytes (%.2f MB)", memoryDelta, float64(memoryDelta)/(1024*1024))
	suite.T().Logf("  - Total heap size: %d bytes (%.2f MB)", finalMemStats.HeapSys, float64(finalMemStats.HeapSys)/(1024*1024))

	// Verify memory usage is within limits
	suite.Assert().Less(memoryUsed, uint64(memoryLimit),
		"Memory usage should be less than %d MB, got %.2f MB",
		memoryLimit/(1024*1024), float64(memoryUsed)/(1024*1024))
}

// Run the test suite
func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	suite.Run(t, new(PerformanceTestSuite))
}

// Helper functions

func generateTestPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}
	return payload
}
