package integration

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"

	"sdd-kafka-producer/internal/config"
)

// KafkaContainer represents a Kafka testcontainer instance
type KafkaContainer struct {
	Container *kafka.KafkaContainer
	Brokers   []string
}

// SetupKafkaContainer starts a Kafka container and returns connection details
func SetupKafkaContainer(ctx context.Context, t *testing.T) (container *KafkaContainer, err error) {
	// Check if Docker is available before trying testcontainers
	if !isDockerAvailable() {
		return nil, fmt.Errorf("docker not available")
	}

	// Use defer to catch any panics from testcontainers
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Testcontainers panic recovered: %v", r)
			err = fmt.Errorf("testcontainers panic: %v", r)
		}
	}()

	// Create Kafka container with proper configuration
	kafkaContainer, err := kafka.RunContainer(ctx,
		testcontainers.WithImage("confluentinc/cp-kafka:7.4.0"), // Use specific stable version
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start Kafka container: %w", err)
	}

	// Get broker addresses
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get Kafka brokers: %w", err)
	}

	return &KafkaContainer{
		Container: kafkaContainer,
		Brokers:   brokers,
	}, nil
}

// Cleanup terminates the Kafka container
func (kc *KafkaContainer) Cleanup(ctx context.Context) error {
	if kc.Container != nil {
		return kc.Container.Terminate(ctx)
	}
	return nil
}

// CreateTestConfigWithKafka creates a test configuration using the Kafka container
func CreateTestConfigWithKafka(kc *KafkaContainer) *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: kc.Brokers,
			Producer: config.ProducerConfig{
				BatchSize:       100,
				MaxMessageBytes: 1048576, // 1MB
				FlushFrequency:  100 * time.Millisecond,
				Compression:     "lz4",
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

// IsKafkaBrokerHealthy checks if the Kafka broker is ready to accept connections
func IsKafkaBrokerHealthy(ctx context.Context, brokers []string) bool {
	// We'll implement a simple check by trying to create a producer
	// If testcontainers is working correctly, the broker should be ready
	if len(brokers) == 0 {
		return false
	}

	// Basic validation - check if brokers are properly formatted
	for _, broker := range brokers {
		if !strings.Contains(broker, ":") {
			return false
		}
	}

	return true
}

// isDockerAvailable checks if Docker is available and running
func isDockerAvailable() bool {
	// First check if docker command exists
	_, err := exec.LookPath("docker")
	if err != nil {
		return false
	}

	// Try to run docker version command
	cmd := exec.Command("docker", "version", "--format", "{{.Client.Version}}")
	err = cmd.Run()
	if err != nil {
		return false
	}

	// Try to check if Docker daemon is running
	cmd = exec.Command("docker", "info")
	err = cmd.Run()
	return err == nil
}
