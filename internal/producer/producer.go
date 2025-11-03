package producer

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	
	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/models"
)

// Producer interface defines the contract for message producers
type Producer interface {
	PublishMessage(ctx context.Context, message *models.Message) (*models.DeliveryReceipt, error)
	Close() error
}

// SaramaProducer implements Producer interface using Sarama Kafka client
type SaramaProducer struct {
	producer sarama.SyncProducer
	config   *config.ProducerConfigurationModel
	logger   *zap.Logger
	mu       sync.RWMutex
	closed   bool
}

// New creates a new Kafka producer instance
func New(cfg *config.Config, logger *zap.Logger) (Producer, error) {
	// Create producer configuration model
	producerConfig, err := config.NewProducerConfiguration(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer configuration: %w", err)
	}

	// Create Sarama configuration
	saramaConfig := sarama.NewConfig()
	
	// Configure producer settings
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas
	saramaConfig.Producer.Retry.Max = producerConfig.RetryPolicy.MaxAttempts
	saramaConfig.Producer.Retry.Backoff = producerConfig.RetryPolicy.InitialBackoff
	
	// Configure compression
	switch producerConfig.CompressionType {
	case config.CompressionNone:
		saramaConfig.Producer.Compression = sarama.CompressionNone
	case config.CompressionGZIP:
		saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case config.CompressionLZ4:
		saramaConfig.Producer.Compression = sarama.CompressionLZ4
	case config.CompressionSnappy:
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	case config.CompressionZSTD:
		saramaConfig.Producer.Compression = sarama.CompressionZSTD
	default:
		saramaConfig.Producer.Compression = sarama.CompressionNone
	}
	
	// Configure batch settings
	saramaConfig.Producer.Flush.Messages = producerConfig.BatchSettings.MaxMessages
	saramaConfig.Producer.Flush.Bytes = producerConfig.BatchSettings.MaxBytes
	saramaConfig.Producer.Flush.Frequency = producerConfig.BatchSettings.FlushInterval
	
	// Configure timeouts
	saramaConfig.Net.DialTimeout = producerConfig.TimeoutSettings.ConnectionTimeout
	saramaConfig.Net.WriteTimeout = producerConfig.TimeoutSettings.RequestTimeout
	saramaConfig.Net.ReadTimeout = producerConfig.TimeoutSettings.RequestTimeout
	
	// Configure security
	if err := configureSecurity(saramaConfig, &producerConfig.SecurityConfig); err != nil {
		return nil, fmt.Errorf("failed to configure security: %w", err)
	}
	
	// Create Sarama producer
	producer, err := sarama.NewSyncProducer(producerConfig.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	sp := &SaramaProducer{
		producer: producer,
		config:   producerConfig,
		logger:   logger,
	}

	logger.Info("Kafka producer created successfully",
		zap.Strings("brokers", producerConfig.Brokers),
		zap.String("compression", string(producerConfig.CompressionType)),
		zap.Int("batch_max_messages", producerConfig.BatchSettings.MaxMessages),
		zap.Int("batch_max_bytes", producerConfig.BatchSettings.MaxBytes),
	)

	return sp, nil
}

// PublishMessage publishes a single message to Kafka
func (p *SaramaProducer) PublishMessage(ctx context.Context, message *models.Message) (*models.DeliveryReceipt, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("producer is closed")
	}
	p.mu.RUnlock()

	// Validate message
	if err := message.Validate(); err != nil {
		return nil, fmt.Errorf("message validation failed: %w", err)
	}

	// Generate message ID
	messageID := generateMessageID()
	
	// Convert to Sarama message
	saramaMsg, err := p.convertToSaramaMessage(message)
	if err != nil {
		return nil, fmt.Errorf("failed to convert message: %w", err)
	}

	// Add message ID to headers
	if saramaMsg.Headers == nil {
		saramaMsg.Headers = make([]sarama.RecordHeader, 0)
	}
	saramaMsg.Headers = append(saramaMsg.Headers, sarama.RecordHeader{
		Key:   []byte("message_id"),
		Value: []byte(messageID),
	})

	// Create channel for result
	resultChan := make(chan *models.DeliveryReceipt, 1)
	errorChan := make(chan error, 1)
	
	// Publish in goroutine to handle context cancellation
	go func() {
		start := time.Now()
		
		partition, offset, err := p.producer.SendMessage(saramaMsg)
		if err != nil {
			// Create failure receipt
			receipt := models.NewFailureReceipt(messageID, message.Topic, err.Error())
			receipt.Metadata.AttemptCount = 1 // This would be updated in retry logic
			
			p.logger.Error("Failed to publish message",
				zap.String("message_id", messageID),
				zap.String("topic", message.Topic),
				zap.Error(err),
			)
			
			errorChan <- fmt.Errorf("failed to send message to Kafka: %w", err)
			return
		}

		// Create success receipt
		receipt := models.NewSuccessReceipt(messageID, message.Topic, partition, offset)
		receipt.Metadata.AttemptCount = 1
		receipt.SetBrokerInfo("", string(p.config.CompressionType), message.Size())
		
		// Calculate delivery latency
		deliveryLatency := time.Since(start)
		
		p.logger.Info("Message published successfully",
			zap.String("message_id", messageID),
			zap.String("topic", message.Topic),
			zap.Int32("partition", partition),
			zap.Int64("offset", offset),
			zap.Duration("latency", deliveryLatency),
		)
		
		resultChan <- receipt
	}()

	// Wait for result or context cancellation
	select {
	case receipt := <-resultChan:
		return receipt, nil
	case err := <-errorChan:
		return nil, err
	case <-ctx.Done():
		// Context was cancelled or timed out
		if ctx.Err() == context.DeadlineExceeded {
			receipt := models.NewFailureReceipt(messageID, message.Topic, "delivery timeout")
			receipt.SetTimeout("context deadline exceeded")
			return receipt, context.DeadlineExceeded
		}
		return nil, ctx.Err()
	}
}

// Close gracefully shuts down the producer
func (p *SaramaProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil
	}
	
	p.closed = true
	
	if err := p.producer.Close(); err != nil {
		p.logger.Error("Error closing Kafka producer", zap.Error(err))
		return fmt.Errorf("failed to close Kafka producer: %w", err)
	}
	
	p.logger.Info("Kafka producer closed successfully")
	return nil
}

// convertToSaramaMessage converts our Message model to Sarama's ProducerMessage
func (p *SaramaProducer) convertToSaramaMessage(message *models.Message) (*sarama.ProducerMessage, error) {
	saramaMsg := &sarama.ProducerMessage{
		Topic:     message.Topic,
		Value:     sarama.ByteEncoder(message.Value),
		Timestamp: message.Timestamp,
	}

	// Set key if provided
	if message.Key != "" {
		saramaMsg.Key = sarama.StringEncoder(message.Key)
	}

	// Convert headers
	if message.Headers != nil && len(message.Headers) > 0 {
		saramaMsg.Headers = make([]sarama.RecordHeader, 0, len(message.Headers))
		for key, value := range message.Headers {
			saramaMsg.Headers = append(saramaMsg.Headers, sarama.RecordHeader{
				Key:   []byte(key),
				Value: []byte(value),
			})
		}
	}

	return saramaMsg, nil
}

// configureSecurity configures security settings for Sarama
func configureSecurity(saramaConfig *sarama.Config, securityConfig *config.SecurityConfigModel) error {
	switch securityConfig.Protocol {
	case config.SecurityPlaintext:
		// No additional configuration needed
		return nil
		
	case config.SecuritySASLPlaintext:
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = securityConfig.Username
		saramaConfig.Net.SASL.Password = securityConfig.Password
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		return nil
		
	case config.SecuritySASLSSL:
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = securityConfig.Username
		saramaConfig.Net.SASL.Password = securityConfig.Password
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		return nil
		
	case config.SecuritySSL:
		saramaConfig.Net.TLS.Enable = true
		// Additional TLS configuration would go here if certificate path is provided
		return nil
		
	default:
		return fmt.Errorf("unsupported security protocol: %s", securityConfig.Protocol)
	}
}

// generateMessageID generates a unique message ID
func generateMessageID() string {
	// Use current timestamp and random bytes for uniqueness
	timestamp := time.Now().UnixNano()
	
	// Generate 4 random bytes
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to pseudo-random if crypto/rand fails
		for i := range randomBytes {
			randomBytes[i] = byte(time.Now().UnixNano() % 256)
		}
	}
	
	// Convert to hex string
	return fmt.Sprintf("msg-%d-%x", timestamp, randomBytes)
}

// GetStats returns producer statistics (placeholder for future implementation)
func (p *SaramaProducer) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	return map[string]interface{}{
		"closed":        p.closed,
		"brokers":       p.config.Brokers,
		"compression":   p.config.CompressionType,
		"batch_settings": map[string]interface{}{
			"max_messages": p.config.BatchSettings.MaxMessages,
			"max_bytes":    p.config.BatchSettings.MaxBytes,
			"flush_interval": p.config.BatchSettings.FlushInterval.String(),
		},
	}
}

// MockProducer is a mock implementation for testing
type MockProducer struct {
	messages []mockMessage
	closed   bool
	mu       sync.RWMutex
}

type mockMessage struct {
	message *models.Message
	receipt *models.DeliveryReceipt
	err     error
}

// NewMockProducer creates a new mock producer for testing
func NewMockProducer() *MockProducer {
	return &MockProducer{
		messages: make([]mockMessage, 0),
	}
}

// PublishMessage mock implementation
func (m *MockProducer) PublishMessage(ctx context.Context, message *models.Message) (*models.DeliveryReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.closed {
		return nil, fmt.Errorf("mock producer is closed")
	}
	
	// Validate message
	if err := message.Validate(); err != nil {
		return nil, fmt.Errorf("message validation failed: %w", err)
	}
	
	// Generate mock receipt
	messageID := generateMessageID()
	receipt := models.NewSuccessReceipt(messageID, message.Topic, 0, int64(len(m.messages)))
	
	// Store the message
	m.messages = append(m.messages, mockMessage{
		message: message.Clone(),
		receipt: receipt,
		err:     nil,
	})
	
	return receipt, nil
}

// Close mock implementation
func (m *MockProducer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.closed = true
	return nil
}

// GetMessages returns all published messages (for testing)
func (m *MockProducer) GetMessages() []*models.Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	messages := make([]*models.Message, len(m.messages))
	for i, msg := range m.messages {
		messages[i] = msg.message
	}
	return messages
}

// Reset clears all messages (for testing)
func (m *MockProducer) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.messages = m.messages[:0]
	m.closed = false
}