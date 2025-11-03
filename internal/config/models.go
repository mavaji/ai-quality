package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// ProducerConfigurationModel represents the validated producer configuration
type ProducerConfigurationModel struct {
	Brokers           []string
	SerializationFormat SerializationFormat
	BatchSettings     BatchConfigModel
	RetryPolicy       RetryConfigModel
	TimeoutSettings   TimeoutConfigModel
	SecurityConfig    SecurityConfigModel
	CompressionType   CompressionType
}

// SerializationFormat enum for supported serialization formats
type SerializationFormat string

const (
	SerializationJSON   SerializationFormat = "json"
	SerializationAvro   SerializationFormat = "avro"
	SerializationBinary SerializationFormat = "binary"
)

// CompressionType enum for supported compression types
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGZIP   CompressionType = "gzip"
	CompressionLZ4    CompressionType = "lz4"
	CompressionSnappy CompressionType = "snappy"
	CompressionZSTD   CompressionType = "zstd"
)

// BatchConfigModel represents validated batch configuration
type BatchConfigModel struct {
	MaxMessages   int           // Maximum messages per batch (1-1000)
	MaxBytes      int           // Maximum batch size in bytes (1KB-1MB)
	FlushInterval time.Duration // Maximum time to wait before sending batch (1ms-10s)
}

// RetryConfigModel represents validated retry configuration
type RetryConfigModel struct {
	MaxAttempts       int           // Maximum retry attempts (0-10)
	InitialBackoff    time.Duration // Initial retry delay (10ms-1s)
	MaxBackoff        time.Duration // Maximum retry delay (1s-300s)
	BackoffMultiplier float64       // Exponential backoff multiplier (1.0-5.0)
}

// TimeoutConfigModel represents validated timeout configuration
type TimeoutConfigModel struct {
	ConnectionTimeout time.Duration // Broker connection timeout (1s-30s)
	RequestTimeout    time.Duration // Request timeout (1s-60s)
	DeliveryTimeout   time.Duration // End-to-end delivery timeout (1s-300s)
}

// SecurityProtocol enum for supported security protocols
type SecurityProtocol string

const (
	SecurityPlaintext     SecurityProtocol = "PLAINTEXT"
	SecuritySASLPlaintext SecurityProtocol = "SASL_PLAINTEXT"
	SecuritySASLSSL       SecurityProtocol = "SASL_SSL"
	SecuritySSL           SecurityProtocol = "SSL"
)

// SecurityConfigModel represents validated security configuration
type SecurityConfigModel struct {
	Protocol        SecurityProtocol
	Username        string
	Password        string
	CertificatePath string
}

// NewProducerConfiguration creates and validates a producer configuration
func NewProducerConfiguration(config *Config) (*ProducerConfigurationModel, error) {
	// Validate and convert configuration
	model := &ProducerConfigurationModel{
		Brokers: config.Kafka.Brokers,
	}

	// Validate brokers
	if err := model.validateBrokers(); err != nil {
		return nil, err
	}

	// Set serialization format (default to JSON for now)
	model.SerializationFormat = SerializationJSON

	// Validate and set compression type
	if err := model.setCompressionType(config.Kafka.Producer.Compression); err != nil {
		return nil, err
	}

	// Validate and set batch settings
	if err := model.setBatchSettings(config.Kafka.Producer); err != nil {
		return nil, err
	}

	// Validate and set retry policy
	if err := model.setRetryPolicy(config.Kafka.Retry); err != nil {
		return nil, err
	}

	// Validate and set timeout settings
	if err := model.setTimeoutSettings(config.Kafka.Timeouts); err != nil {
		return nil, err
	}

	// Validate and set security config
	if err := model.setSecurityConfig(config.Kafka.Security); err != nil {
		return nil, err
	}

	return model, nil
}

// validateBrokers validates broker addresses
func (p *ProducerConfigurationModel) validateBrokers() error {
	if len(p.Brokers) == 0 {
		return fmt.Errorf("at least one broker address must be provided")
	}

	for i, broker := range p.Brokers {
		if err := validateBrokerAddress(broker); err != nil {
			return fmt.Errorf("invalid broker address at index %d (%s): %w", i, broker, err)
		}
	}

	return nil
}

// validateBrokerAddress validates a single broker address
func validateBrokerAddress(address string) error {
	if address == "" {
		return fmt.Errorf("broker address cannot be empty")
	}

	// Check if it contains host:port format
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("broker address must be in host:port format: %w", err)
	}

	// Validate host
	if host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	// Validate port
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port number: %w", err)
	}

	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", port)
	}

	return nil
}

// setCompressionType validates and sets compression type
func (p *ProducerConfigurationModel) setCompressionType(compression string) error {
	switch strings.ToLower(compression) {
	case "none", "":
		p.CompressionType = CompressionNone
	case "gzip":
		p.CompressionType = CompressionGZIP
	case "lz4":
		p.CompressionType = CompressionLZ4
	case "snappy":
		p.CompressionType = CompressionSnappy
	case "zstd":
		p.CompressionType = CompressionZSTD
	default:
		return fmt.Errorf("unsupported compression type: %s", compression)
	}
	return nil
}

// setBatchSettings validates and sets batch configuration
func (p *ProducerConfigurationModel) setBatchSettings(config ProducerConfig) error {
	// Validate max messages
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return fmt.Errorf("batch size must be between 1 and 1000, got %d", config.BatchSize)
	}

	// Validate max bytes (1KB to 1MB)
	maxBytes := config.MaxMessageBytes
	if maxBytes < 1024 || maxBytes > 1048576 {
		return fmt.Errorf("max message bytes must be between 1KB and 1MB, got %d", maxBytes)
	}

	// Validate flush interval (1ms to 10s)
	if config.FlushFrequency < time.Millisecond || config.FlushFrequency > 10*time.Second {
		return fmt.Errorf("flush frequency must be between 1ms and 10s, got %v", config.FlushFrequency)
	}

	p.BatchSettings = BatchConfigModel{
		MaxMessages:   config.BatchSize,
		MaxBytes:      maxBytes,
		FlushInterval: config.FlushFrequency,
	}

	return nil
}

// setRetryPolicy validates and sets retry configuration
func (p *ProducerConfigurationModel) setRetryPolicy(config RetryConfig) error {
	// Validate max attempts
	if config.MaxAttempts < 0 || config.MaxAttempts > 10 {
		return fmt.Errorf("max retry attempts must be between 0 and 10, got %d", config.MaxAttempts)
	}

	// Validate initial backoff (10ms to 1s)
	if config.InitialBackoff < 10*time.Millisecond || config.InitialBackoff > time.Second {
		return fmt.Errorf("initial backoff must be between 10ms and 1s, got %v", config.InitialBackoff)
	}

	// Validate max backoff (1s to 300s)
	if config.MaxBackoff < time.Second || config.MaxBackoff > 300*time.Second {
		return fmt.Errorf("max backoff must be between 1s and 300s, got %v", config.MaxBackoff)
	}

	// Validate backoff multiplier (1.0 to 5.0)
	if config.BackoffMultiplier < 1.0 || config.BackoffMultiplier > 5.0 {
		return fmt.Errorf("backoff multiplier must be between 1.0 and 5.0, got %f", config.BackoffMultiplier)
	}

	p.RetryPolicy = RetryConfigModel{
		MaxAttempts:       config.MaxAttempts,
		InitialBackoff:    config.InitialBackoff,
		MaxBackoff:        config.MaxBackoff,
		BackoffMultiplier: config.BackoffMultiplier,
	}

	return nil
}

// setTimeoutSettings validates and sets timeout configuration
func (p *ProducerConfigurationModel) setTimeoutSettings(config TimeoutConfig) error {
	// Validate connection timeout (1s to 30s)
	if config.Connection < time.Second || config.Connection > 30*time.Second {
		return fmt.Errorf("connection timeout must be between 1s and 30s, got %v", config.Connection)
	}

	// Validate request timeout (1s to 60s)
	if config.Request < time.Second || config.Request > 60*time.Second {
		return fmt.Errorf("request timeout must be between 1s and 60s, got %v", config.Request)
	}

	// Validate delivery timeout (1s to 300s)
	if config.Delivery < time.Second || config.Delivery > 300*time.Second {
		return fmt.Errorf("delivery timeout must be between 1s and 300s, got %v", config.Delivery)
	}

	p.TimeoutSettings = TimeoutConfigModel{
		ConnectionTimeout: config.Connection,
		RequestTimeout:    config.Request,
		DeliveryTimeout:   config.Delivery,
	}

	return nil
}

// setSecurityConfig validates and sets security configuration
func (p *ProducerConfigurationModel) setSecurityConfig(config SecurityConfig) error {
	// Validate protocol
	switch SecurityProtocol(strings.ToUpper(config.Protocol)) {
	case SecurityPlaintext:
		p.SecurityConfig.Protocol = SecurityPlaintext
	case SecuritySASLPlaintext:
		p.SecurityConfig.Protocol = SecuritySASLPlaintext
	case SecuritySASLSSL:
		p.SecurityConfig.Protocol = SecuritySASLSSL
	case SecuritySSL:
		p.SecurityConfig.Protocol = SecuritySSL
	default:
		return fmt.Errorf("unsupported security protocol: %s", config.Protocol)
	}

	// Set optional fields
	p.SecurityConfig.Username = config.Username
	p.SecurityConfig.Password = config.Password
	p.SecurityConfig.CertificatePath = config.CertificatePath

	// Validate SASL credentials if needed
	if p.SecurityConfig.Protocol == SecuritySASLPlaintext || p.SecurityConfig.Protocol == SecuritySASLSSL {
		if p.SecurityConfig.Username == "" || p.SecurityConfig.Password == "" {
			return fmt.Errorf("SASL protocols require username and password")
		}
	}

	return nil
}