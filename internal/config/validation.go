package config

import (
	"fmt"
	"strings"
)

// Validation constants
const (
	MinPort = 1
	MaxPort = 65535

	MinBatchSize = 1
	MaxBatchSize = 1000

	MinMessageBytes = 1024     // 1KB
	MaxMessageBytes = 10485760 // 10MB

	MinRetryAttempts = 0
	MaxRetryAttempts = 10

	MinBackoffMultiplier = 1.0
	MaxBackoffMultiplier = 5.0

	MinWorkerPoolSize = 1
	MaxWorkerPoolSize = 100

	MinChannelBufferSize = 1
	MaxChannelBufferSize = 10000

	MinMessagePoolSize = 100
	MaxMessagePoolSize = 100000
)

// Valid configuration values
var (
	ValidCompressionTypes  = []string{"none", "gzip", "lz4", "snappy", "zstd"}
	ValidLogLevels         = []string{"debug", "info", "warn", "error"}
	ValidLogFormats        = []string{"json", "text"}
	ValidSecurityProtocols = []string{"PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"}
)

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

// Error implements the error interface
func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error in field '%s': %s (value: %v)", e.Field, e.Message, e.Value)
}

// ValidationErrors represents multiple validation errors
type ValidationErrors []ValidationError

// Error implements the error interface
func (e ValidationErrors) Error() string {
	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("configuration validation failed with %d errors:\n%s", len(e), strings.Join(messages, "\n"))
}

// ValidateConfiguration performs comprehensive configuration validation
func ValidateConfiguration(config *Config) error {
	var errors ValidationErrors

	// Define validation steps
	validationSteps := []struct {
		name      string
		validator func() error
	}{
		{"kafka", func() error { return validateKafkaConfig(config.Kafka) }},
		{"server", func() error { return validateServerConfig(config.Server) }},
		{"logging", func() error { return validateLoggingConfig(config.Logging) }},
		{"monitoring", func() error { return validateMonitoringConfig(config.Monitoring) }},
		{"performance", func() error { return validatePerformanceConfig(config.Performance) }},
	}

	// Execute all validation steps
	for _, step := range validationSteps {
		if err := step.validator(); err != nil {
			errors = appendValidationError(errors, step.name, err)
		}
	}

	// Perform cross-field validation
	if crossFieldErrors := validateCrossFieldRules(config); crossFieldErrors != nil {
		if ve, ok := crossFieldErrors.(ValidationErrors); ok {
			errors = append(errors, ve...)
		} else {
			errors = append(errors, ValidationError{
				Field:   "cross-field",
				Message: crossFieldErrors.Error(),
			})
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// appendValidationError is a helper function to append validation errors consistently
func appendValidationError(errors ValidationErrors, field string, err error) ValidationErrors {
	if ve, ok := err.(ValidationErrors); ok {
		return append(errors, ve...)
	} else {
		return append(errors, ValidationError{
			Field:   field,
			Message: err.Error(),
		})
	}
}

// isValidChoice checks if a value is in a list of valid choices
func isValidChoice(value string, validChoices []string, caseSensitive bool) bool {
	for _, choice := range validChoices {
		if caseSensitive {
			if value == choice {
				return true
			}
		} else {
			if strings.EqualFold(value, choice) {
				return true
			}
		}
	}
	return false
}

// validateCrossFieldRules performs validation that requires checking multiple fields
func validateCrossFieldRules(config *Config) error {
	var errors ValidationErrors

	// Port conflict validation
	ports := map[int]string{
		config.Server.Port:            "server.port",
		config.Monitoring.MetricsPort: "monitoring.metrics_port",
	}

	if config.Monitoring.ProfilingEnabled {
		ports[config.Monitoring.ProfilingPort] = "monitoring.profiling_port"
	}

	// Check for port conflicts
	seenPorts := make(map[int]string)
	for port, field := range ports {
		if existingField, exists := seenPorts[port]; exists {
			errors = append(errors, ValidationError{
				Field:   field,
				Value:   port,
				Message: fmt.Sprintf("port %d conflicts with %s", port, existingField),
			})
		} else {
			seenPorts[port] = field
		}
	}

	// Validate timeout relationships
	if config.Kafka.Timeouts.Connection > config.Kafka.Timeouts.Request {
		errors = append(errors, ValidationError{
			Field:   "kafka.timeouts.connection",
			Value:   config.Kafka.Timeouts.Connection,
			Message: "connection timeout should not be greater than request timeout",
		})
	}

	if config.Kafka.Timeouts.Request > config.Kafka.Timeouts.Delivery {
		errors = append(errors, ValidationError{
			Field:   "kafka.timeouts.request",
			Value:   config.Kafka.Timeouts.Request,
			Message: "request timeout should not be greater than delivery timeout",
		})
	}

	// Validate batch configuration against message size
	maxBatchSize := config.Kafka.Producer.BatchSize * config.Kafka.Producer.MaxMessageBytes
	if maxBatchSize > 100*1024*1024 { // 100MB reasonable batch limit
		errors = append(errors, ValidationError{
			Field:   "kafka.producer",
			Value:   fmt.Sprintf("batch_size=%d * max_message_bytes=%d", config.Kafka.Producer.BatchSize, config.Kafka.Producer.MaxMessageBytes),
			Message: fmt.Sprintf("combined batch size (%d bytes) exceeds reasonable limit (100MB)", maxBatchSize),
		})
	}

	// Validate server shutdown timeout
	if config.Server.ShutdownTimeout < config.Server.WriteTimeout {
		errors = append(errors, ValidationError{
			Field:   "server.shutdown_timeout",
			Value:   config.Server.ShutdownTimeout,
			Message: "shutdown timeout should be at least as long as write timeout to allow graceful completion",
		})
	}

	// Validate performance settings consistency
	if config.Performance.MessagePoolSize < config.Performance.ChannelBufferSize*2 {
		errors = append(errors, ValidationError{
			Field:   "performance.message_pool_size",
			Value:   config.Performance.MessagePoolSize,
			Message: "message pool size should be at least twice the channel buffer size for optimal performance",
		})
	}

	// Validate security configuration consistency
	if config.Kafka.Security.Protocol != "PLAINTEXT" {
		if config.Kafka.Security.Protocol == "SASL_PLAINTEXT" || config.Kafka.Security.Protocol == "SASL_SSL" {
			if config.Kafka.Security.Username == "" {
				errors = append(errors, ValidationError{
					Field:   "kafka.security.username",
					Value:   config.Kafka.Security.Username,
					Message: fmt.Sprintf("username is required for protocol %s", config.Kafka.Security.Protocol),
				})
			}
		}

		if config.Kafka.Security.Protocol == "SSL" || config.Kafka.Security.Protocol == "SASL_SSL" {
			if config.Kafka.Security.CertificatePath == "" {
				errors = append(errors, ValidationError{
					Field:   "kafka.security.certificate_path",
					Value:   config.Kafka.Security.CertificatePath,
					Message: fmt.Sprintf("certificate path is required for protocol %s", config.Kafka.Security.Protocol),
				})
			}
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// ValidateRuntimeConstraints validates configuration against runtime constraints
func ValidateRuntimeConstraints(config *Config) error {
	var errors ValidationErrors

	// Validate that batch settings don't exceed memory constraints
	estimatedMemoryPerBatch := config.Kafka.Producer.BatchSize * config.Kafka.Producer.MaxMessageBytes
	estimatedTotalMemory := estimatedMemoryPerBatch * config.Performance.WorkerPoolSize

	if estimatedTotalMemory > 512*1024*1024 { // 512MB limit
		errors = append(errors, ValidationError{
			Field: "performance",
			Value: fmt.Sprintf("batch_size=%d * max_message_bytes=%d * worker_pool_size=%d",
				config.Kafka.Producer.BatchSize, config.Kafka.Producer.MaxMessageBytes, config.Performance.WorkerPoolSize),
			Message: fmt.Sprintf("estimated memory usage (%d MB) exceeds 512MB limit", estimatedTotalMemory/(1024*1024)),
		})
	}

	// Validate flush frequency is reasonable
	if config.Kafka.Producer.FlushFrequency < 1*time.Millisecond {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.flush_frequency",
			Value:   config.Kafka.Producer.FlushFrequency,
			Message: "flush frequency should be at least 1ms to avoid excessive CPU usage",
		})
	}

	if config.Kafka.Producer.FlushFrequency > 60*time.Second {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.flush_frequency",
			Value:   config.Kafka.Producer.FlushFrequency,
			Message: "flush frequency should not exceed 60s to ensure timely message delivery",
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// ValidateEnvironmentConsistency checks for environment-specific configuration issues
func ValidateEnvironmentConsistency(config *Config, environment string) error {
	var errors ValidationErrors

	switch strings.ToLower(environment) {
	case "production", "prod":
		// Production-specific validations
		if config.Logging.Level == "debug" {
			errors = append(errors, ValidationError{
				Field:   "logging.level",
				Value:   config.Logging.Level,
				Message: "debug logging not recommended for production environments",
			})
		}

		if config.Monitoring.ProfilingEnabled {
			errors = append(errors, ValidationError{
				Field:   "monitoring.profiling_enabled",
				Value:   config.Monitoring.ProfilingEnabled,
				Message: "profiling should be disabled in production for security and performance",
			})
		}

		if config.Kafka.Security.Protocol == "PLAINTEXT" {
			errors = append(errors, ValidationError{
				Field:   "kafka.security.protocol",
				Value:   config.Kafka.Security.Protocol,
				Message: "PLAINTEXT protocol not recommended for production environments",
			})
		}

	case "development", "dev":
		// Development-specific recommendations
		if config.Kafka.Producer.Acks != 1 {
			// This is just a warning, not an error
			// Could log this as a recommendation
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// validateBrokerAddress validates a Kafka broker address format
func validateBrokerAddress(broker string) error {
	if broker == "" {
		return fmt.Errorf("broker address cannot be empty")
	}

	// Simple validation for host:port format
	parts := strings.Split(broker, ":")
	if len(parts) != 2 {
		return fmt.Errorf("broker address must be in format 'host:port'")
	}

	host := parts[0]
	port := parts[1]

	if host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	if port == "" {
		return fmt.Errorf("port cannot be empty")
	}

	// Basic port validation
	var portNum int
	if n, err := fmt.Sscanf(port, "%d", &portNum); n != 1 || err != nil {
		return fmt.Errorf("port must be a valid number")
	}

	if portNum < MinPort || portNum > MaxPort {
		return fmt.Errorf("port must be between %d and %d", MinPort, MaxPort)
	}

	return nil
}

// validateKafkaConfig validates Kafka configuration
func validateKafkaConfig(config KafkaConfig) error {
	var errors ValidationErrors

	// Validate brokers
	if len(config.Brokers) == 0 {
		errors = append(errors, ValidationError{
			Field:   "kafka.brokers",
			Value:   config.Brokers,
			Message: "at least one broker must be specified",
		})
	}

	for i, broker := range config.Brokers {
		if err := validateBrokerAddress(broker); err != nil {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("kafka.brokers[%d]", i),
				Value:   broker,
				Message: err.Error(),
			})
		}
	}

	// Validate producer settings
	if config.Producer.BatchSize < MinBatchSize || config.Producer.BatchSize > MaxBatchSize {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.batch_size",
			Value:   config.Producer.BatchSize,
			Message: fmt.Sprintf("must be between %d and %d", MinBatchSize, MaxBatchSize),
		})
	}

	if config.Producer.MaxMessageBytes < MinMessageBytes || config.Producer.MaxMessageBytes > MaxMessageBytes {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.max_message_bytes",
			Value:   config.Producer.MaxMessageBytes,
			Message: fmt.Sprintf("must be between %d and %d bytes", MinMessageBytes, MaxMessageBytes),
		})
	}

	if !isValidChoice(config.Producer.Compression, ValidCompressionTypes, false) {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.compression",
			Value:   config.Producer.Compression,
			Message: fmt.Sprintf("must be one of: %s", strings.Join(ValidCompressionTypes, ", ")),
		})
	}

	// Validate retry settings
	if config.Retry.MaxAttempts < MinRetryAttempts || config.Retry.MaxAttempts > MaxRetryAttempts {
		errors = append(errors, ValidationError{
			Field:   "kafka.retry.max_attempts",
			Value:   config.Retry.MaxAttempts,
			Message: fmt.Sprintf("must be between %d and %d", MinRetryAttempts, MaxRetryAttempts),
		})
	}

	if config.Retry.BackoffMultiplier < MinBackoffMultiplier || config.Retry.BackoffMultiplier > MaxBackoffMultiplier {
		errors = append(errors, ValidationError{
			Field:   "kafka.retry.backoff_multiplier",
			Value:   config.Retry.BackoffMultiplier,
			Message: fmt.Sprintf("must be between %.1f and %.1f", MinBackoffMultiplier, MaxBackoffMultiplier),
		})
	}

	// Validate security protocol
	if !isValidChoice(config.Security.Protocol, ValidSecurityProtocols, false) {
		errors = append(errors, ValidationError{
			Field:   "kafka.security.protocol",
			Value:   config.Security.Protocol,
			Message: fmt.Sprintf("must be one of: %s", strings.Join(ValidSecurityProtocols, ", ")),
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// validateServerConfig validates server configuration
func validateServerConfig(config ServerConfig) error {
	var errors ValidationErrors

	if config.Port < MinPort || config.Port > MaxPort {
		errors = append(errors, ValidationError{
			Field:   "server.port",
			Value:   config.Port,
			Message: fmt.Sprintf("must be between %d and %d", MinPort, MaxPort),
		})
	}

	if config.Host == "" {
		errors = append(errors, ValidationError{
			Field:   "server.host",
			Value:   config.Host,
			Message: "cannot be empty",
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// validateLoggingConfig validates logging configuration
func validateLoggingConfig(config LoggingConfig) error {
	var errors ValidationErrors

	if !isValidChoice(config.Level, ValidLogLevels, false) {
		errors = append(errors, ValidationError{
			Field:   "logging.level",
			Value:   config.Level,
			Message: fmt.Sprintf("must be one of: %s", strings.Join(ValidLogLevels, ", ")),
		})
	}

	if !isValidChoice(config.Format, ValidLogFormats, false) {
		errors = append(errors, ValidationError{
			Field:   "logging.format",
			Value:   config.Format,
			Message: fmt.Sprintf("must be one of: %s", strings.Join(ValidLogFormats, ", ")),
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// validateMonitoringConfig validates monitoring configuration
func validateMonitoringConfig(config MonitoringConfig) error {
	var errors ValidationErrors

	if config.MetricsPort < MinPort || config.MetricsPort > MaxPort {
		errors = append(errors, ValidationError{
			Field:   "monitoring.metrics_port",
			Value:   config.MetricsPort,
			Message: fmt.Sprintf("must be between %d and %d", MinPort, MaxPort),
		})
	}

	if config.ProfilingEnabled && (config.ProfilingPort < MinPort || config.ProfilingPort > MaxPort) {
		errors = append(errors, ValidationError{
			Field:   "monitoring.profiling_port",
			Value:   config.ProfilingPort,
			Message: fmt.Sprintf("must be between %d and %d when profiling is enabled", MinPort, MaxPort),
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// validatePerformanceConfig validates performance configuration
func validatePerformanceConfig(config PerformanceConfig) error {
	var errors ValidationErrors

	if config.WorkerPoolSize < MinWorkerPoolSize || config.WorkerPoolSize > MaxWorkerPoolSize {
		errors = append(errors, ValidationError{
			Field:   "performance.worker_pool_size",
			Value:   config.WorkerPoolSize,
			Message: fmt.Sprintf("must be between %d and %d", MinWorkerPoolSize, MaxWorkerPoolSize),
		})
	}

	if config.ChannelBufferSize < MinChannelBufferSize || config.ChannelBufferSize > MaxChannelBufferSize {
		errors = append(errors, ValidationError{
			Field:   "performance.channel_buffer_size",
			Value:   config.ChannelBufferSize,
			Message: fmt.Sprintf("must be between %d and %d", MinChannelBufferSize, MaxChannelBufferSize),
		})
	}

	if config.MessagePoolSize < MinMessagePoolSize || config.MessagePoolSize > MaxMessagePoolSize {
		errors = append(errors, ValidationError{
			Field:   "performance.message_pool_size",
			Value:   config.MessagePoolSize,
			Message: fmt.Sprintf("must be between %d and %d", MinMessagePoolSize, MaxMessagePoolSize),
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}
