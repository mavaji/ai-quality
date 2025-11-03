package config

import (
	"fmt"
	"strings"
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

	// Validate Kafka configuration
	if err := validateKafkaConfig(config.Kafka); err != nil {
		if ve, ok := err.(ValidationErrors); ok {
			errors = append(errors, ve...)
		} else {
			errors = append(errors, ValidationError{
				Field:   "kafka",
				Message: err.Error(),
			})
		}
	}

	// Validate Server configuration
	if err := validateServerConfig(config.Server); err != nil {
		if ve, ok := err.(ValidationErrors); ok {
			errors = append(errors, ve...)
		} else {
			errors = append(errors, ValidationError{
				Field:   "server",
				Message: err.Error(),
			})
		}
	}

	// Validate Logging configuration
	if err := validateLoggingConfig(config.Logging); err != nil {
		if ve, ok := err.(ValidationErrors); ok {
			errors = append(errors, ve...)
		} else {
			errors = append(errors, ValidationError{
				Field:   "logging",
				Message: err.Error(),
			})
		}
	}

	// Validate Monitoring configuration
	if err := validateMonitoringConfig(config.Monitoring); err != nil {
		if ve, ok := err.(ValidationErrors); ok {
			errors = append(errors, ve...)
		} else {
			errors = append(errors, ValidationError{
				Field:   "monitoring",
				Message: err.Error(),
			})
		}
	}

	// Validate Performance configuration
	if err := validatePerformanceConfig(config.Performance); err != nil {
		if ve, ok := err.(ValidationErrors); ok {
			errors = append(errors, ve...)
		} else {
			errors = append(errors, ValidationError{
				Field:   "performance",
				Message: err.Error(),
			})
		}
	}

	if len(errors) > 0 {
		return errors
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
	if config.Producer.BatchSize < 1 || config.Producer.BatchSize > 1000 {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.batch_size",
			Value:   config.Producer.BatchSize,
			Message: "must be between 1 and 1000",
		})
	}

	if config.Producer.MaxMessageBytes < 1024 || config.Producer.MaxMessageBytes > 10485760 { // 1KB to 10MB
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.max_message_bytes",
			Value:   config.Producer.MaxMessageBytes,
			Message: "must be between 1KB and 10MB",
		})
	}

	validCompressionTypes := map[string]bool{
		"none": true, "gzip": true, "lz4": true, "snappy": true, "zstd": true,
	}
	if !validCompressionTypes[strings.ToLower(config.Producer.Compression)] {
		errors = append(errors, ValidationError{
			Field:   "kafka.producer.compression",
			Value:   config.Producer.Compression,
			Message: "must be one of: none, gzip, lz4, snappy, zstd",
		})
	}

	// Validate retry settings
	if config.Retry.MaxAttempts < 0 || config.Retry.MaxAttempts > 10 {
		errors = append(errors, ValidationError{
			Field:   "kafka.retry.max_attempts",
			Value:   config.Retry.MaxAttempts,
			Message: "must be between 0 and 10",
		})
	}

	if config.Retry.BackoffMultiplier < 1.0 || config.Retry.BackoffMultiplier > 5.0 {
		errors = append(errors, ValidationError{
			Field:   "kafka.retry.backoff_multiplier",
			Value:   config.Retry.BackoffMultiplier,
			Message: "must be between 1.0 and 5.0",
		})
	}

	// Validate security protocol
	validProtocols := map[string]bool{
		"PLAINTEXT": true, "SASL_PLAINTEXT": true, "SASL_SSL": true, "SSL": true,
	}
	if !validProtocols[strings.ToUpper(config.Security.Protocol)] {
		errors = append(errors, ValidationError{
			Field:   "kafka.security.protocol",
			Value:   config.Security.Protocol,
			Message: "must be one of: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL",
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

	if config.Port < 1 || config.Port > 65535 {
		errors = append(errors, ValidationError{
			Field:   "server.port",
			Value:   config.Port,
			Message: "must be between 1 and 65535",
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

	validLevels := map[string]bool{
		"debug": true, "info": true, "warn": true, "error": true,
	}
	if !validLevels[strings.ToLower(config.Level)] {
		errors = append(errors, ValidationError{
			Field:   "logging.level",
			Value:   config.Level,
			Message: "must be one of: debug, info, warn, error",
		})
	}

	validFormats := map[string]bool{
		"json": true, "text": true,
	}
	if !validFormats[strings.ToLower(config.Format)] {
		errors = append(errors, ValidationError{
			Field:   "logging.format",
			Value:   config.Format,
			Message: "must be one of: json, text",
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

	if config.MetricsPort < 1 || config.MetricsPort > 65535 {
		errors = append(errors, ValidationError{
			Field:   "monitoring.metrics_port",
			Value:   config.MetricsPort,
			Message: "must be between 1 and 65535",
		})
	}

	if config.ProfilingEnabled && (config.ProfilingPort < 1 || config.ProfilingPort > 65535) {
		errors = append(errors, ValidationError{
			Field:   "monitoring.profiling_port",
			Value:   config.ProfilingPort,
			Message: "must be between 1 and 65535 when profiling is enabled",
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

	if config.WorkerPoolSize < 1 || config.WorkerPoolSize > 100 {
		errors = append(errors, ValidationError{
			Field:   "performance.worker_pool_size",
			Value:   config.WorkerPoolSize,
			Message: "must be between 1 and 100",
		})
	}

	if config.ChannelBufferSize < 1 || config.ChannelBufferSize > 10000 {
		errors = append(errors, ValidationError{
			Field:   "performance.channel_buffer_size",
			Value:   config.ChannelBufferSize,
			Message: "must be between 1 and 10000",
		})
	}

	if config.MessagePoolSize < 100 || config.MessagePoolSize > 100000 {
		errors = append(errors, ValidationError{
			Field:   "performance.message_pool_size",
			Value:   config.MessagePoolSize,
			Message: "must be between 100 and 100000",
		})
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}