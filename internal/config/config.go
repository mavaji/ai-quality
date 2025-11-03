package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete application configuration
type Config struct {
	Kafka       KafkaConfig       `yaml:"kafka"`
	Server      ServerConfig      `yaml:"server"`
	Logging     LoggingConfig     `yaml:"logging"`
	Monitoring  MonitoringConfig  `yaml:"monitoring"`
	Performance PerformanceConfig `yaml:"performance"`
}

// KafkaConfig holds Kafka-related configuration
type KafkaConfig struct {
	Brokers  []string      `yaml:"brokers"`
	Producer ProducerConfig `yaml:"producer"`
	Retry    RetryConfig   `yaml:"retry"`
	Timeouts TimeoutConfig `yaml:"timeouts"`
	Security SecurityConfig `yaml:"security"`
}

// ProducerConfig defines Kafka producer settings
type ProducerConfig struct {
	Acks              int           `yaml:"acks"`
	Compression       string        `yaml:"compression"`
	BatchSize         int           `yaml:"batch_size"`
	FlushFrequency    time.Duration `yaml:"flush_frequency"`
	MaxMessageBytes   int           `yaml:"max_message_bytes"`
}

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxAttempts       int           `yaml:"max_attempts"`
	InitialBackoff    time.Duration `yaml:"initial_backoff"`
	MaxBackoff        time.Duration `yaml:"max_backoff"`
	BackoffMultiplier float64       `yaml:"backoff_multiplier"`
}

// TimeoutConfig defines timeout settings
type TimeoutConfig struct {
	Connection time.Duration `yaml:"connection"`
	Request    time.Duration `yaml:"request"`
	Delivery   time.Duration `yaml:"delivery"`
}

// SecurityConfig defines security settings
type SecurityConfig struct {
	Protocol        string `yaml:"protocol"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	CertificatePath string `yaml:"certificate_path"`
}

// ServerConfig defines HTTP server settings
type ServerConfig struct {
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// LoggingConfig defines logging settings
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

// MonitoringConfig defines monitoring settings
type MonitoringConfig struct {
	MetricsPort           int           `yaml:"metrics_port"`
	HealthCheckInterval   time.Duration `yaml:"health_check_interval"`
	ProfilingEnabled      bool          `yaml:"profiling_enabled"`
	ProfilingPort         int           `yaml:"profiling_port"`
}

// PerformanceConfig defines performance tuning settings
type PerformanceConfig struct {
	WorkerPoolSize     int `yaml:"worker_pool_size"`
	ChannelBufferSize  int `yaml:"channel_buffer_size"`
	MessagePoolSize    int `yaml:"message_pool_size"`
	BatchPoolSize      int `yaml:"batch_pool_size"`
}

// Load reads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	// Set defaults
	config := &Config{
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Producer: ProducerConfig{
				Acks:              1,
				Compression:       "lz4",
				BatchSize:         100,
				FlushFrequency:    5 * time.Millisecond,
				MaxMessageBytes:   1048576, // 1MB
			},
			Retry: RetryConfig{
				MaxAttempts:       3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        30 * time.Second,
				BackoffMultiplier: 2.0,
			},
			Timeouts: TimeoutConfig{
				Connection: 30 * time.Second,
				Request:    60 * time.Second,
				Delivery:   300 * time.Second,
			},
			Security: SecurityConfig{
				Protocol: "PLAINTEXT",
			},
		},
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			ShutdownTimeout: 60 * time.Second,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
		Monitoring: MonitoringConfig{
			MetricsPort:         9090,
			HealthCheckInterval: 30 * time.Second,
			ProfilingEnabled:    true,
			ProfilingPort:       6060,
		},
		Performance: PerformanceConfig{
			WorkerPoolSize:    10,
			ChannelBufferSize: 1024,
			MessagePoolSize:   1000,
			BatchPoolSize:     100,
		},
	}

	// Load from file if provided
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
		}
	}

	// Override with environment variables
	applyEnvironmentOverrides(config)

	return config, nil
}

// applyEnvironmentOverrides applies environment variable overrides
func applyEnvironmentOverrides(config *Config) {
	if val := os.Getenv("KAFKA_BROKERS"); val != "" {
		// Simple comma-separated brokers for now
		// In production, you might want more sophisticated parsing
		config.Kafka.Brokers = []string{val}
	}
	
	if val := os.Getenv("SERVER_PORT"); val != "" {
		if port := parseInt(val); port > 0 {
			config.Server.Port = port
		}
	}
	
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		config.Logging.Level = val
	}
}

// parseInt parses integer with error handling
func parseInt(s string) int {
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}

// Validate performs configuration validation
func (c *Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one Kafka broker must be configured")
	}

	if c.Kafka.Producer.BatchSize < 1 || c.Kafka.Producer.BatchSize > 1000 {
		return fmt.Errorf("batch size must be between 1 and 1000, got %d", c.Kafka.Producer.BatchSize)
	}

	if c.Kafka.Retry.MaxAttempts < 0 || c.Kafka.Retry.MaxAttempts > 10 {
		return fmt.Errorf("max retry attempts must be between 0 and 10, got %d", c.Kafka.Retry.MaxAttempts)
	}

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("server port must be between 1 and 65535, got %d", c.Server.Port)
	}

	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[c.Logging.Level] {
		return fmt.Errorf("log level must be one of: debug, info, warn, error, got %s", c.Logging.Level)
	}

	validCompressionTypes := map[string]bool{"none": true, "gzip": true, "lz4": true, "snappy": true}
	if !validCompressionTypes[c.Kafka.Producer.Compression] {
		return fmt.Errorf("compression type must be one of: none, gzip, lz4, snappy, got %s", c.Kafka.Producer.Compression)
	}

	return nil
}