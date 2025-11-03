package config

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SetupLogger creates and configures a structured logger based on configuration
func SetupLogger(config LoggingConfig) (*zap.Logger, error) {
	// Configure log level
	level, err := zapcore.ParseLevel(config.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", config.Level, err)
	}

	// Configure encoder
	var encoderConfig zapcore.EncoderConfig
	var encoder zapcore.Encoder

	switch config.Format {
	case "json":
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	case "text":
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("unsupported log format: %s", config.Format)
	}

	// Configure output
	var output zapcore.WriteSyncer
	switch config.Output {
	case "stdout":
		output = zapcore.AddSync(os.Stdout)
	case "stderr":
		output = zapcore.AddSync(os.Stderr)
	default:
		// Assume it's a file path
		file, err := os.OpenFile(config.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", config.Output, err)
		}
		output = zapcore.AddSync(file)
	}

	// Create core
	core := zapcore.NewCore(encoder, output, level)

	// Create logger with caller information and stack traces for errors
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logger, nil
}

// LoggerWithFields creates a logger with common fields for a component
func LoggerWithFields(logger *zap.Logger, component string, fields ...zap.Field) *zap.Logger {
	allFields := append([]zap.Field{zap.String("component", component)}, fields...)
	return logger.With(allFields...)
}

// ProducerLogger creates a logger specifically for producer operations
func ProducerLogger(logger *zap.Logger) *zap.Logger {
	return LoggerWithFields(logger, "producer")
}

// ServerLogger creates a logger specifically for HTTP server operations
func ServerLogger(logger *zap.Logger) *zap.Logger {
	return LoggerWithFields(logger, "server")
}

// HealthLogger creates a logger specifically for health check operations
func HealthLogger(logger *zap.Logger) *zap.Logger {
	return LoggerWithFields(logger, "health")
}

// MetricsLogger creates a logger specifically for metrics operations
func MetricsLogger(logger *zap.Logger) *zap.Logger {
	return LoggerWithFields(logger, "metrics")
}