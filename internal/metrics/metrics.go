package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Manager handles Prometheus metrics collection
type Manager struct {
	registry *prometheus.Registry
	logger   *zap.Logger

	// Producer metrics
	MessagesProducedTotal   prometheus.Counter
	MessagesSentTotal       prometheus.Counter
	MessagesErrorTotal      prometheus.Counter
	BatchSizeHistogram      prometheus.Histogram
	PublishLatencyHistogram prometheus.Histogram
	ConnectionErrorsTotal   prometheus.Counter
	RetryAttemptsTotal      prometheus.Counter

	// Performance metrics
	GoroutinesGauge      prometheus.Gauge
	MemoryUsageGauge     prometheus.Gauge
	CPUUsageGauge        prometheus.Gauge
	GCDurationHistogram  prometheus.Histogram

	// HTTP metrics
	HTTPRequestsTotal      *prometheus.CounterVec
	HTTPDurationHistogram  *prometheus.HistogramVec
	HTTPRequestSizeBytes   *prometheus.HistogramVec
	HTTPResponseSizeBytes  *prometheus.HistogramVec
}

// NewManager creates a new metrics manager
func NewManager(logger *zap.Logger) *Manager {
	registry := prometheus.NewRegistry()

	m := &Manager{
		registry: registry,
		logger:   logger,
	}

	m.initializeMetrics()
	m.registerMetrics()

	return m
}

// initializeMetrics creates all Prometheus metrics
func (m *Manager) initializeMetrics() {
	// Producer metrics
	m.MessagesProducedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_producer_messages_produced_total",
		Help: "Total number of messages produced",
	})

	m.MessagesSentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_producer_messages_sent_total",
		Help: "Total number of messages successfully sent to Kafka",
	})

	m.MessagesErrorTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_producer_messages_error_total",
		Help: "Total number of message send errors",
	})

	m.BatchSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_producer_batch_size",
		Help:    "Distribution of message batch sizes",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})

	m.PublishLatencyHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_producer_publish_latency_seconds",
		Help:    "Message publish latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to 16s
	})

	m.ConnectionErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_producer_connection_errors_total",
		Help: "Total number of Kafka connection errors",
	})

	m.RetryAttemptsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_producer_retry_attempts_total",
		Help: "Total number of message retry attempts",
	})

	// Performance metrics
	m.GoroutinesGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_producer_goroutines_count",
		Help: "Current number of goroutines",
	})

	m.MemoryUsageGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_producer_memory_usage_bytes",
		Help: "Current memory usage in bytes",
	})

	m.CPUUsageGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_producer_cpu_usage_percent",
		Help: "Current CPU usage percentage",
	})

	m.GCDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_producer_gc_duration_seconds",
		Help:    "Garbage collection duration",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms to 100s
	})

	// HTTP metrics
	m.HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_producer_http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "endpoint", "status_code"})

	m.HTTPDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_producer_http_request_duration_seconds",
		Help:    "HTTP request duration distribution",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "endpoint"})

	m.HTTPRequestSizeBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_producer_http_request_size_bytes",
		Help:    "HTTP request size distribution",
		Buckets: prometheus.ExponentialBuckets(100, 10, 8), // 100B to 100MB
	}, []string{"method", "endpoint"})

	m.HTTPResponseSizeBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_producer_http_response_size_bytes",
		Help:    "HTTP response size distribution",
		Buckets: prometheus.ExponentialBuckets(100, 10, 8), // 100B to 100MB
	}, []string{"method", "endpoint"})
}

// registerMetrics registers all metrics with the registry
func (m *Manager) registerMetrics() {
	// Producer metrics
	m.registry.MustRegister(m.MessagesProducedTotal)
	m.registry.MustRegister(m.MessagesSentTotal)
	m.registry.MustRegister(m.MessagesErrorTotal)
	m.registry.MustRegister(m.BatchSizeHistogram)
	m.registry.MustRegister(m.PublishLatencyHistogram)
	m.registry.MustRegister(m.ConnectionErrorsTotal)
	m.registry.MustRegister(m.RetryAttemptsTotal)

	// Performance metrics
	m.registry.MustRegister(m.GoroutinesGauge)
	m.registry.MustRegister(m.MemoryUsageGauge)
	m.registry.MustRegister(m.CPUUsageGauge)
	m.registry.MustRegister(m.GCDurationHistogram)

	// HTTP metrics
	m.registry.MustRegister(m.HTTPRequestsTotal)
	m.registry.MustRegister(m.HTTPDurationHistogram)
	m.registry.MustRegister(m.HTTPRequestSizeBytes)
	m.registry.MustRegister(m.HTTPResponseSizeBytes)

	m.logger.Info("Prometheus metrics registered")
}

// Handler returns the HTTP handler for Prometheus metrics
func (m *Manager) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordMessageProduced increments the messages produced counter
func (m *Manager) RecordMessageProduced() {
	m.MessagesProducedTotal.Inc()
}

// RecordMessageSent increments the messages sent counter
func (m *Manager) RecordMessageSent() {
	m.MessagesSentTotal.Inc()
}

// RecordMessageError increments the messages error counter
func (m *Manager) RecordMessageError() {
	m.MessagesErrorTotal.Inc()
}

// RecordBatchSize records a batch size measurement
func (m *Manager) RecordBatchSize(size float64) {
	m.BatchSizeHistogram.Observe(size)
}

// RecordPublishLatency records a publish latency measurement
func (m *Manager) RecordPublishLatency(duration time.Duration) {
	m.PublishLatencyHistogram.Observe(duration.Seconds())
}

// RecordConnectionError increments the connection error counter
func (m *Manager) RecordConnectionError() {
	m.ConnectionErrorsTotal.Inc()
}

// RecordRetryAttempt increments the retry attempt counter
func (m *Manager) RecordRetryAttempt() {
	m.RetryAttemptsTotal.Inc()
}

// UpdateGoroutinesCount updates the goroutines gauge
func (m *Manager) UpdateGoroutinesCount(count float64) {
	m.GoroutinesGauge.Set(count)
}

// UpdateMemoryUsage updates the memory usage gauge
func (m *Manager) UpdateMemoryUsage(bytes float64) {
	m.MemoryUsageGauge.Set(bytes)
}

// UpdateCPUUsage updates the CPU usage gauge
func (m *Manager) UpdateCPUUsage(percent float64) {
	m.CPUUsageGauge.Set(percent)
}

// RecordGCDuration records a garbage collection duration
func (m *Manager) RecordGCDuration(duration time.Duration) {
	m.GCDurationHistogram.Observe(duration.Seconds())
}

// RecordHTTPRequest records HTTP request metrics
func (m *Manager) RecordHTTPRequest(method, endpoint, statusCode string, duration time.Duration, requestSize, responseSize float64) {
	m.HTTPRequestsTotal.WithLabelValues(method, endpoint, statusCode).Inc()
	m.HTTPDurationHistogram.WithLabelValues(method, endpoint).Observe(duration.Seconds())
	m.HTTPRequestSizeBytes.WithLabelValues(method, endpoint).Observe(requestSize)
	m.HTTPResponseSizeBytes.WithLabelValues(method, endpoint).Observe(responseSize)
}

// GetRegistry returns the Prometheus registry for advanced usage
func (m *Manager) GetRegistry() *prometheus.Registry {
	return m.registry
}

// MetricsSnapshot represents a snapshot of current metrics for JSON responses
type MetricsSnapshot struct {
	Timestamp   time.Time            `json:"timestamp"`
	Performance PerformanceMetrics   `json:"performance"`
	Throughput  ThroughputMetrics    `json:"throughput"`
	Errors      ErrorMetrics         `json:"errors"`
	Memory      MemoryMetrics        `json:"memory"`
}

// PerformanceMetrics represents performance-related metrics
type PerformanceMetrics struct {
	AverageLatency int `json:"average_latency_ms"`
	P95Latency     int `json:"p95_latency_ms"`
	P99Latency     int `json:"p99_latency_ms"`
}

// ThroughputMetrics represents throughput-related metrics
type ThroughputMetrics struct {
	MessagesPerSecond int   `json:"messages_per_second"`
	BytesPerSecond    int   `json:"bytes_per_second"`
	TotalMessages     int64 `json:"total_messages"`
}

// ErrorMetrics represents error-related metrics
type ErrorMetrics struct {
	TotalErrors   int64                `json:"total_errors"`
	ErrorRate     float64              `json:"error_rate"`
	ErrorsByType  map[string]int       `json:"errors_by_type"`
}

// MemoryMetrics represents memory-related metrics
type MemoryMetrics struct {
	HeapSize        int64 `json:"heap_size_bytes"`
	AllocatedMemory int64 `json:"allocated_memory_bytes"`
	GCPauses        int   `json:"gc_pauses_last_minute"`
}

// GetSnapshot returns a snapshot of current metrics (placeholder for now)
func (m *Manager) GetSnapshot() MetricsSnapshot {
	// This is a placeholder implementation
	// In a real implementation, you would gather actual metric values
	return MetricsSnapshot{
		Timestamp: time.Now(),
		Performance: PerformanceMetrics{
			AverageLatency: 0,
			P95Latency:     0,
			P99Latency:     0,
		},
		Throughput: ThroughputMetrics{
			MessagesPerSecond: 0,
			BytesPerSecond:    0,
			TotalMessages:     0,
		},
		Errors: ErrorMetrics{
			TotalErrors:  0,
			ErrorRate:    0.0,
			ErrorsByType: make(map[string]int),
		},
		Memory: MemoryMetrics{
			HeapSize:        0,
			AllocatedMemory: 0,
			GCPauses:        0,
		},
	}
}