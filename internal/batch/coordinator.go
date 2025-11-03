package batch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"sdd-kafka-producer/internal/models"
)

// Coordinator manages batching of messages for optimal throughput
type Coordinator struct {
	config models.BatchConfig
	logger *zap.Logger
	mutex  sync.RWMutex

	// Batch management
	batches     map[string]*models.Batch // topic -> current batch
	flushTimers map[string]*time.Timer   // topic -> flush timer

	// Channels
	messageQueue chan *MessageRequest
	flushRequest chan string
	stopChan     chan struct{}

	// Callbacks
	batchHandler BatchHandler

	// Metrics
	metrics *BatchMetrics

	// State
	running int32
}

// MessageRequest represents a request to add a message to a batch
type MessageRequest struct {
	Message  *models.Message
	Response chan *BatchResponse
}

// BatchResponse contains the result of adding a message to a batch
type BatchResponse struct {
	BatchID string
	Error   error
}

// BatchHandler is called when a batch is ready to be sent
type BatchHandler func(context.Context, *models.Batch) error

// BatchMetrics tracks batching statistics
type BatchMetrics struct {
	TotalMessages    int64
	TotalBatches     int64
	MessagesPerBatch float64
	FlushesTriggered int64
	TimeoutFlushes   int64
	SizeFlushes      int64
	mutex            sync.RWMutex
}

// CoordinatorConfig contains configuration for the batch coordinator
type CoordinatorConfig struct {
	BatchConfig     models.BatchConfig
	QueueSize       int
	NumWorkers      int
	MetricsInterval time.Duration
}

// DefaultCoordinatorConfig returns default configuration
func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		BatchConfig:     models.DefaultBatchConfig(),
		QueueSize:       10000,
		NumWorkers:      4,
		MetricsInterval: 30 * time.Second,
	}
}

// NewCoordinator creates a new batch coordinator
func NewCoordinator(config CoordinatorConfig, handler BatchHandler, logger *zap.Logger) *Coordinator {
	return &Coordinator{
		config:       config.BatchConfig,
		logger:       logger,
		batches:      make(map[string]*models.Batch),
		flushTimers:  make(map[string]*time.Timer),
		messageQueue: make(chan *MessageRequest, config.QueueSize),
		flushRequest: make(chan string, 100),
		stopChan:     make(chan struct{}),
		batchHandler: handler,
		metrics:      &BatchMetrics{},
	}
}

// Start begins the batch coordination process
func (c *Coordinator) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.running, 0, 1) {
		return fmt.Errorf("coordinator is already running")
	}

	c.logger.Info("Starting batch coordinator",
		zap.Int("max_messages", c.config.MaxMessages),
		zap.Int("max_bytes", c.config.MaxBytes),
		zap.Duration("flush_interval", c.config.FlushInterval))

	// Start worker goroutines
	for i := 0; i < 4; i++ {
		go c.worker(ctx, i)
	}

	// Start flush manager
	go c.flushManager(ctx)

	// Start metrics reporter
	go c.metricsReporter(ctx)

	return nil
}

// Stop stops the batch coordinator gracefully
func (c *Coordinator) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.running, 1, 0) {
		return fmt.Errorf("coordinator is not running")
	}

	c.logger.Info("Stopping batch coordinator")

	// Signal stop
	close(c.stopChan)

	// Flush remaining batches
	c.flushAllBatches(ctx)

	c.logger.Info("Batch coordinator stopped")
	return nil
}

// AddMessage adds a message to a batch
func (c *Coordinator) AddMessage(ctx context.Context, message *models.Message) (string, error) {
	if atomic.LoadInt32(&c.running) == 0 {
		return "", fmt.Errorf("coordinator is not running")
	}

	request := &MessageRequest{
		Message:  message,
		Response: make(chan *BatchResponse, 1),
	}

	select {
	case c.messageQueue <- request:
		// Request queued successfully
	case <-ctx.Done():
		return "", ctx.Err()
	case <-c.stopChan:
		return "", fmt.Errorf("coordinator is stopping")
	}

	// Wait for response
	select {
	case response := <-request.Response:
		return response.BatchID, response.Error
	case <-ctx.Done():
		return "", ctx.Err()
	case <-c.stopChan:
		return "", fmt.Errorf("coordinator is stopping")
	}
}

// FlushTopic forces all batches for a topic to be sent
func (c *Coordinator) FlushTopic(topic string) {
	select {
	case c.flushRequest <- topic:
		// Flush request queued
	default:
		c.logger.Warn("Flush request queue full, dropping flush request", zap.String("topic", topic))
	}
}

// FlushAll forces all current batches to be sent
func (c *Coordinator) FlushAll(ctx context.Context) {
	c.flushAllBatches(ctx)
}

// GetMetrics returns current batching metrics
func (c *Coordinator) GetMetrics() BatchMetrics {
	c.metrics.mutex.RLock()
	defer c.metrics.mutex.RUnlock()

	metrics := *c.metrics
	if metrics.TotalBatches > 0 {
		metrics.MessagesPerBatch = float64(metrics.TotalMessages) / float64(metrics.TotalBatches)
	}
	return metrics
}

// worker processes message requests
func (c *Coordinator) worker(ctx context.Context, workerID int) {
	c.logger.Debug("Starting batch worker", zap.Int("worker_id", workerID))
	defer c.logger.Debug("Batch worker stopped", zap.Int("worker_id", workerID))

	for {
		select {
		case request := <-c.messageQueue:
			c.processMessageRequest(ctx, request)

		case <-ctx.Done():
			return

		case <-c.stopChan:
			return
		}
	}
}

// processMessageRequest handles a single message request
func (c *Coordinator) processMessageRequest(ctx context.Context, request *MessageRequest) {
	response := &BatchResponse{}

	c.mutex.Lock()
	batch, exists := c.batches[request.Message.Topic]

	if !exists || !batch.CanAccept(request.Message) {
		// Need to create new batch or flush current one
		if exists {
			// Flush current batch
			go c.sendBatch(ctx, batch)
			c.stopFlushTimer(request.Message.Topic)
		}

		// Create new batch
		batchID := c.generateBatchID(request.Message.Topic)
		batch = models.NewBatch(batchID, request.Message.Topic, nil)
		batch.MaxMessages = c.config.MaxMessages
		batch.MaxBytes = c.config.MaxBytes
		c.batches[request.Message.Topic] = batch

		c.logger.Debug("Created new batch",
			zap.String("batch_id", batchID),
			zap.String("topic", request.Message.Topic))
	}

	// Add message to batch
	err := batch.AddMessage(request.Message)
	if err != nil {
		c.mutex.Unlock()
		response.Error = fmt.Errorf("failed to add message to batch: %w", err)
		request.Response <- response
		return
	}

	response.BatchID = batch.ID
	batchTopic := batch.Topic
	shouldFlush := batch.IsFull()

	c.mutex.Unlock()

	// Start or restart flush timer
	c.startFlushTimer(batchTopic)

	// If batch is full, flush immediately
	if shouldFlush {
		c.logger.Debug("Batch is full, flushing immediately",
			zap.String("batch_id", batch.ID),
			zap.String("topic", batchTopic))
		c.FlushTopic(batchTopic)
		atomic.AddInt64(&c.metrics.SizeFlushes, 1)
	}

	// Update metrics
	atomic.AddInt64(&c.metrics.TotalMessages, 1)

	request.Response <- response
}

// flushManager handles flush requests and timeouts
func (c *Coordinator) flushManager(ctx context.Context) {
	c.logger.Debug("Starting flush manager")
	defer c.logger.Debug("Flush manager stopped")

	for {
		select {
		case topic := <-c.flushRequest:
			c.flushTopicBatch(ctx, topic)

		case <-ctx.Done():
			return

		case <-c.stopChan:
			return
		}
	}
}

// flushTopicBatch flushes the current batch for a topic
func (c *Coordinator) flushTopicBatch(ctx context.Context, topic string) {
	c.mutex.Lock()
	batch, exists := c.batches[topic]
	if exists {
		delete(c.batches, topic)
		c.stopFlushTimer(topic)
	}
	c.mutex.Unlock()

	if exists && batch.MessageCount() > 0 {
		c.logger.Debug("Flushing batch",
			zap.String("batch_id", batch.ID),
			zap.String("topic", topic),
			zap.Int("message_count", batch.MessageCount()))

		go c.sendBatch(ctx, batch)
		atomic.AddInt64(&c.metrics.FlushesTriggered, 1)
	}
}

// flushAllBatches flushes all current batches
func (c *Coordinator) flushAllBatches(ctx context.Context) {
	c.mutex.Lock()
	batches := make([]*models.Batch, 0, len(c.batches))
	for topic, batch := range c.batches {
		if batch.MessageCount() > 0 {
			batches = append(batches, batch)
		}
		delete(c.batches, topic)
		c.stopFlushTimer(topic)
	}
	c.mutex.Unlock()

	// Send all batches
	for _, batch := range batches {
		c.sendBatch(ctx, batch)
	}
}

// sendBatch sends a batch using the configured handler
func (c *Coordinator) sendBatch(ctx context.Context, batch *models.Batch) {
	batch.MarkSent()

	err := c.batchHandler(ctx, batch)
	if err != nil {
		c.logger.Error("Failed to send batch",
			zap.String("batch_id", batch.ID),
			zap.String("topic", batch.Topic),
			zap.Error(err))
	} else {
		batch.MarkCompleted()
		c.logger.Debug("Batch sent successfully",
			zap.String("batch_id", batch.ID),
			zap.String("topic", batch.Topic),
			zap.Int("message_count", batch.MessageCount()),
			zap.Duration("processing_time", batch.ProcessingDuration()))
	}

	atomic.AddInt64(&c.metrics.TotalBatches, 1)
}

// startFlushTimer starts or restarts the flush timer for a topic
func (c *Coordinator) startFlushTimer(topic string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Stop existing timer
	if timer, exists := c.flushTimers[topic]; exists {
		timer.Stop()
	}

	// Start new timer
	timer := time.AfterFunc(c.config.FlushInterval, func() {
		c.logger.Debug("Flush timer expired", zap.String("topic", topic))
		c.FlushTopic(topic)
		atomic.AddInt64(&c.metrics.TimeoutFlushes, 1)
	})

	c.flushTimers[topic] = timer
}

// stopFlushTimer stops the flush timer for a topic
func (c *Coordinator) stopFlushTimer(topic string) {
	if timer, exists := c.flushTimers[topic]; exists {
		timer.Stop()
		delete(c.flushTimers, topic)
	}
}

// generateBatchID generates a unique batch ID
func (c *Coordinator) generateBatchID(topic string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("batch-%s-%d", topic, timestamp)
}

// metricsReporter periodically reports metrics
func (c *Coordinator) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := c.GetMetrics()
			c.logger.Info("Batch coordinator metrics",
				zap.Int64("total_messages", metrics.TotalMessages),
				zap.Int64("total_batches", metrics.TotalBatches),
				zap.Float64("avg_messages_per_batch", metrics.MessagesPerBatch),
				zap.Int64("flushes_triggered", metrics.FlushesTriggered),
				zap.Int64("timeout_flushes", metrics.TimeoutFlushes),
				zap.Int64("size_flushes", metrics.SizeFlushes))

		case <-ctx.Done():
			return

		case <-c.stopChan:
			return
		}
	}
}

// GetActiveBatches returns information about currently active batches
func (c *Coordinator) GetActiveBatches() map[string]BatchInfo {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	info := make(map[string]BatchInfo)
	for topic, batch := range c.batches {
		info[topic] = BatchInfo{
			BatchID:      batch.ID,
			Topic:        topic,
			MessageCount: batch.MessageCount(),
			TotalBytes:   batch.TotalBytes,
			CreatedAt:    batch.CreatedAt,
			Age:          time.Since(batch.CreatedAt),
		}
	}
	return info
}

// BatchInfo contains information about a batch
type BatchInfo struct {
	BatchID      string        `json:"batch_id"`
	Topic        string        `json:"topic"`
	MessageCount int           `json:"message_count"`
	TotalBytes   int           `json:"total_bytes"`
	CreatedAt    time.Time     `json:"created_at"`
	Age          time.Duration `json:"age"`
}
