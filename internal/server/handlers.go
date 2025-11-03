package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"sdd-kafka-producer/internal/models"
	"sdd-kafka-producer/internal/producer"
)

// HandlerManager manages HTTP handlers and their dependencies
type HandlerManager struct {
	producer producer.Producer
	tracker  *producer.MessageTracker
	logger   *zap.Logger
	server   *Server
}

// NewHandlerManager creates a new handler manager
func NewHandlerManager(producer producer.Producer, tracker *producer.MessageTracker, logger *zap.Logger, server *Server) *HandlerManager {
	return &HandlerManager{
		producer: producer,
		tracker:  tracker,
		logger:   logger,
		server:   server,
	}
}

// RegisterRoutes registers all message-related routes
func (h *HandlerManager) RegisterRoutes() {
	// Message endpoints
	h.server.AddHandler("/api/v1/messages", http.HandlerFunc(h.handleMessages))
	h.server.AddHandler("/api/v1/messages/batch", http.HandlerFunc(h.handleMessagesBatch))
	h.server.AddHandler("/api/v1/messages/", http.HandlerFunc(h.handleMessageStatus))
	h.server.AddHandler("/api/v1/metrics", http.HandlerFunc(h.handleMetrics))
	h.server.AddHandler("/api/v1/config", http.HandlerFunc(h.handleConfig))
}

// MessageRequest represents the API request structure for posting messages
type MessageRequest struct {
	Topic     string            `json:"topic"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp *time.Time        `json:"timestamp,omitempty"`
}

// MessageResponse represents the API response structure
type MessageResponse struct {
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	DeliveredAt time.Time `json:"delivered_at"`
}

// BatchMessageRequest represents a request for multiple messages
type BatchMessageRequest struct {
	Messages []MessageRequest `json:"messages"`
}

// BatchMessageResponse represents a response for batch publishing
type BatchMessageResponse struct {
	Results     []MessageResponse `json:"results"`
	SuccessCount int              `json:"success_count"`
	FailureCount int              `json:"failure_count"`
	TotalCount   int              `json:"total_count"`
}

// handleMessages handles the POST /api/v1/messages endpoint
func (h *HandlerManager) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.postMessage(w, r)
	case http.MethodOptions:
		// CORS preflight is handled by middleware
		w.WriteHeader(http.StatusOK)
	default:
		h.server.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", 
			"Only POST method is supported for this endpoint", nil)
	}
}

// postMessage handles posting a single message
func (h *HandlerManager) postMessage(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req MessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.server.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", 
			"Request body is not valid JSON", err.Error())
		return
	}

	// Validate request
	if err := h.validateMessageRequest(&req); err != nil {
		h.server.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", 
			err.Error(), nil)
		return
	}

	// Convert request to message model
	message, err := h.requestToMessage(&req)
	if err != nil {
		h.server.writeErrorResponse(w, http.StatusBadRequest, "INVALID_MESSAGE", 
			err.Error(), nil)
		return
	}

	// Publish message
	ctx := r.Context()
	receipt, err := h.producer.PublishMessage(ctx, message)
	if err != nil {
		h.logger.Error("Failed to publish message", 
			zap.String("topic", message.Topic),
			zap.String("key", message.Key),
			zap.Error(err))

		// Determine appropriate HTTP status code based on error
		statusCode := http.StatusInternalServerError
		errorCode := "PUBLISH_ERROR"
		
		// Handle specific error cases
		if ctx.Err() != nil {
			if ctx.Err() == context.DeadlineExceeded {
				statusCode = http.StatusRequestTimeout
				errorCode = "TIMEOUT"
			} else {
				statusCode = http.StatusBadRequest
				errorCode = "REQUEST_CANCELLED"
			}
		}

		h.server.writeErrorResponse(w, statusCode, errorCode, 
			"Failed to publish message", err.Error())
		return
	}

	// Convert receipt to response
	response := h.receiptToResponse(receipt)

	// Write successful response
	h.server.writeJSONResponse(w, http.StatusCreated, response)

	h.logger.Info("Message published successfully via API",
		zap.String("message_id", receipt.MessageID),
		zap.String("topic", receipt.Topic),
		zap.Int32("partition", receipt.Partition),
		zap.Int64("offset", receipt.Offset))
}

// validateMessageRequest validates the incoming message request
func (h *HandlerManager) validateMessageRequest(req *MessageRequest) error {
	if req.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	if req.Value == "" {
		return fmt.Errorf("value is required")
	}

	// Validate topic name format
	if len(req.Topic) > 255 {
		return fmt.Errorf("topic name cannot exceed 255 characters")
	}

	// Validate key length
	if len(req.Key) > 1024 {
		return fmt.Errorf("key cannot exceed 1024 characters")
	}

	// Validate value size (1MB limit)
	if len(req.Value) > 1024*1024 {
		return fmt.Errorf("message value cannot exceed 1MB")
	}

	// Validate headers
	if req.Headers != nil {
		if len(req.Headers) > 50 {
			return fmt.Errorf("cannot have more than 50 headers")
		}

		for key, value := range req.Headers {
			if len(key) > 256 {
				return fmt.Errorf("header key '%s' exceeds 256 characters", key)
			}
			if len(value) > 1024 {
				return fmt.Errorf("header value for '%s' exceeds 1024 characters", key)
			}
		}
	}

	// Validate timestamp if provided
	if req.Timestamp != nil {
		now := time.Now()
		if req.Timestamp.After(now.Add(5 * time.Minute)) {
			return fmt.Errorf("timestamp cannot be more than 5 minutes in the future")
		}
		if req.Timestamp.Before(now.Add(-24 * time.Hour)) {
			return fmt.Errorf("timestamp cannot be more than 24 hours in the past")
		}
	}

	return nil
}

// requestToMessage converts a MessageRequest to a Message model
func (h *HandlerManager) requestToMessage(req *MessageRequest) (*models.Message, error) {
	message := &models.Message{
		Topic:   req.Topic,
		Key:     req.Key,
		Value:   []byte(req.Value),
		Headers: req.Headers,
	}

	// Set timestamp
	if req.Timestamp != nil {
		message.Timestamp = *req.Timestamp
	} else {
		message.Timestamp = time.Now()
	}

	// Validate the created message
	if err := message.Validate(); err != nil {
		return nil, fmt.Errorf("message validation failed: %w", err)
	}

	return message, nil
}

// receiptToResponse converts a DeliveryReceipt to a MessageResponse
func (h *HandlerManager) receiptToResponse(receipt *models.DeliveryReceipt) MessageResponse {
	status := "success"
	if receipt.Status != models.DeliveryStatusSuccess {
		status = string(receipt.Status)
	}

	return MessageResponse{
		MessageID:   receipt.MessageID,
		Status:      status,
		Topic:       receipt.Topic,
		Partition:   receipt.Partition,
		Offset:      receipt.Offset,
		DeliveredAt: receipt.DeliveredAt,
	}
}

// handleMessagesBatch handles the POST /api/v1/messages/batch endpoint
func (h *HandlerManager) handleMessagesBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.server.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", 
			"Only POST method is supported for this endpoint", nil)
		return
	}

	// Parse request body
	var req BatchMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.server.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", 
			"Request body is not valid JSON", err.Error())
		return
	}

	// Validate batch request
	if err := h.validateBatchRequest(&req); err != nil {
		h.server.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", 
			err.Error(), nil)
		return
	}

	// Process messages in batch
	results := make([]MessageResponse, len(req.Messages))
	successCount := 0
	failureCount := 0

	ctx := r.Context()

	for i, messageReq := range req.Messages {
		// Convert to message
		message, err := h.requestToMessage(&messageReq)
		if err != nil {
			// Create error response for this message
			results[i] = MessageResponse{
				MessageID:   fmt.Sprintf("error-%d", i),
				Status:      "failed",
				Topic:       messageReq.Topic,
				Partition:   -1,
				Offset:      -1,
				DeliveredAt: time.Now(),
			}
			failureCount++
			continue
		}

		// Publish message
		receipt, err := h.producer.PublishMessage(ctx, message)
		if err != nil {
			// Create error response
			results[i] = MessageResponse{
				MessageID:   fmt.Sprintf("error-%d", i),
				Status:      "failed",
				Topic:       message.Topic,
				Partition:   -1,
				Offset:      -1,
				DeliveredAt: time.Now(),
			}
			failureCount++

			h.logger.Error("Failed to publish message in batch", 
				zap.Int("message_index", i),
				zap.String("topic", message.Topic),
				zap.Error(err))
		} else {
			// Create success response
			results[i] = h.receiptToResponse(receipt)
			successCount++
		}
	}

	// Create batch response
	response := BatchMessageResponse{
		Results:      results,
		SuccessCount: successCount,
		FailureCount: failureCount,
		TotalCount:   len(req.Messages),
	}

	// Determine response status code
	statusCode := http.StatusCreated
	if failureCount > 0 {
		if successCount == 0 {
			statusCode = http.StatusBadRequest // All failed
		} else {
			statusCode = http.StatusMultiStatus // Partial success
		}
	}

	h.server.writeJSONResponse(w, statusCode, response)

	h.logger.Info("Batch message processing completed",
		zap.Int("total", len(req.Messages)),
		zap.Int("success", successCount),
		zap.Int("failed", failureCount))
}

// validateBatchRequest validates a batch message request
func (h *HandlerManager) validateBatchRequest(req *BatchMessageRequest) error {
	if len(req.Messages) == 0 {
		return fmt.Errorf("batch cannot be empty")
	}

	if len(req.Messages) > 100 {
		return fmt.Errorf("batch cannot contain more than 100 messages")
	}

	// Validate each message request
	for i, messageReq := range req.Messages {
		if err := h.validateMessageRequest(&messageReq); err != nil {
			return fmt.Errorf("message at index %d: %w", i, err)
		}
	}

	return nil
}

// handleMetrics handles the GET /api/v1/metrics endpoint
func (h *HandlerManager) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.server.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", 
			"Only GET method is supported for this endpoint", nil)
		return
	}

	// This is a placeholder - in a full implementation, you would
	// gather actual metrics from the metrics manager
	metrics := map[string]interface{}{
		"timestamp": time.Now(),
		"status":    "operational",
		"producer": map[string]interface{}{
			"status": "connected",
			"messages_sent_total": 0,
			"messages_failed_total": 0,
		},
		"system": map[string]interface{}{
			"memory_usage": 0,
			"cpu_usage": 0,
			"goroutines": 0,
		},
	}

	h.server.writeJSONResponse(w, http.StatusOK, metrics)
}

// handleConfig handles the GET /api/v1/config endpoint
func (h *HandlerManager) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.server.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", 
			"Only GET method is supported for this endpoint", nil)
		return
	}

	// Return sanitized configuration (without sensitive data)
	config := map[string]interface{}{
		"timestamp": time.Now(),
		"version":   "1.0.0",
		"endpoints": []string{
			"/api/v1/messages",
			"/api/v1/messages/batch", 
			"/api/v1/metrics",
			"/api/v1/config",
			"/api/v1/health",
		},
		"limits": map[string]interface{}{
			"max_message_size": 1024 * 1024, // 1MB
			"max_batch_size":   100,
			"max_headers":      50,
		},
	}

	h.server.writeJSONResponse(w, http.StatusOK, config)
}

// handleMessageStatus handles GET /api/v1/messages/{id}/status endpoint
func (h *HandlerManager) handleMessageStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.server.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", 
			"Only GET method is supported for this endpoint", nil)
		return
	}

	// Extract message ID from URL path
	// Expected format: /api/v1/messages/{id}/status
	path := r.URL.Path
	if !strings.HasPrefix(path, "/api/v1/messages/") {
		h.server.writeErrorResponse(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", 
			"Invalid endpoint format", nil)
		return
	}

	// Remove prefix to get the rest of the path
	remaining := strings.TrimPrefix(path, "/api/v1/messages/")
	parts := strings.Split(remaining, "/")
	
	if len(parts) != 2 || parts[1] != "status" {
		h.server.writeErrorResponse(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", 
			"Expected format: /api/v1/messages/{id}/status", nil)
		return
	}

	messageID := parts[0]
	if messageID == "" {
		h.server.writeErrorResponse(w, http.StatusBadRequest, "INVALID_MESSAGE_ID", 
			"Message ID cannot be empty", nil)
		return
	}

	// Get status from tracker
	status, err := h.tracker.GetStatus(messageID)
	if err != nil {
		h.server.writeErrorResponse(w, http.StatusNotFound, "MESSAGE_NOT_FOUND", 
			fmt.Sprintf("Message %s not found", messageID), nil)
		return
	}

	h.server.writeJSONResponse(w, http.StatusOK, status)
	
	h.logger.Debug("Message status retrieved",
		zap.String("message_id", messageID),
		zap.String("current_state", string(status.CurrentState)))
}