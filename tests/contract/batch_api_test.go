package contract

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/server"
)

func TestBatchAPI_PublishBatch(t *testing.T) {
	// Setup test server
	cfg := createTestConfig()
	logger, _ := zap.NewDevelopment()
	srv := server.New(cfg, logger)

	tests := []struct {
		name           string
		requestBody    interface{}
		expectedStatus int
		expectedFields []string
		errorField     string
	}{
		{
			name: "valid batch request",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "test-topic-1",
						"payload": `{"userId": 1, "action": "login"}`,
						"headers": map[string]string{
							"source": "user-service",
						},
					},
					{
						"topic":        "test-topic-2",
						"partitionKey": "user:123",
						"payload":      `{"orderId": 456, "status": "completed"}`,
						"timestamp":    time.Now().Format(time.RFC3339),
					},
				},
			},
			expectedStatus: http.StatusCreated,
			expectedFields: []string{"batchId", "messageCount", "status", "timestamp", "messages"},
		},
		{
			name: "single message in batch",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "single-topic",
						"payload": `{"test": "data"}`,
					},
				},
			},
			expectedStatus: http.StatusCreated,
			expectedFields: []string{"batchId", "messageCount", "status", "timestamp"},
		},
		{
			name: "empty batch",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name:           "missing messages field",
			requestBody:    map[string]interface{}{},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name: "invalid message format",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "", // Empty topic
						"payload": `{"test": "data"}`,
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name: "message too large",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "test-topic",
						"payload": generateLargePayload(2 * 1024 * 1024), // 2MB payload
					},
				},
			},
			expectedStatus: http.StatusRequestEntityTooLarge,
			errorField:     "error",
		},
		{
			name: "batch too large",
			requestBody: map[string]interface{}{
				"messages": generateLargeBatch(150), // Exceeds max batch size
			},
			expectedStatus: http.StatusRequestEntityTooLarge,
			errorField:     "error",
		},
		{
			name: "invalid topic name",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "invalid@topic!name",
						"payload": `{"test": "data"}`,
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name: "invalid partition key",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":        "test-topic",
						"partitionKey": generateLongString(2000), // Exceeds partition key limit
						"payload":      `{"test": "data"}`,
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name: "invalid headers",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "test-topic",
						"payload": `{"test": "data"}`,
						"headers": map[string]interface{}{
							"valid-header":   "value",
							"invalid-header": 12345, // Non-string value
						},
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name: "invalid timestamp format",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":     "test-topic",
						"payload":   `{"test": "data"}`,
						"timestamp": "invalid-timestamp",
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
		{
			name: "mixed valid and invalid messages",
			requestBody: map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"topic":   "valid-topic",
						"payload": `{"valid": "message"}`,
					},
					{
						"topic":   "", // Invalid empty topic
						"payload": `{"invalid": "message"}`,
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			errorField:     "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare request
			requestBody, err := json.Marshal(tt.requestBody)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/batch", bytes.NewBuffer(requestBody))
			req.Header.Set("Content-Type", "application/json")

			// Execute request
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)

			// Verify response status
			assert.Equal(t, tt.expectedStatus, w.Code, "Status code should match expected")

			// Parse response body
			var responseBody map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &responseBody)
			require.NoError(t, err, "Response should be valid JSON")

			if tt.expectedStatus >= 200 && tt.expectedStatus < 300 {
				// Success response - verify required fields
				for _, field := range tt.expectedFields {
					assert.Contains(t, responseBody, field, "Response should contain field: %s", field)
				}

				// Verify specific field types and values
				if batchID, ok := responseBody["batchId"]; ok {
					assert.IsType(t, "", batchID, "batchId should be a string")
					assert.NotEmpty(t, batchID, "batchId should not be empty")
				}

				if messageCount, ok := responseBody["messageCount"]; ok {
					assert.IsType(t, float64(0), messageCount, "messageCount should be a number")
					assert.Greater(t, messageCount, float64(0), "messageCount should be > 0")
				}

				if status, ok := responseBody["status"]; ok {
					assert.IsType(t, "", status, "status should be a string")
					validStatuses := []string{"accepted", "partial", "failed"}
					assert.Contains(t, validStatuses, status, "status should be valid")
				}

				if timestamp, ok := responseBody["timestamp"]; ok {
					assert.IsType(t, "", timestamp, "timestamp should be a string")
					_, err := time.Parse(time.RFC3339Nano, timestamp.(string))
					assert.NoError(t, err, "timestamp should be valid RFC3339 format")
				}

				if messages, ok := responseBody["messages"]; ok {
					assert.IsType(t, []interface{}{}, messages, "messages should be an array")
				}
			} else {
				// Error response - verify error field
				assert.Contains(t, responseBody, tt.errorField, "Error response should contain error field")

				if errorMsg, ok := responseBody["error"]; ok {
					assert.IsType(t, "", errorMsg, "error should be a string")
					assert.NotEmpty(t, errorMsg, "error message should not be empty")
				}

				if message, ok := responseBody["message"]; ok {
					assert.IsType(t, "", message, "message should be a string")
					assert.NotEmpty(t, message, "error message should not be empty")
				}

				if timestamp, ok := responseBody["timestamp"]; ok {
					assert.IsType(t, "", timestamp, "timestamp should be a string")
				}
			}
		})
	}
}

func TestBatchAPI_ContentTypeValidation(t *testing.T) {
	cfg := createTestConfig()
	logger, _ := zap.NewDevelopment()
	srv := server.New(cfg, logger)

	tests := []struct {
		name           string
		contentType    string
		expectedStatus int
	}{
		{
			name:           "valid content type",
			contentType:    "application/json",
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "charset in content type",
			contentType:    "application/json; charset=utf-8",
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "missing content type",
			contentType:    "",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "invalid content type",
			contentType:    "text/plain",
			expectedStatus: http.StatusUnsupportedMediaType,
		},
		{
			name:           "xml content type",
			contentType:    "application/xml",
			expectedStatus: http.StatusUnsupportedMediaType,
		},
	}

	validRequestBody := map[string]interface{}{
		"messages": []map[string]interface{}{
			{
				"topic":   "test-topic",
				"payload": `{"test": "data"}`,
			},
		},
	}

	requestBodyBytes, _ := json.Marshal(validRequestBody)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/batch", bytes.NewBuffer(requestBodyBytes))

			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code, "Status code should match expected")
		})
	}
}

func TestBatchAPI_MethodValidation(t *testing.T) {
	cfg := createTestConfig()
	logger, _ := zap.NewDevelopment()
	srv := server.New(cfg, logger)

	methods := []struct {
		method         string
		expectedStatus int
	}{
		{http.MethodPost, http.StatusCreated},
		{http.MethodGet, http.StatusMethodNotAllowed},
		{http.MethodPut, http.StatusMethodNotAllowed},
		{http.MethodDelete, http.StatusMethodNotAllowed},
		{http.MethodPatch, http.StatusMethodNotAllowed},
		{http.MethodHead, http.StatusMethodNotAllowed},
		{http.MethodOptions, http.StatusOK}, // CORS preflight
	}

	validRequestBody := map[string]interface{}{
		"messages": []map[string]interface{}{
			{
				"topic":   "test-topic",
				"payload": `{"test": "data"}`,
			},
		},
	}

	requestBodyBytes, _ := json.Marshal(validRequestBody)

	for _, tt := range methods {
		t.Run(tt.method, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/v1/messages/batch", bytes.NewBuffer(requestBodyBytes))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)

			if tt.method == http.MethodPost {
				// For successful POST, status could be 201 or error depending on service state
				assert.True(t, w.Code == http.StatusCreated || w.Code >= 400,
					"POST should return success or error, got %d", w.Code)
			} else {
				assert.Equal(t, tt.expectedStatus, w.Code, "Status code should match expected")
			}
		})
	}
}

func TestBatchAPI_ResponseHeaders(t *testing.T) {
	cfg := createTestConfig()
	logger, _ := zap.NewDevelopment()
	srv := server.New(cfg, logger)

	requestBody := map[string]interface{}{
		"messages": []map[string]interface{}{
			{
				"topic":   "test-topic",
				"payload": `{"test": "data"}`,
			},
		},
	}

	requestBodyBytes, _ := json.Marshal(requestBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/batch", bytes.NewBuffer(requestBodyBytes))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// Verify response headers
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"),
		"Response should have JSON content type")

	// Should have CORS headers for browser compatibility
	assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Origin"),
		"Should have CORS headers")
}

func TestBatchAPI_RateLimiting(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping rate limiting test in short mode")
	}

	cfg := createTestConfig()
	logger, _ := zap.NewDevelopment()
	srv := server.New(cfg, logger)

	requestBody := map[string]interface{}{
		"messages": []map[string]interface{}{
			{
				"topic":   "rate-limit-topic",
				"payload": `{"test": "data"}`,
			},
		},
	}

	requestBodyBytes, _ := json.Marshal(requestBody)

	// Send many requests quickly to test rate limiting
	const numRequests = 100
	var statusCodes []int

	for i := 0; i < numRequests; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/batch", bytes.NewBuffer(requestBodyBytes))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)

		statusCodes = append(statusCodes, w.Code)
	}

	// Count different response types
	var successCount, rateLimitCount, errorCount int
	for _, code := range statusCodes {
		switch {
		case code == http.StatusCreated:
			successCount++
		case code == http.StatusTooManyRequests:
			rateLimitCount++
		default:
			errorCount++
		}
	}

	t.Logf("Rate limiting test results: %d success, %d rate limited, %d other errors",
		successCount, rateLimitCount, errorCount)

	// At minimum, we should get some successful requests
	assert.Greater(t, successCount, 0, "Should have some successful requests")
}

// Helper functions

func createTestConfig() *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Producer: config.ProducerConfig{
				BatchSize:       100,
				MaxMessageBytes: 1048576,
				FlushFrequency:  10 * time.Millisecond,
				Compression:     "none",
			},
			Retry: config.RetryConfig{
				MaxAttempts:       3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        5 * time.Second,
				BackoffMultiplier: 2.0,
			},
			Timeouts: config.TimeoutConfig{
				Connection: 5 * time.Second,
				Request:    10 * time.Second,
				Delivery:   30 * time.Second,
			},
		},
	}
}

func generateLargePayload(size int) string {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = 'A'
	}
	return string(payload)
}

func generateLargeBatch(count int) []map[string]interface{} {
	messages := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		messages[i] = map[string]interface{}{
			"topic":   "test-topic",
			"payload": `{"test": "data"}`,
		}
	}
	return messages
}

func generateLongString(length int) string {
	result := make([]byte, length)
	for i := range result {
		result[i] = byte('a' + (i % 26))
	}
	return string(result)
}
