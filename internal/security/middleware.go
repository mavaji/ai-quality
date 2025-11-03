package security

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// SecurityConfig configures security middleware options
type SecurityConfig struct {
	EnableRateLimit       bool          `yaml:"enable_rate_limit"`
	EnableInputValidation bool          `yaml:"enable_input_validation"`
	StrictMode            bool          `yaml:"strict_mode"`
	MaxRequestSize        int64         `yaml:"max_request_size"`
	ReadTimeout           time.Duration `yaml:"read_timeout"`
}

// SecurityMiddleware provides comprehensive security middleware
type SecurityMiddleware struct {
	config      SecurityConfig
	rateLimiter *RateLimiter
	validator   *InputValidator
	logger      *zap.Logger
}

// NewSecurityMiddleware creates a new security middleware
func NewSecurityMiddleware(config SecurityConfig, logger *zap.Logger) *SecurityMiddleware {
	return &SecurityMiddleware{
		config:      config,
		rateLimiter: NewRateLimiter(config.EnableRateLimit, logger),
		validator:   NewInputValidator(config.StrictMode),
		logger:      logger,
	}
}

// Middleware returns the HTTP middleware handler
func (sm *SecurityMiddleware) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Apply security headers first
			sm.setSecurityHeaders(w)

			// Apply rate limiting
			if sm.config.EnableRateLimit {
				rateLimitHandler := sm.rateLimiter.Middleware()
				rateLimitHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					sm.handleRequest(w, r, next)
				})).ServeHTTP(w, r)
			} else {
				sm.handleRequest(w, r, next)
			}
		})
	}
}

// handleRequest handles the main security processing
func (sm *SecurityMiddleware) handleRequest(w http.ResponseWriter, r *http.Request, next http.Handler) {
	// Validate request size
	if sm.config.MaxRequestSize > 0 && r.ContentLength > sm.config.MaxRequestSize {
		sm.logger.Warn("Request size exceeds limit",
			zap.String("client_ip", sm.getClientIP(r)),
			zap.String("endpoint", r.URL.Path),
			zap.Int64("content_length", r.ContentLength),
			zap.Int64("max_size", sm.config.MaxRequestSize))

		http.Error(w, "Request entity too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Apply input validation for relevant endpoints
	if sm.config.EnableInputValidation && sm.shouldValidateInput(r) {
		if !sm.validateRequestInput(w, r) {
			return // Validation failed, response already sent
		}
	}

	// Log security events
	sm.logSecurityEvent(r)

	next.ServeHTTP(w, r)
}

// shouldValidateInput determines if input validation should be applied
func (sm *SecurityMiddleware) shouldValidateInput(r *http.Request) bool {
	// Only validate POST requests to message endpoints
	if r.Method != http.MethodPost {
		return false
	}

	path := r.URL.Path
	return strings.HasPrefix(path, "/api/v1/messages")
}

// validateRequestInput validates the input of message publishing requests
func (sm *SecurityMiddleware) validateRequestInput(w http.ResponseWriter, r *http.Request) bool {
	// Read and validate request body
	body, err := sm.readRequestBody(r)
	if err != nil {
		sm.logger.Error("Failed to read request body", zap.Error(err))
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return false
	}

	// Parse JSON request
	var requestData map[string]interface{}
	if err := json.Unmarshal(body, &requestData); err != nil {
		sm.logger.Warn("Invalid JSON in request",
			zap.String("client_ip", sm.getClientIP(r)),
			zap.Error(err))

		sm.writeValidationError(w, &ValidationError{
			Field:  "request",
			Reason: "invalid JSON format",
			Hint:   "ensure request body is valid JSON",
		})
		return false
	}

	// Validate based on endpoint
	if strings.HasSuffix(r.URL.Path, "/batch") {
		return sm.validateBatchRequest(w, r, requestData)
	} else {
		return sm.validateSingleMessageRequest(w, r, requestData)
	}
}

// validateSingleMessageRequest validates single message requests
func (sm *SecurityMiddleware) validateSingleMessageRequest(w http.ResponseWriter, r *http.Request, data map[string]interface{}) bool {
	// Validate topic
	if topic, ok := data["topic"].(string); ok {
		if err := sm.validator.ValidateTopicName(topic); err != nil {
			sm.writeValidationError(w, err.(*ValidationError))
			return false
		}
	} else {
		sm.writeValidationError(w, &ValidationError{
			Field:  "topic",
			Reason: "missing or invalid type",
			Hint:   "provide a valid topic name as string",
		})
		return false
	}

	// Validate partition key (optional)
	if partitionKey, ok := data["partitionKey"]; ok {
		if key, ok := partitionKey.(string); ok {
			if err := sm.validator.ValidatePartitionKey(key); err != nil {
				sm.writeValidationError(w, err.(*ValidationError))
				return false
			}
		} else {
			sm.writeValidationError(w, &ValidationError{
				Field:  "partitionKey",
				Reason: "invalid type",
				Hint:   "partition key must be a string",
			})
			return false
		}
	}

	// Validate payload
	if payload, ok := data["payload"].(string); ok {
		if err := sm.validator.ValidateMessagePayload([]byte(payload)); err != nil {
			sm.writeValidationError(w, err.(*ValidationError))
			return false
		}
	} else {
		sm.writeValidationError(w, &ValidationError{
			Field:  "payload",
			Reason: "missing or invalid type",
			Hint:   "provide message payload as string",
		})
		return false
	}

	// Validate headers (optional)
	if headers, ok := data["headers"]; ok {
		if headerMap, ok := headers.(map[string]interface{}); ok {
			stringHeaders := make(map[string]string)
			for k, v := range headerMap {
				if strVal, ok := v.(string); ok {
					stringHeaders[k] = strVal
				} else {
					sm.writeValidationError(w, &ValidationError{
						Field:  "headers",
						Reason: fmt.Sprintf("header '%s' value is not a string", k),
						Hint:   "all header values must be strings",
					})
					return false
				}
			}
			if err := sm.validator.ValidateHeaders(stringHeaders); err != nil {
				sm.writeValidationError(w, err.(*ValidationError))
				return false
			}
		} else {
			sm.writeValidationError(w, &ValidationError{
				Field:  "headers",
				Reason: "invalid type",
				Hint:   "headers must be an object with string keys and values",
			})
			return false
		}
	}

	return true
}

// validateBatchRequest validates batch message requests
func (sm *SecurityMiddleware) validateBatchRequest(w http.ResponseWriter, r *http.Request, data map[string]interface{}) bool {
	// Get messages array
	messages, ok := data["messages"].([]interface{})
	if !ok {
		sm.writeValidationError(w, &ValidationError{
			Field:  "messages",
			Reason: "missing or invalid type",
			Hint:   "provide an array of messages",
		})
		return false
	}

	// Validate message count
	if len(messages) == 0 {
		sm.writeValidationError(w, &ValidationError{
			Field:  "messages",
			Reason: "cannot be empty",
			Hint:   "provide at least one message",
		})
		return false
	}

	if len(messages) > 100 { // Max batch size
		sm.writeValidationError(w, &ValidationError{
			Field:  "messages",
			Reason: "exceeds maximum batch size of 100",
			Hint:   "reduce number of messages in batch",
		})
		return false
	}

	// Validate each message
	for i, msg := range messages {
		msgMap, ok := msg.(map[string]interface{})
		if !ok {
			sm.writeValidationError(w, &ValidationError{
				Field:  fmt.Sprintf("messages[%d]", i),
				Reason: "invalid message format",
				Hint:   "each message must be an object",
			})
			return false
		}

		// Validate each message using the same logic as single messages
		if !sm.validateSingleMessageData(w, msgMap, fmt.Sprintf("messages[%d]", i)) {
			return false
		}
	}

	return true
}

// validateSingleMessageData validates a single message object
func (sm *SecurityMiddleware) validateSingleMessageData(w http.ResponseWriter, data map[string]interface{}, prefix string) bool {
	// Validate topic
	if topic, ok := data["topic"].(string); ok {
		if err := sm.validator.ValidateTopicName(topic); err != nil {
			validationErr := err.(*ValidationError)
			validationErr.Field = prefix + "." + validationErr.Field
			sm.writeValidationError(w, validationErr)
			return false
		}
	} else {
		sm.writeValidationError(w, &ValidationError{
			Field:  prefix + ".topic",
			Reason: "missing or invalid type",
			Hint:   "provide a valid topic name as string",
		})
		return false
	}

	// Validate payload
	if payload, ok := data["payload"].(string); ok {
		if err := sm.validator.ValidateMessagePayload([]byte(payload)); err != nil {
			validationErr := err.(*ValidationError)
			validationErr.Field = prefix + "." + validationErr.Field
			sm.writeValidationError(w, validationErr)
			return false
		}
	} else {
		sm.writeValidationError(w, &ValidationError{
			Field:  prefix + ".payload",
			Reason: "missing or invalid type",
			Hint:   "provide message payload as string",
		})
		return false
	}

	// Other validations (partition key, headers) are optional for batch messages
	return true
}

// readRequestBody reads and limits the request body
func (sm *SecurityMiddleware) readRequestBody(r *http.Request) ([]byte, error) {
	if sm.config.MaxRequestSize > 0 {
		r.Body = http.MaxBytesReader(nil, r.Body, sm.config.MaxRequestSize)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Replace the body so it can be read again
	r.Body = io.NopCloser(bytes.NewReader(body))

	return body, nil
}

// writeValidationError writes a validation error response
func (sm *SecurityMiddleware) writeValidationError(w http.ResponseWriter, err *ValidationError) {
	sm.logger.Warn("Input validation failed",
		zap.String("field", err.Field),
		zap.String("reason", err.Reason),
		zap.Any("value", err.Value))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)

	response := map[string]interface{}{
		"error":     "VALIDATION_ERROR",
		"message":   err.Error(),
		"field":     err.Field,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	json.NewEncoder(w).Encode(response)
}

// setSecurityHeaders sets standard security headers
func (sm *SecurityMiddleware) setSecurityHeaders(w http.ResponseWriter) {
	headers := w.Header()

	// Prevent MIME type sniffing
	headers.Set("X-Content-Type-Options", "nosniff")

	// Prevent clickjacking
	headers.Set("X-Frame-Options", "DENY")

	// XSS protection
	headers.Set("X-XSS-Protection", "1; mode=block")

	// Content Security Policy (restrictive for API)
	headers.Set("Content-Security-Policy", "default-src 'none'")

	// Referrer Policy
	headers.Set("Referrer-Policy", "no-referrer")

	// HSTS (if serving over HTTPS)
	headers.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
}

// logSecurityEvent logs security-related events
func (sm *SecurityMiddleware) logSecurityEvent(r *http.Request) {
	// Only log in debug mode to avoid noise
	sm.logger.Debug("Security middleware processed request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("client_ip", sm.getClientIP(r)),
		zap.String("user_agent", r.UserAgent()),
		zap.Int64("content_length", r.ContentLength))
}

// getClientIP extracts client IP address from request
func (sm *SecurityMiddleware) getClientIP(r *http.Request) string {
	return sm.rateLimiter.getClientIP(r)
}

// GetRateLimiter returns the rate limiter instance
func (sm *SecurityMiddleware) GetRateLimiter() *RateLimiter {
	return sm.rateLimiter
}

// Stop stops the security middleware
func (sm *SecurityMiddleware) Stop() {
	sm.rateLimiter.Stop()
}
