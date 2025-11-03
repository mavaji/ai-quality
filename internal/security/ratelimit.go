package security

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// RateLimit represents rate limiting configuration
type RateLimit struct {
	RequestsPerSecond int           `json:"requests_per_second"`
	BurstSize         int           `json:"burst_size"`
	WindowSize        time.Duration `json:"window_size"`
}

// RateLimiter manages rate limiting for HTTP requests
type RateLimiter struct {
	limits  map[string]*RateLimit    // endpoint -> rate limit
	clients map[string]*ClientBucket // client IP -> bucket
	mu      sync.RWMutex
	logger  *zap.Logger
	enabled bool
	// Cleanup settings
	cleanupInterval time.Duration
	clientTimeout   time.Duration
	stopCleanup     chan bool
}

// ClientBucket tracks rate limiting state for a client
type ClientBucket struct {
	tokens     int
	lastRefill time.Time
	mu         sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(enabled bool, logger *zap.Logger) *RateLimiter {
	rl := &RateLimiter{
		limits:          make(map[string]*RateLimit),
		clients:         make(map[string]*ClientBucket),
		logger:          logger,
		enabled:         enabled,
		cleanupInterval: 5 * time.Minute,
		clientTimeout:   30 * time.Minute,
		stopCleanup:     make(chan bool, 1),
	}

	// Set default limits
	rl.setDefaultLimits()

	// Start cleanup goroutine
	if enabled {
		go rl.cleanupRoutine()
	}

	return rl
}

// setDefaultLimits sets reasonable default rate limits for different endpoints
func (rl *RateLimiter) setDefaultLimits() {
	// Individual message publishing - moderate rate limit
	rl.limits["/api/v1/messages"] = &RateLimit{
		RequestsPerSecond: 100,
		BurstSize:         200,
		WindowSize:        time.Second,
	}

	// Batch message publishing - lower rate limit (more resource intensive)
	rl.limits["/api/v1/messages/batch"] = &RateLimit{
		RequestsPerSecond: 20,
		BurstSize:         50,
		WindowSize:        time.Second,
	}

	// Health checks - higher rate limit
	rl.limits["/api/v1/health"] = &RateLimit{
		RequestsPerSecond: 1000,
		BurstSize:         1000,
		WindowSize:        time.Second,
	}

	// Metrics endpoint - moderate rate limit
	rl.limits["/api/v1/metrics"] = &RateLimit{
		RequestsPerSecond: 10,
		BurstSize:         20,
		WindowSize:        time.Second,
	}

	// Configuration endpoint - low rate limit (sensitive)
	rl.limits["/api/v1/config"] = &RateLimit{
		RequestsPerSecond: 5,
		BurstSize:         10,
		WindowSize:        time.Second,
	}

	// Message status endpoint - moderate rate limit
	rl.limits["/api/v1/messages/*/status"] = &RateLimit{
		RequestsPerSecond: 50,
		BurstSize:         100,
		WindowSize:        time.Second,
	}
}

// SetLimit sets a custom rate limit for an endpoint
func (rl *RateLimiter) SetLimit(endpoint string, limit *RateLimit) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limits[endpoint] = limit
}

// Allow checks if a request should be allowed
func (rl *RateLimiter) Allow(clientIP, endpoint string) (bool, *RateLimitResult) {
	if !rl.enabled {
		return true, &RateLimitResult{
			Allowed:   true,
			Remaining: -1, // Indicate unlimited
			ResetTime: time.Time{},
		}
	}

	// Get rate limit for endpoint
	limit := rl.getLimitForEndpoint(endpoint)
	if limit == nil {
		// No limit configured, allow by default but log
		rl.logger.Debug("No rate limit configured for endpoint",
			zap.String("endpoint", endpoint),
			zap.String("client_ip", clientIP))
		return true, &RateLimitResult{
			Allowed:   true,
			Remaining: -1,
			ResetTime: time.Time{},
		}
	}

	// Get or create client bucket
	bucket := rl.getOrCreateBucket(clientIP)

	// Check rate limit
	return rl.checkLimit(bucket, limit)
}

// getLimitForEndpoint gets the rate limit for an endpoint (with pattern matching)
func (rl *RateLimiter) getLimitForEndpoint(endpoint string) *RateLimit {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	// Try exact match first
	if limit, exists := rl.limits[endpoint]; exists {
		return limit
	}

	// Try pattern matching for endpoints with wildcards
	for pattern, limit := range rl.limits {
		if rl.matchesPattern(endpoint, pattern) {
			return limit
		}
	}

	return nil
}

// matchesPattern checks if an endpoint matches a pattern (simple wildcard support)
func (rl *RateLimiter) matchesPattern(endpoint, pattern string) bool {
	// Handle wildcard patterns like "/api/v1/messages/*/status"
	if strings.Contains(pattern, "*") {
		parts := strings.Split(pattern, "*")
		if len(parts) != 2 {
			return false
		}

		prefix := parts[0]
		suffix := parts[1]

		return strings.HasPrefix(endpoint, prefix) && strings.HasSuffix(endpoint, suffix)
	}

	return endpoint == pattern
}

// getOrCreateBucket gets or creates a rate limiting bucket for a client
func (rl *RateLimiter) getOrCreateBucket(clientIP string) *ClientBucket {
	rl.mu.RLock()
	bucket, exists := rl.clients[clientIP]
	rl.mu.RUnlock()

	if exists {
		return bucket
	}

	// Create new bucket
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check after acquiring write lock
	if bucket, exists := rl.clients[clientIP]; exists {
		return bucket
	}

	bucket = &ClientBucket{
		tokens:     0,
		lastRefill: time.Now(),
	}
	rl.clients[clientIP] = bucket

	rl.logger.Debug("Created new rate limit bucket",
		zap.String("client_ip", clientIP),
		zap.Int("total_clients", len(rl.clients)))

	return bucket
}

// checkLimit checks if request is allowed based on token bucket algorithm
func (rl *RateLimiter) checkLimit(bucket *ClientBucket, limit *RateLimit) (bool, *RateLimitResult) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	now := time.Now()

	// Refill tokens based on time elapsed
	elapsed := now.Sub(bucket.lastRefill)
	tokensToAdd := int(elapsed.Seconds() * float64(limit.RequestsPerSecond))

	if tokensToAdd > 0 {
		bucket.tokens = min(bucket.tokens+tokensToAdd, limit.BurstSize)
		bucket.lastRefill = now
	}

	// Check if we have tokens available
	if bucket.tokens > 0 {
		bucket.tokens--

		// Calculate reset time (when bucket will be full again)
		resetTime := now.Add(time.Duration(float64(limit.BurstSize-bucket.tokens)/float64(limit.RequestsPerSecond)) * time.Second)

		return true, &RateLimitResult{
			Allowed:   true,
			Remaining: bucket.tokens,
			ResetTime: resetTime,
		}
	}

	// Rate limited - calculate when next token will be available
	nextTokenTime := bucket.lastRefill.Add(time.Duration(float64(1)/float64(limit.RequestsPerSecond)) * time.Second)

	return false, &RateLimitResult{
		Allowed:   false,
		Remaining: 0,
		ResetTime: nextTokenTime,
	}
}

// RateLimitResult contains the result of a rate limit check
type RateLimitResult struct {
	Allowed   bool
	Remaining int       // Remaining requests in current window (-1 if unlimited)
	ResetTime time.Time // When the rate limit resets
}

// Middleware creates an HTTP middleware for rate limiting
func (rl *RateLimiter) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !rl.enabled {
				next.ServeHTTP(w, r)
				return
			}

			// Get client IP
			clientIP := rl.getClientIP(r)

			// Check rate limit
			allowed, result := rl.Allow(clientIP, r.URL.Path)

			// Set rate limit headers
			rl.setRateLimitHeaders(w, result)

			if !allowed {
				rl.logger.Warn("Rate limit exceeded",
					zap.String("client_ip", clientIP),
					zap.String("endpoint", r.URL.Path),
					zap.String("method", r.Method),
					zap.Time("reset_time", result.ResetTime))

				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// getClientIP extracts the client IP from the request
func (rl *RateLimiter) getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (proxy/load balancer)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			ip := strings.TrimSpace(ips[0])
			if net.ParseIP(ip) != nil {
				return ip
			}
		}
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if net.ParseIP(xri) != nil {
			return xri
		}
	}

	// Fall back to remote address
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return ip
}

// setRateLimitHeaders sets standard rate limiting headers
func (rl *RateLimiter) setRateLimitHeaders(w http.ResponseWriter, result *RateLimitResult) {
	if result.Remaining >= 0 {
		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
	}

	if !result.ResetTime.IsZero() {
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", result.ResetTime.Unix()))
	}

	// Set retry-after header for rate limited requests
	if !result.Allowed && !result.ResetTime.IsZero() {
		retryAfter := int(time.Until(result.ResetTime).Seconds())
		if retryAfter < 0 {
			retryAfter = 1
		}
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
	}
}

// cleanupRoutine periodically removes old client buckets
func (rl *RateLimiter) cleanupRoutine() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.cleanup()
		case <-rl.stopCleanup:
			return
		}
	}
}

// cleanup removes old inactive client buckets
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cutoff := time.Now().Add(-rl.clientTimeout)
	removed := 0

	for ip, bucket := range rl.clients {
		bucket.mu.Lock()
		if bucket.lastRefill.Before(cutoff) {
			delete(rl.clients, ip)
			removed++
		}
		bucket.mu.Unlock()
	}

	if removed > 0 {
		rl.logger.Debug("Cleaned up inactive rate limit buckets",
			zap.Int("removed", removed),
			zap.Int("remaining", len(rl.clients)))
	}
}

// Stop stops the rate limiter and cleanup routines
func (rl *RateLimiter) Stop() {
	if rl.enabled {
		select {
		case rl.stopCleanup <- true:
		default:
		}
	}
}

// GetStats returns rate limiting statistics
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled":        rl.enabled,
		"client_count":   len(rl.clients),
		"endpoint_count": len(rl.limits),
	}

	// Add per-endpoint limits
	endpoints := make(map[string]interface{})
	for endpoint, limit := range rl.limits {
		endpoints[endpoint] = map[string]interface{}{
			"requests_per_second": limit.RequestsPerSecond,
			"burst_size":          limit.BurstSize,
			"window_size":         limit.WindowSize.String(),
		}
	}
	stats["endpoints"] = endpoints

	return stats
}

// UpdateConfiguration updates rate limiter configuration
func (rl *RateLimiter) UpdateConfiguration(config map[string]*RateLimit) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for endpoint, limit := range config {
		rl.limits[endpoint] = limit
	}

	rl.logger.Info("Updated rate limiter configuration",
		zap.Int("endpoints", len(config)))
}
