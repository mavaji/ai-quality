package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/health"
)

// Server represents the HTTP server
type Server struct {
	server        *http.Server
	logger        *zap.Logger
	config        config.ServerConfig
	healthManager *health.Manager
	mux           *http.ServeMux
}

// New creates a new HTTP server
func New(cfg config.ServerConfig, logger *zap.Logger, healthManager *health.Manager) *Server {
	mux := http.NewServeMux()

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	s := &Server{
		server:        server,
		logger:        logger,
		config:        cfg,
		healthManager: healthManager,
		mux:           mux,
	}

	s.setupRoutes()
	return s
}

// setupRoutes configures HTTP routes
func (s *Server) setupRoutes() {
	// Add middleware wrapper for all routes
	handler := s.loggingMiddleware(s.corsMiddleware(s.mux))
	s.server.Handler = handler

	// Health check endpoint
	s.mux.Handle("/api/v1/health", s.healthManager)

	// API v1 endpoints (placeholder for implementation)
	apiRoutes := map[string]http.HandlerFunc{
		"/api/v1/messages":       s.notImplementedHandler,
		"/api/v1/messages/batch": s.notImplementedHandler,
		"/api/v1/metrics":        s.notImplementedHandler,
		"/api/v1/config":         s.notImplementedHandler,
	}

	for pattern, handler := range apiRoutes {
		s.mux.HandleFunc(pattern, handler)
	}

	// Default handler for unmatched routes
	s.mux.HandleFunc("/", s.notFoundHandler)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP server",
		zap.String("address", s.server.Addr),
		zap.Duration("read_timeout", s.config.ReadTimeout),
		zap.Duration("write_timeout", s.config.WriteTimeout),
	)

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server")

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	s.logger.Info("HTTP server shutdown complete")
	return nil
}

// Middleware functions

// loggingMiddleware logs HTTP requests
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap ResponseWriter to capture status code
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrappedWriter, r)

		duration := time.Since(start)

		s.logger.Info("HTTP request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.String("user_agent", r.UserAgent()),
			zap.Int("status_code", wrappedWriter.statusCode),
			zap.Duration("duration", duration),
		)
	})
}

// corsMiddleware adds CORS headers
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Handler functions

// notFoundHandler handles 404 errors
func (s *Server) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	s.writeErrorResponse(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", "The requested endpoint was not found", nil)
}

// notImplementedHandler handles endpoints not yet implemented
func (s *Server) notImplementedHandler(w http.ResponseWriter, r *http.Request) {
	s.writeErrorResponse(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "This endpoint is not yet implemented", nil)
}

// Response helper functions

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error     string      `json:"error"`
	Message   string      `json:"message"`
	Details   interface{} `json:"details,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// writeErrorResponse writes a standardized error response
func (s *Server) writeErrorResponse(w http.ResponseWriter, statusCode int, errorCode, message string, details interface{}) {
	response := ErrorResponse{
		Error:     errorCode,
		Message:   message,
		Details:   details,
		Timestamp: time.Now(),
	}

	s.writeJSONResponse(w, statusCode, response)
}

// writeJSONResponse writes a JSON response
func (s *Server) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("Failed to encode JSON response", zap.Error(err))
		// Write a simple error response if JSON encoding fails
		fmt.Fprintf(w, `{"error": "Internal server error"}`)
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// AddHandler allows adding custom handlers to the server
func (s *Server) AddHandler(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, s.loggingMiddleware(handler))
}

// GetMux returns the HTTP mux for advanced configuration
func (s *Server) GetMux() *http.ServeMux {
	return s.mux
}
