package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Status represents the health status of a component
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
	StatusDegraded  Status = "degraded"
)

// Check represents a single health check
type Check struct {
	Status  Status        `json:"status"`
	Message string        `json:"message,omitempty"`
	Latency time.Duration `json:"latency,omitempty"`
}

// Response represents the overall health check response
type Response struct {
	Status    Status            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Checks    map[string]Check  `json:"checks"`
}

// Checker interface for health check implementations
type Checker interface {
	Check(ctx context.Context) Check
	Name() string
}

// Manager manages health checks and provides HTTP endpoints
type Manager struct {
	checkers []Checker
	timeout  time.Duration
	logger   *zap.Logger
	mu       sync.RWMutex
}

// NewManager creates a new health check manager
func NewManager(timeout time.Duration, logger *zap.Logger) *Manager {
	return &Manager{
		checkers: make([]Checker, 0),
		timeout:  timeout,
		logger:   logger,
	}
}

// RegisterChecker adds a health checker to the manager
func (m *Manager) RegisterChecker(checker Checker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkers = append(m.checkers, checker)
	m.logger.Info("Registered health checker", zap.String("name", checker.Name()))
}

// CheckHealth runs all health checks and returns the overall status
func (m *Manager) CheckHealth(ctx context.Context) Response {
	m.mu.RLock()
	checkers := make([]Checker, len(m.checkers))
	copy(checkers, m.checkers)
	m.mu.RUnlock()

	response := Response{
		Status:    StatusHealthy,
		Timestamp: time.Now(),
		Checks:    make(map[string]Check),
	}

	// Create timeout context
	checkCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	// Run all checks
	checkResults := make(chan checkResult, len(checkers))
	for _, checker := range checkers {
		go func(c Checker) {
			start := time.Now()
			check := c.Check(checkCtx)
			check.Latency = time.Since(start)
			checkResults <- checkResult{
				name:  c.Name(),
				check: check,
			}
		}(checker)
	}

	// Collect results
	for i := 0; i < len(checkers); i++ {
		result := <-checkResults
		response.Checks[result.name] = result.check

		// Update overall status
		if result.check.Status == StatusUnhealthy {
			response.Status = StatusUnhealthy
		} else if result.check.Status == StatusDegraded && response.Status == StatusHealthy {
			response.Status = StatusDegraded
		}
	}

	return response
}

// ServeHTTP handles HTTP health check requests
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	health := m.CheckHealth(r.Context())

	w.Header().Set("Content-Type", "application/json")
	if health.Status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if err := json.NewEncoder(w).Encode(health); err != nil {
		m.logger.Error("Failed to encode health response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}

	// Log health check
	m.logger.Debug("Health check completed",
		zap.String("status", string(health.Status)),
		zap.Int("checks", len(health.Checks)),
		zap.Duration("duration", time.Since(health.Timestamp)),
	)
}

// checkResult is used internally for collecting check results
type checkResult struct {
	name  string
	check Check
}

// Simple health checker implementation
type SimpleChecker struct {
	name     string
	checkFunc func(ctx context.Context) Check
}

// NewSimpleChecker creates a simple health checker
func NewSimpleChecker(name string, checkFunc func(ctx context.Context) Check) Checker {
	return &SimpleChecker{
		name:      name,
		checkFunc: checkFunc,
	}
}

// Name returns the checker name
func (c *SimpleChecker) Name() string {
	return c.name
}

// Check runs the health check
func (c *SimpleChecker) Check(ctx context.Context) Check {
	return c.checkFunc(ctx)
}

// StaticChecker always returns a fixed status (useful for testing)
type StaticChecker struct {
	name   string
	status Check
}

// NewStaticChecker creates a static health checker
func NewStaticChecker(name string, status Status, message string) Checker {
	return &StaticChecker{
		name: name,
		status: Check{
			Status:  status,
			Message: message,
		},
	}
}

// Name returns the checker name
func (c *StaticChecker) Name() string {
	return c.name
}

// Check returns the static status
func (c *StaticChecker) Check(ctx context.Context) Check {
	return c.status
}