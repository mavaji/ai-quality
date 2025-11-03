package profiling

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"go.uber.org/zap"
)

// Profiler handles application profiling and diagnostics
type Profiler struct {
	server *http.Server
	logger *zap.Logger
	config Config
}

// Config defines profiling configuration
type Config struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
	// Additional profiling options
	BlockProfileRate     int  `yaml:"block_profile_rate"`
	MutexProfileFraction int  `yaml:"mutex_profile_fraction"`
	EnableAllocs         bool `yaml:"enable_allocs"`
}

// New creates a new profiler instance
func New(config Config, logger *zap.Logger) *Profiler {
	return &Profiler{
		config: config,
		logger: logger,
	}
}

// Start starts the profiling server with enhanced endpoints
func (p *Profiler) Start() error {
	if !p.config.Enabled {
		p.logger.Info("Profiling is disabled")
		return nil
	}

	// Configure runtime profiling
	p.configureRuntimeProfiling()

	// Create HTTP server with enhanced profiling endpoints
	mux := http.NewServeMux()

	// Standard pprof endpoints (automatically registered)
	// /debug/pprof/ - index page
	// /debug/pprof/cmdline - command line
	// /debug/pprof/profile - CPU profile
	// /debug/pprof/symbol - symbol lookup
	// /debug/pprof/trace - execution trace

	// Add custom endpoints for enhanced diagnostics
	mux.HandleFunc("/debug/info", p.handleInfo)
	mux.HandleFunc("/debug/gc", p.handleGC)
	mux.HandleFunc("/debug/memory", p.handleMemoryInfo)
	mux.HandleFunc("/debug/goroutines", p.handleGoroutineInfo)
	mux.HandleFunc("/debug/runtime", p.handleRuntimeInfo)
	mux.HandleFunc("/health", p.handleHealth)

	// Add middleware for logging
	handler := p.loggingMiddleware(mux)

	p.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", p.config.Port),
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	p.logger.Info("Starting profiling server",
		zap.Int("port", p.config.Port),
		zap.Bool("allocs_enabled", p.config.EnableAllocs),
		zap.Int("block_profile_rate", p.config.BlockProfileRate),
		zap.Int("mutex_profile_fraction", p.config.MutexProfileFraction),
	)

	go func() {
		if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Error("Profiling server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the profiling server
func (p *Profiler) Stop(ctx context.Context) error {
	if !p.config.Enabled || p.server == nil {
		return nil
	}

	p.logger.Info("Stopping profiling server")

	if err := p.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown profiling server: %w", err)
	}

	return nil
}

// configureRuntimeProfiling sets up runtime profiling parameters
func (p *Profiler) configureRuntimeProfiling() {
	// Enable memory allocation profiling if configured
	if p.config.EnableAllocs {
		runtime.MemProfileRate = 1
	}

	// Set block profile rate
	if p.config.BlockProfileRate > 0 {
		runtime.SetBlockProfileRate(p.config.BlockProfileRate)
	}

	// Set mutex profile fraction
	if p.config.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(p.config.MutexProfileFraction)
	}
}

// loggingMiddleware adds request logging for profiling endpoints
func (p *Profiler) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		next.ServeHTTP(w, r)

		duration := time.Since(start)
		p.logger.Debug("Profiling request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
			zap.Duration("duration", duration),
		)
	})
}

// Enhanced diagnostic endpoints

// handleInfo provides general application information
func (p *Profiler) handleInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"service":       "kafka-producer",
		"go_version":    runtime.Version(),
		"go_arch":       runtime.GOARCH,
		"go_os":         runtime.GOOS,
		"num_cpu":       runtime.NumCPU(),
		"num_goroutine": runtime.NumGoroutine(),
		"timestamp":     time.Now().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
  "service": "%s",
  "go_version": "%s",
  "go_arch": "%s", 
  "go_os": "%s",
  "num_cpu": %d,
  "num_goroutine": %d,
  "timestamp": "%s"
}`,
		info["service"], info["go_version"], info["go_arch"],
		info["go_os"], info["num_cpu"], info["num_goroutine"], info["timestamp"])
}

// handleGC forces garbage collection (for debugging purposes)
func (p *Profiler) handleGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	before := time.Now()
	runtime.GC()
	duration := time.Since(before)

	p.logger.Info("Manual GC triggered via profiling endpoint",
		zap.Duration("duration", duration))

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"message": "GC completed", "duration_ms": %.2f}`,
		float64(duration.Nanoseconds())/1000000.0)
}

// handleMemoryInfo provides detailed memory statistics
func (p *Profiler) handleMemoryInfo(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
  "heap_alloc_bytes": %d,
  "heap_sys_bytes": %d,
  "heap_idle_bytes": %d,
  "heap_inuse_bytes": %d,
  "heap_released_bytes": %d,
  "heap_objects": %d,
  "stack_inuse_bytes": %d,
  "stack_sys_bytes": %d,
  "mspan_inuse_bytes": %d,
  "mspan_sys_bytes": %d,
  "mcache_inuse_bytes": %d,
  "mcache_sys_bytes": %d,
  "gc_sys_bytes": %d,
  "other_sys_bytes": %d,
  "next_gc_bytes": %d,
  "last_gc_ns": %d,
  "pause_total_ns": %d,
  "num_gc": %d,
  "num_forced_gc": %d,
  "gc_cpu_fraction": %f,
  "mallocs": %d,
  "frees": %d
}`,
		m.HeapAlloc, m.HeapSys, m.HeapIdle, m.HeapInuse, m.HeapReleased,
		m.HeapObjects, m.StackInuse, m.StackSys, m.MSpanInuse, m.MSpanSys,
		m.MCacheInuse, m.MCacheSys, m.GCSys, m.OtherSys, m.NextGC,
		m.LastGC, m.PauseTotalNs, m.NumGC, m.NumForcedGC, m.GCCPUFraction,
		m.Mallocs, m.Frees)
}

// handleGoroutineInfo provides goroutine statistics
func (p *Profiler) handleGoroutineInfo(w http.ResponseWriter, r *http.Request) {
	numGoroutines := runtime.NumGoroutine()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
  "num_goroutines": %d,
  "timestamp": "%s"
}`, numGoroutines, time.Now().Format(time.RFC3339))
}

// handleRuntimeInfo provides comprehensive runtime information
func (p *Profiler) handleRuntimeInfo(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
  "version": "%s",
  "arch": "%s",
  "os": "%s",
  "num_cpu": %d,
  "num_goroutine": %d,
  "num_cgo_call": %d,
  "mem_profile_rate": %d,
  "block_profile_rate": %d,
  "mutex_profile_fraction": %d,
  "gc_percent": %d,
  "max_procs": %d,
  "compiler": "%s"
}`,
		runtime.Version(), runtime.GOARCH, runtime.GOOS, runtime.NumCPU(),
		runtime.NumGoroutine(), runtime.NumCgoCall(), runtime.MemProfileRate,
		runtime.SetBlockProfileRate(-1), runtime.SetMutexProfileFraction(-1),
		runtime.GOMAXPROCS(-1), runtime.GOMAXPROCS(-1), runtime.Compiler)
}

// handleHealth provides health status for the profiling server
func (p *Profiler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
  "status": "healthy",
  "service": "profiling",
  "timestamp": "%s"
}`, time.Now().Format(time.RFC3339))
}

// DefaultConfig returns a default profiling configuration
func DefaultConfig() Config {
	return Config{
		Enabled:              false, // Disabled by default for security
		Port:                 6060,
		BlockProfileRate:     0,     // Disabled by default (can impact performance)
		MutexProfileFraction: 0,     // Disabled by default (can impact performance)
		EnableAllocs:         false, // Disabled by default (high overhead)
	}
}

// ProductionConfig returns a production-safe profiling configuration
func ProductionConfig() Config {
	return Config{
		Enabled:              false, // Always disabled in production unless explicitly needed
		Port:                 6060,
		BlockProfileRate:     0,
		MutexProfileFraction: 0,
		EnableAllocs:         false,
	}
}

// DevelopmentConfig returns a development-friendly profiling configuration
func DevelopmentConfig() Config {
	return Config{
		Enabled:              true,
		Port:                 6060,
		BlockProfileRate:     1,    // Enable block profiling
		MutexProfileFraction: 1,    // Enable mutex profiling
		EnableAllocs:         true, // Enable allocation profiling
	}
}
