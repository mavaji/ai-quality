package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"sdd-kafka-producer/internal/config"
	"sdd-kafka-producer/internal/health"
	"sdd-kafka-producer/internal/metrics"
	"sdd-kafka-producer/internal/producer"
	"sdd-kafka-producer/internal/profiling"
	"sdd-kafka-producer/internal/server"
)

var (
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	// Parse command line flags
	var (
		configPath  = flag.String("config", "configs/kafka-producer.yaml", "Path to configuration file")
		showVersion = flag.Bool("version", false, "Show version information")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("Kafka Producer Service\nVersion: %s\nBuild Time: %s\nGit Commit: %s\n", version, buildTime, gitCommit)
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration validation failed: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger, err := config.SetupLogger(cfg.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Log startup information
	logger.Info("Starting Kafka Producer Service",
		zap.String("version", version),
		zap.String("build_time", buildTime),
		zap.String("git_commit", gitCommit),
		zap.String("config_path", *configPath),
	)

	// Create application context for potential future use
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	app, err := initializeApplication(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize application", zap.Error(err))
	}

	// Setup signal handling for graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Start services
	var wg sync.WaitGroup

	// Start metrics server
	wg.Add(1)
	go func() {
		defer wg.Done()
		startMetricsServer(app.metrics, cfg.Monitoring, logger)
	}()

	// Start profiling server if enabled
	if cfg.Monitoring.ProfilingEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := app.profiler.Start(); err != nil {
				logger.Error("Profiling server error", zap.Error(err))
			}
		}()
	}

	// Start main HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := app.server.Start(); err != nil {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	logger.Info("All services started successfully")

	// Wait for shutdown signal
	<-shutdown
	logger.Info("Shutdown signal received, starting graceful shutdown")

	// Cancel context to signal all services to stop
	cancel()

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	// Shutdown services gracefully
	shutdownServices(shutdownCtx, app, logger)

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All services shutdown successfully")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout exceeded, forcing exit")
	}
}

// Application holds all application components
type Application struct {
	server   *server.Server
	health   *health.Manager
	metrics  *metrics.Manager
	profiler *profiling.Profiler
	producer producer.Producer
	tracker  *producer.MessageTracker
	logger   *zap.Logger
}

// initializeApplication sets up all application components
func initializeApplication(cfg *config.Config, logger *zap.Logger) (*Application, error) {
	// Initialize metrics manager
	metricsManager := metrics.NewManager(config.MetricsLogger(logger))

	// Initialize health check manager
	healthManager := health.NewManager(30*time.Second, config.HealthLogger(logger))

	// Add basic health checks
	healthManager.RegisterChecker(health.NewStaticChecker(
		"service", health.StatusHealthy, "Service is running",
	))

	// TODO: Add Kafka broker health check (will be implemented in User Story 1)
	// healthManager.RegisterChecker(kafkaBrokerChecker)

	// Initialize producer (using mock for now)
	kafkaProducer := producer.NewMockProducer()

	// Initialize message tracker
	messageTracker := producer.NewMessageTracker(
		config.ProducerLogger(logger),
		1000,         // Max history size
		24*time.Hour, // Retention time
	)

	// Initialize HTTP server
	httpServer := server.New(cfg.Server, config.ServerLogger(logger), healthManager)

	// Initialize handler manager and register routes
	handlerManager := server.NewHandlerManager(kafkaProducer, messageTracker, config.ServerLogger(logger), httpServer)
	handlerManager.RegisterRoutes()

	// Initialize profiler
	profilerConfig := profiling.Config{
		Enabled:              cfg.Monitoring.ProfilingEnabled,
		Port:                 cfg.Monitoring.ProfilingPort,
		BlockProfileRate:     0,     // Conservative for production
		MutexProfileFraction: 0,     // Conservative for production
		EnableAllocs:         false, // Conservative for production
	}
	profiler := profiling.New(profilerConfig, config.ProfilingLogger(logger))

	app := &Application{
		server:   httpServer,
		health:   healthManager,
		metrics:  metricsManager,
		profiler: profiler,
		producer: kafkaProducer,
		tracker:  messageTracker,
		logger:   logger,
	}

	logger.Info("Application components initialized successfully")
	return app, nil
}

// startMetricsServer starts the Prometheus metrics server
func startMetricsServer(metricsManager *metrics.Manager, cfg config.MonitoringConfig, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsManager.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: mux,
	}

	logger.Info("Starting metrics server", zap.Int("port", cfg.MetricsPort))

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Metrics server error", zap.Error(err))
	}
}

// shutdownServices gracefully shuts down all services
func shutdownServices(ctx context.Context, app *Application, logger *zap.Logger) {
	logger.Info("Shutting down services")

	// Shutdown HTTP server
	if err := app.server.Shutdown(ctx); err != nil {
		logger.Error("Error shutting down HTTP server", zap.Error(err))
	}

	// Shutdown profiling server
	if err := app.profiler.Stop(ctx); err != nil {
		logger.Error("Error shutting down profiling server", zap.Error(err))
	}

	// Shutdown producer
	if err := app.producer.Close(); err != nil {
		logger.Error("Error shutting down Kafka producer", zap.Error(err))
	}

	// Shutdown message tracker
	if err := app.tracker.Close(); err != nil {
		logger.Error("Error shutting down message tracker", zap.Error(err))
	}

	logger.Info("Service shutdown complete")
}
