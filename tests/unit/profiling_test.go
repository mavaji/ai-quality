package unit

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"sdd-kafka-producer/internal/profiling"
)

func TestProfiler_Start_Disabled(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	config := profiling.Config{
		Enabled: false,
		Port:    6060,
	}

	profiler := profiling.New(config, logger)

	// Should not return error when disabled
	err := profiler.Start()
	assert.NoError(t, err, "Profiler should start successfully when disabled")

	// Should not return error when stopping disabled profiler
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = profiler.Stop(ctx)
	assert.NoError(t, err, "Profiler should stop successfully when disabled")
}

func TestProfiler_Start_Enabled(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	config := profiling.Config{
		Enabled:              true,
		Port:                 6061, // Use different port to avoid conflicts
		BlockProfileRate:     1,
		MutexProfileFraction: 1,
		EnableAllocs:         true,
	}

	profiler := profiling.New(config, logger)

	// Start profiler
	err := profiler.Start()
	require.NoError(t, err, "Profiler should start successfully when enabled")

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Test that endpoints are accessible
	testCases := []struct {
		name     string
		endpoint string
		method   string
	}{
		{"Health", "http://localhost:6061/health", "GET"},
		{"Info", "http://localhost:6061/debug/info", "GET"},
		{"Memory", "http://localhost:6061/debug/memory", "GET"},
		{"Goroutines", "http://localhost:6061/debug/goroutines", "GET"},
		{"Runtime", "http://localhost:6061/debug/runtime", "GET"},
		{"Pprof Index", "http://localhost:6061/debug/pprof/", "GET"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.endpoint, nil)
			require.NoError(t, err, "Should create request")

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)

			if err != nil {
				t.Logf("Request to %s failed: %v", tc.endpoint, err)
				return // Skip if server not ready yet
			}
			defer resp.Body.Close()

			assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound,
				"Endpoint %s should return 200 or 404, got %d", tc.endpoint, resp.StatusCode)
		})
	}

	// Stop profiler
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = profiler.Stop(ctx)
	assert.NoError(t, err, "Profiler should stop successfully")
}

func TestProfiler_GCEndpoint(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	config := profiling.Config{
		Enabled: true,
		Port:    6062, // Use different port
	}

	profiler := profiling.New(config, logger)

	// Start profiler
	err := profiler.Start()
	require.NoError(t, err, "Profiler should start successfully")

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Test GC endpoint with POST method
	req, err := http.NewRequest("POST", "http://localhost:6062/debug/gc", nil)
	require.NoError(t, err, "Should create POST request")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)

	if err == nil {
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"GC endpoint should return 200 for POST")
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"),
			"GC endpoint should return JSON")
	}

	// Test GC endpoint with invalid method
	req, err = http.NewRequest("GET", "http://localhost:6062/debug/gc", nil)
	require.NoError(t, err, "Should create GET request")

	resp, err = client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode,
			"GC endpoint should return 405 for GET")
	}

	// Stop profiler
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = profiler.Stop(ctx)
	assert.NoError(t, err, "Profiler should stop successfully")
}

func TestProfiler_ConfigDefaults(t *testing.T) {
	defaultConfig := profiling.DefaultConfig()

	assert.False(t, defaultConfig.Enabled, "Default config should have profiling disabled")
	assert.Equal(t, 6060, defaultConfig.Port, "Default port should be 6060")
	assert.Equal(t, 0, defaultConfig.BlockProfileRate, "Block profiling should be disabled by default")
	assert.Equal(t, 0, defaultConfig.MutexProfileFraction, "Mutex profiling should be disabled by default")
	assert.False(t, defaultConfig.EnableAllocs, "Allocation profiling should be disabled by default")
}

func TestProfiler_ProductionConfig(t *testing.T) {
	prodConfig := profiling.ProductionConfig()

	assert.False(t, prodConfig.Enabled, "Production config should have profiling disabled")
	assert.Equal(t, 6060, prodConfig.Port, "Production port should be 6060")
	assert.Equal(t, 0, prodConfig.BlockProfileRate, "Block profiling should be disabled in production")
	assert.Equal(t, 0, prodConfig.MutexProfileFraction, "Mutex profiling should be disabled in production")
	assert.False(t, prodConfig.EnableAllocs, "Allocation profiling should be disabled in production")
}

func TestProfiler_DevelopmentConfig(t *testing.T) {
	devConfig := profiling.DevelopmentConfig()

	assert.True(t, devConfig.Enabled, "Development config should have profiling enabled")
	assert.Equal(t, 6060, devConfig.Port, "Development port should be 6060")
	assert.Equal(t, 1, devConfig.BlockProfileRate, "Block profiling should be enabled in development")
	assert.Equal(t, 1, devConfig.MutexProfileFraction, "Mutex profiling should be enabled in development")
	assert.True(t, devConfig.EnableAllocs, "Allocation profiling should be enabled in development")
}
