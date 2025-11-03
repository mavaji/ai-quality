package unit

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sdd-kafka-producer/internal/partitioner"
)

func TestHashPartitioner_GetPartition(t *testing.T) {
	p := partitioner.NewHashPartitioner()

	tests := []struct {
		name          string
		key           string
		numPartitions int32
		expectedRange bool
	}{
		{
			name:          "empty key",
			key:           "",
			numPartitions: 3,
			expectedRange: true,
		},
		{
			name:          "simple key",
			key:           "user123",
			numPartitions: 5,
			expectedRange: true,
		},
		{
			name:          "long key",
			key:           "very-long-partition-key-with-lots-of-characters",
			numPartitions: 10,
			expectedRange: true,
		},
		{
			name:          "unicode key",
			key:           "用户123",
			numPartitions: 4,
			expectedRange: true,
		},
		{
			name:          "single partition",
			key:           "test-key",
			numPartitions: 1,
			expectedRange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partition := p.GetPartition(tt.key, tt.numPartitions)

			if tt.expectedRange {
				assert.GreaterOrEqual(t, partition, int32(0), "Partition should be >= 0")
				assert.Less(t, partition, tt.numPartitions, "Partition should be < numPartitions")
			}
		})
	}
}

func TestHashPartitioner_Consistency(t *testing.T) {
	p := partitioner.NewHashPartitioner()

	key := "consistent-test-key"
	numPartitions := int32(7)

	// Get partition for same key multiple times
	firstPartition := p.GetPartition(key, numPartitions)

	for i := 0; i < 100; i++ {
		partition := p.GetPartition(key, numPartitions)
		assert.Equal(t, firstPartition, partition,
			"Partition should be consistent for same key")
	}
}

func TestHashPartitioner_Distribution(t *testing.T) {
	p := partitioner.NewHashPartitioner()

	numPartitions := int32(5)
	numKeys := 1000
	partitionCounts := make(map[int32]int)

	// Generate many keys and count partition distribution
	for i := 0; i < numKeys; i++ {
		key := generateTestKey(i)
		partition := p.GetPartition(key, numPartitions)
		partitionCounts[partition]++
	}

	// Check that all partitions are used
	assert.Equal(t, int(numPartitions), len(partitionCounts),
		"All partitions should be used")

	// Check roughly even distribution (allow 30% variance)
	expectedPerPartition := numKeys / int(numPartitions)
	tolerance := float64(expectedPerPartition) * 0.3

	for partition, count := range partitionCounts {
		assert.InDelta(t, expectedPerPartition, count, tolerance,
			"Partition %d should have roughly even distribution", partition)
	}
}

func TestRoundRobinPartitioner_GetPartition(t *testing.T) {
	p := partitioner.NewRoundRobinPartitioner()

	numPartitions := int32(3)

	// Test round-robin behavior
	expectedSequence := []int32{0, 1, 2, 0, 1, 2, 0, 1, 2}

	for i, expected := range expectedSequence {
		partition := p.GetPartition("any-key", numPartitions)
		assert.Equal(t, expected, partition,
			"Iteration %d should return partition %d", i, expected)
	}
}

func TestRoundRobinPartitioner_ThreadSafety(t *testing.T) {
	p := partitioner.NewRoundRobinPartitioner()
	numPartitions := int32(5)
	numGoroutines := 10
	partitionsPerGoroutine := 100

	results := make(chan int32, numGoroutines*partitionsPerGoroutine)

	// Launch multiple goroutines
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < partitionsPerGoroutine; j++ {
				partition := p.GetPartition("test-key", numPartitions)
				results <- partition
			}
		}()
	}

	// Collect all results
	partitionCounts := make(map[int32]int)
	totalResults := numGoroutines * partitionsPerGoroutine

	for i := 0; i < totalResults; i++ {
		partition := <-results
		assert.GreaterOrEqual(t, partition, int32(0), "Partition should be >= 0")
		assert.Less(t, partition, numPartitions, "Partition should be < numPartitions")
		partitionCounts[partition]++
	}

	// All partitions should be used
	assert.Equal(t, int(numPartitions), len(partitionCounts),
		"All partitions should be used in concurrent access")
}

func TestRandomPartitioner_GetPartition(t *testing.T) {
	p := partitioner.NewRandomPartitioner()

	numPartitions := int32(5)
	numTests := 1000
	partitionCounts := make(map[int32]int)

	// Generate many random partitions
	for i := 0; i < numTests; i++ {
		partition := p.GetPartition("any-key", numPartitions)

		// Validate range
		assert.GreaterOrEqual(t, partition, int32(0), "Partition should be >= 0")
		assert.Less(t, partition, numPartitions, "Partition should be < numPartitions")

		partitionCounts[partition]++
	}

	// Check that all partitions are used (with high probability)
	assert.GreaterOrEqual(t, len(partitionCounts), int(numPartitions)-1,
		"Most partitions should be used")

	// Check roughly even distribution for random (allow 40% variance)
	expectedPerPartition := numTests / int(numPartitions)
	tolerance := float64(expectedPerPartition) * 0.4

	for partition, count := range partitionCounts {
		assert.InDelta(t, expectedPerPartition, count, tolerance,
			"Partition %d should have roughly random distribution", partition)
	}
}

func TestManualPartitioner_GetPartition(t *testing.T) {
	targetPartition := int32(3)
	p := partitioner.NewManualPartitioner(targetPartition)

	numPartitions := int32(5)

	// Test multiple calls return the same partition
	for i := 0; i < 10; i++ {
		partition := p.GetPartition("any-key", numPartitions)
		assert.Equal(t, targetPartition, partition,
			"Manual partitioner should always return the same partition")
	}
}

func TestManualPartitioner_InvalidPartition(t *testing.T) {
	invalidPartition := int32(10)
	p := partitioner.NewManualPartitioner(invalidPartition)

	numPartitions := int32(5)

	// Should return a valid partition even if configured partition is invalid
	partition := p.GetPartition("any-key", numPartitions)
	assert.GreaterOrEqual(t, partition, int32(0), "Should return valid partition")
	assert.Less(t, partition, numPartitions, "Should return valid partition")
}

func TestPartitionerStrategy_Interface(t *testing.T) {
	// Test that all partitioners implement the interface correctly
	partitioners := []partitioner.Strategy{
		partitioner.NewHashPartitioner(),
		partitioner.NewRoundRobinPartitioner(),
		partitioner.NewRandomPartitioner(),
		partitioner.NewManualPartitioner(0),
	}

	for i, p := range partitioners {
		t.Run(getPartitionerName(i), func(t *testing.T) {
			require.NotNil(t, p, "Partitioner should not be nil")

			// Test basic interface compliance
			partition := p.GetPartition("test", 3)
			assert.GreaterOrEqual(t, partition, int32(0), "Should return valid partition")
			assert.Less(t, partition, int32(3), "Should return valid partition")
		})
	}
}

func TestPartitionKey_Validation(t *testing.T) {
	p := partitioner.NewHashPartitioner()

	tests := []struct {
		name          string
		key           string
		numPartitions int32
		shouldPanic   bool
	}{
		{
			name:          "valid parameters",
			key:           "valid-key",
			numPartitions: 5,
			shouldPanic:   false,
		},
		{
			name:          "zero partitions",
			key:           "valid-key",
			numPartitions: 0,
			shouldPanic:   true,
		},
		{
			name:          "negative partitions",
			key:           "valid-key",
			numPartitions: -1,
			shouldPanic:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					p.GetPartition(tt.key, tt.numPartitions)
				}, "Should panic with invalid partition count")
			} else {
				assert.NotPanics(t, func() {
					p.GetPartition(tt.key, tt.numPartitions)
				}, "Should not panic with valid parameters")
			}
		})
	}
}

func TestPartitioner_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		partitioner partitioner.Strategy
		key         string
		partitions  int32
	}{
		{
			name:        "hash with very long key",
			partitioner: partitioner.NewHashPartitioner(),
			key:         generateLongKey(10000),
			partitions:  100,
		},
		{
			name:        "hash with binary data in key",
			partitioner: partitioner.NewHashPartitioner(),
			key:         string([]byte{0x00, 0xFF, 0x80, 0x7F}),
			partitions:  3,
		},
		{
			name:        "round-robin with single partition",
			partitioner: partitioner.NewRoundRobinPartitioner(),
			key:         "any-key",
			partitions:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partition := tt.partitioner.GetPartition(tt.key, tt.partitions)

			assert.GreaterOrEqual(t, partition, int32(0), "Should return valid partition")
			assert.Less(t, partition, tt.partitions, "Should return valid partition")
		})
	}
}

func BenchmarkHashPartitioner(b *testing.B) {
	p := partitioner.NewHashPartitioner()
	key := "benchmark-test-key"
	numPartitions := int32(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.GetPartition(key, numPartitions)
	}
}

func BenchmarkRoundRobinPartitioner(b *testing.B) {
	p := partitioner.NewRoundRobinPartitioner()
	key := "benchmark-test-key"
	numPartitions := int32(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.GetPartition(key, numPartitions)
	}
}

func BenchmarkRandomPartitioner(b *testing.B) {
	p := partitioner.NewRandomPartitioner()
	key := "benchmark-test-key"
	numPartitions := int32(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.GetPartition(key, numPartitions)
	}
}

// Helper functions

func generateTestKey(i int) string {
	return fmt.Sprintf("test-key-%d", i)
}

func generateLongKey(length int) string {
	result := make([]byte, length)
	for i := range result {
		result[i] = byte('a' + (i % 26))
	}
	return string(result)
}

func getPartitionerName(index int) string {
	names := []string{"Hash", "RoundRobin", "Random", "Manual"}
	if index < len(names) {
		return names[index]
	}
	return "Unknown"
}
