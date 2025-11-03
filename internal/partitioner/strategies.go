package partitioner

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Strategy defines the interface for partition selection strategies
type Strategy interface {
	GetPartition(key string, numPartitions int32) int32
}

// HashPartitioner uses consistent hashing to assign partitions based on message keys
type HashPartitioner struct {
	// No state needed for hash-based partitioning
}

// NewHashPartitioner creates a new hash-based partitioner
func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{}
}

// GetPartition returns a partition based on the hash of the key
func (h *HashPartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	// Use FNV-1a hash for good distribution
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()

	return int32(hash % uint32(numPartitions))
}

// RoundRobinPartitioner assigns partitions in round-robin fashion
type RoundRobinPartitioner struct {
	counter int64
}

// NewRoundRobinPartitioner creates a new round-robin partitioner
func NewRoundRobinPartitioner() *RoundRobinPartitioner {
	return &RoundRobinPartitioner{}
}

// GetPartition returns the next partition in round-robin order
func (r *RoundRobinPartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	// Atomically increment counter and return modulo numPartitions
	next := atomic.AddInt64(&r.counter, 1) - 1 // Subtract 1 to start from 0
	return int32(next % int64(numPartitions))
}

// RandomPartitioner assigns partitions randomly
type RandomPartitioner struct {
	rand *rand.Rand
	mu   sync.Mutex
}

// NewRandomPartitioner creates a new random partitioner
func NewRandomPartitioner() *RandomPartitioner {
	return &RandomPartitioner{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GetPartition returns a random partition
func (r *RandomPartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return int32(r.rand.Intn(int(numPartitions)))
}

// ManualPartitioner always returns the same specified partition
type ManualPartitioner struct {
	partition int32
}

// NewManualPartitioner creates a new manual partitioner
func NewManualPartitioner(partition int32) *ManualPartitioner {
	return &ManualPartitioner{
		partition: partition,
	}
}

// GetPartition returns the configured partition, bounded by numPartitions
func (m *ManualPartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	// Ensure partition is within valid range
	if m.partition >= numPartitions {
		return numPartitions - 1
	}
	if m.partition < 0 {
		return 0
	}

	return m.partition
}

// StickyPartitioner attempts to use the same partition for a period of time
// before switching to increase batching efficiency
type StickyPartitioner struct {
	mu               sync.RWMutex
	currentPartition int32
	messageCount     int64
	switchThreshold  int64
	rand             *rand.Rand
	lastSwitch       time.Time
	switchInterval   time.Duration
}

// NewStickyPartitioner creates a new sticky partitioner
func NewStickyPartitioner(switchThreshold int64, switchInterval time.Duration) *StickyPartitioner {
	return &StickyPartitioner{
		currentPartition: 0,
		messageCount:     0,
		switchThreshold:  switchThreshold,
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
		lastSwitch:       time.Now(),
		switchInterval:   switchInterval,
	}
}

// GetPartition returns the current sticky partition or switches to a new one
func (s *StickyPartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	count := atomic.AddInt64(&s.messageCount, 1)

	// Check if we need to switch partitions
	s.mu.Lock()
	defer s.mu.Unlock()

	shouldSwitch := false
	if s.switchThreshold > 0 && count%s.switchThreshold == 0 {
		shouldSwitch = true
	}
	if s.switchInterval > 0 && time.Since(s.lastSwitch) >= s.switchInterval {
		shouldSwitch = true
	}

	if shouldSwitch {
		// Switch to a new random partition
		s.currentPartition = int32(s.rand.Intn(int(numPartitions)))
		s.lastSwitch = time.Now()
	}

	return s.currentPartition
}

// KeyRangePartitioner partitions based on key ranges (useful for ordered keys)
type KeyRangePartitioner struct {
	ranges []KeyRange
	mu     sync.RWMutex
}

// KeyRange defines a range of keys that map to a specific partition
type KeyRange struct {
	StartKey  string
	EndKey    string
	Partition int32
}

// NewKeyRangePartitioner creates a new key range partitioner
func NewKeyRangePartitioner(ranges []KeyRange) *KeyRangePartitioner {
	return &KeyRangePartitioner{
		ranges: ranges,
	}
}

// GetPartition returns a partition based on key ranges
func (k *KeyRangePartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	k.mu.RLock()
	defer k.mu.RUnlock()

	// Find matching range
	for _, r := range k.ranges {
		if key >= r.StartKey && key <= r.EndKey {
			if r.Partition < numPartitions {
				return r.Partition
			}
		}
	}

	// Fall back to hash partitioning if no range matches
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()
	return int32(hash % uint32(numPartitions))
}

// UpdateRanges updates the key ranges (thread-safe)
func (k *KeyRangePartitioner) UpdateRanges(ranges []KeyRange) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.ranges = ranges
}

// WeightedRandomPartitioner assigns partitions based on weights
type WeightedRandomPartitioner struct {
	weights []float64
	total   float64
	rand    *rand.Rand
	mu      sync.Mutex
}

// NewWeightedRandomPartitioner creates a new weighted random partitioner
func NewWeightedRandomPartitioner(weights []float64) *WeightedRandomPartitioner {
	total := 0.0
	for _, w := range weights {
		total += w
	}

	return &WeightedRandomPartitioner{
		weights: weights,
		total:   total,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GetPartition returns a partition based on weights
func (w *WeightedRandomPartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// If we don't have enough weights, fall back to uniform random
	if len(w.weights) < int(numPartitions) {
		return int32(w.rand.Intn(int(numPartitions)))
	}

	// Select partition based on weights
	r := w.rand.Float64() * w.total
	cumulative := 0.0

	for i := int32(0); i < numPartitions && i < int32(len(w.weights)); i++ {
		cumulative += w.weights[i]
		if r <= cumulative {
			return i
		}
	}

	// Fallback (should rarely happen)
	return numPartitions - 1
}

// UpdateWeights updates the partition weights (thread-safe)
func (w *WeightedRandomPartitioner) UpdateWeights(weights []float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.weights = weights
	w.total = 0.0
	for _, weight := range weights {
		w.total += weight
	}
}

// CompositePartitioner allows combining multiple strategies
type CompositePartitioner struct {
	strategies []WeightedStrategy
	rand       *rand.Rand
	mu         sync.Mutex
}

// WeightedStrategy combines a strategy with a weight for selection
type WeightedStrategy struct {
	Strategy Strategy
	Weight   float64
}

// NewCompositePartitioner creates a new composite partitioner
func NewCompositePartitioner(strategies []WeightedStrategy) *CompositePartitioner {
	return &CompositePartitioner{
		strategies: strategies,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GetPartition selects a strategy based on weights and uses it
func (c *CompositePartitioner) GetPartition(key string, numPartitions int32) int32 {
	if numPartitions <= 0 {
		panic("numPartitions must be positive")
	}

	if len(c.strategies) == 0 {
		// Fallback to hash partitioning
		hasher := fnv.New32a()
		hasher.Write([]byte(key))
		hash := hasher.Sum32()
		return int32(hash % uint32(numPartitions))
	}

	if len(c.strategies) == 1 {
		return c.strategies[0].Strategy.GetPartition(key, numPartitions)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Calculate total weight
	totalWeight := 0.0
	for _, ws := range c.strategies {
		totalWeight += ws.Weight
	}

	// Select strategy based on weight
	r := c.rand.Float64() * totalWeight
	cumulative := 0.0

	for _, ws := range c.strategies {
		cumulative += ws.Weight
		if r <= cumulative {
			return ws.Strategy.GetPartition(key, numPartitions)
		}
	}

	// Fallback to last strategy
	return c.strategies[len(c.strategies)-1].Strategy.GetPartition(key, numPartitions)
}

// PartitionerType represents the type of partitioner
type PartitionerType string

const (
	PartitionerTypeHash           PartitionerType = "hash"
	PartitionerTypeRoundRobin     PartitionerType = "round_robin"
	PartitionerTypeRandom         PartitionerType = "random"
	PartitionerTypeManual         PartitionerType = "manual"
	PartitionerTypeSticky         PartitionerType = "sticky"
	PartitionerTypeKeyRange       PartitionerType = "key_range"
	PartitionerTypeWeightedRandom PartitionerType = "weighted_random"
	PartitionerTypeComposite      PartitionerType = "composite"
)

// PartitionerConfig contains configuration for creating partitioners
type PartitionerConfig struct {
	Type            PartitionerType `json:"type" yaml:"type"`
	ManualPartition int32           `json:"manual_partition,omitempty" yaml:"manual_partition,omitempty"`
	StickyConfig    *StickyConfig   `json:"sticky_config,omitempty" yaml:"sticky_config,omitempty"`
	KeyRanges       []KeyRange      `json:"key_ranges,omitempty" yaml:"key_ranges,omitempty"`
	Weights         []float64       `json:"weights,omitempty" yaml:"weights,omitempty"`
}

// StickyConfig contains configuration for sticky partitioner
type StickyConfig struct {
	SwitchThreshold int64         `json:"switch_threshold" yaml:"switch_threshold"`
	SwitchInterval  time.Duration `json:"switch_interval" yaml:"switch_interval"`
}

// CreatePartitioner creates a partitioner based on configuration
func CreatePartitioner(config PartitionerConfig) Strategy {
	switch config.Type {
	case PartitionerTypeHash:
		return NewHashPartitioner()
	case PartitionerTypeRoundRobin:
		return NewRoundRobinPartitioner()
	case PartitionerTypeRandom:
		return NewRandomPartitioner()
	case PartitionerTypeManual:
		return NewManualPartitioner(config.ManualPartition)
	case PartitionerTypeSticky:
		if config.StickyConfig != nil {
			return NewStickyPartitioner(
				config.StickyConfig.SwitchThreshold,
				config.StickyConfig.SwitchInterval,
			)
		}
		return NewStickyPartitioner(1000, 30*time.Second) // Default values
	case PartitionerTypeKeyRange:
		return NewKeyRangePartitioner(config.KeyRanges)
	case PartitionerTypeWeightedRandom:
		return NewWeightedRandomPartitioner(config.Weights)
	default:
		// Default to hash partitioner
		return NewHashPartitioner()
	}
}
