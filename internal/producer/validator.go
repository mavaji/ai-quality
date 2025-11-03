package producer

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"sdd-kafka-producer/internal/models"
)

// Validator handles message validation beyond basic model validation
type Validator struct {
	maxMessageSize    int
	allowedTopics     map[string]bool
	topicPatterns     []*regexp.Regexp
	requiredHeaders   []string
	forbiddenPatterns []*regexp.Regexp
}

// ValidationConfig holds configuration for message validation
type ValidationConfig struct {
	MaxMessageSize    int                 `yaml:"max_message_size"`
	AllowedTopics     []string            `yaml:"allowed_topics"`
	TopicPatterns     []string            `yaml:"topic_patterns"`
	RequiredHeaders   []string            `yaml:"required_headers"`
	ForbiddenPatterns []string            `yaml:"forbidden_patterns"`
	EnableContentScan bool                `yaml:"enable_content_scan"`
}

// NewValidator creates a new message validator
func NewValidator(config ValidationConfig) (*Validator, error) {
	validator := &Validator{
		maxMessageSize:  config.MaxMessageSize,
		allowedTopics:   make(map[string]bool),
		requiredHeaders: config.RequiredHeaders,
	}

	// Build allowed topics map for O(1) lookup
	for _, topic := range config.AllowedTopics {
		validator.allowedTopics[topic] = true
	}

	// Compile topic patterns
	for _, pattern := range config.TopicPatterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid topic pattern '%s': %w", pattern, err)
		}
		validator.topicPatterns = append(validator.topicPatterns, regex)
	}

	// Compile forbidden patterns
	for _, pattern := range config.ForbiddenPatterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid forbidden pattern '%s': %w", pattern, err)
		}
		validator.forbiddenPatterns = append(validator.forbiddenPatterns, regex)
	}

	return validator, nil
}

// ValidateMessage performs comprehensive message validation
func (v *Validator) ValidateMessage(message *models.Message) error {
	// First, run basic model validation
	if err := message.Validate(); err != nil {
		return fmt.Errorf("basic validation failed: %w", err)
	}

	// Additional producer-level validations
	if err := v.validateTopicAccess(message.Topic); err != nil {
		return fmt.Errorf("topic validation failed: %w", err)
	}

	if err := v.validateMessageSize(message); err != nil {
		return fmt.Errorf("size validation failed: %w", err)
	}

	if err := v.validateRequiredHeaders(message); err != nil {
		return fmt.Errorf("header validation failed: %w", err)
	}

	if err := v.validateContent(message); err != nil {
		return fmt.Errorf("content validation failed: %w", err)
	}

	if err := v.validateTimestamp(message); err != nil {
		return fmt.Errorf("timestamp validation failed: %w", err)
	}

	return nil
}

// validateTopicAccess validates if the message can be published to the specified topic
func (v *Validator) validateTopicAccess(topic string) error {
	// If allowed topics are specified, check if topic is in the list
	if len(v.allowedTopics) > 0 {
		if !v.allowedTopics[topic] {
			return fmt.Errorf("topic '%s' is not in the allowed topics list", topic)
		}
		return nil
	}

	// Check topic patterns
	if len(v.topicPatterns) > 0 {
		for _, pattern := range v.topicPatterns {
			if pattern.MatchString(topic) {
				return nil // Topic matches at least one pattern
			}
		}
		return fmt.Errorf("topic '%s' does not match any allowed patterns", topic)
	}

	// If no restrictions are configured, allow all topics
	return nil
}

// validateMessageSize validates the total message size
func (v *Validator) validateMessageSize(message *models.Message) error {
	if v.maxMessageSize <= 0 {
		return nil // No size limit configured
	}

	size := message.Size()
	if size > v.maxMessageSize {
		return fmt.Errorf("message size %d bytes exceeds maximum allowed size of %d bytes", 
			size, v.maxMessageSize)
	}

	return nil
}

// validateRequiredHeaders ensures all required headers are present
func (v *Validator) validateRequiredHeaders(message *models.Message) error {
	if len(v.requiredHeaders) == 0 {
		return nil // No required headers configured
	}

	for _, requiredHeader := range v.requiredHeaders {
		if _, exists := message.GetHeader(requiredHeader); !exists {
			return fmt.Errorf("required header '%s' is missing", requiredHeader)
		}
	}

	return nil
}

// validateContent scans message content for forbidden patterns
func (v *Validator) validateContent(message *models.Message) error {
	if len(v.forbiddenPatterns) == 0 {
		return nil // No content restrictions configured
	}

	content := string(message.Value)

	for _, pattern := range v.forbiddenPatterns {
		if pattern.MatchString(content) {
			return fmt.Errorf("message content contains forbidden pattern")
		}
	}

	// Check headers for forbidden content
	for _, value := range message.Headers {
		for _, pattern := range v.forbiddenPatterns {
			if pattern.MatchString(value) {
				return fmt.Errorf("message headers contain forbidden pattern")
			}
		}
	}

	return nil
}

// validateTimestamp performs additional timestamp validation
func (v *Validator) validateTimestamp(message *models.Message) error {
	now := time.Now()
	
	// Check if timestamp is too far in the future (more than 5 minutes)
	if message.Timestamp.After(now.Add(5 * time.Minute)) {
		return fmt.Errorf("timestamp is too far in the future (max 5 minutes allowed)")
	}

	// Check if timestamp is too old (more than 7 days)
	if message.Timestamp.Before(now.Add(-7 * 24 * time.Hour)) {
		return fmt.Errorf("timestamp is too old (max 7 days allowed)")
	}

	return nil
}

// ValidateMessageBatch validates a batch of messages
func (v *Validator) ValidateMessageBatch(messages []*models.Message) error {
	if len(messages) == 0 {
		return fmt.Errorf("batch cannot be empty")
	}

	for i, message := range messages {
		if err := v.ValidateMessage(message); err != nil {
			return fmt.Errorf("message at index %d failed validation: %w", i, err)
		}
	}

	return nil
}

// SanitizeMessage sanitizes potentially unsafe content from messages
func (v *Validator) SanitizeMessage(message *models.Message) *models.Message {
	sanitized := message.Clone()

	// Sanitize headers by removing potentially dangerous ones
	dangerousHeaders := []string{
		"authorization", "cookie", "set-cookie", "x-auth-token",
	}

	for _, header := range dangerousHeaders {
		sanitized.RemoveHeader(header)
		sanitized.RemoveHeader(strings.ToUpper(header))
	}

	// Truncate very long keys
	if len(sanitized.Key) > 1024 {
		sanitized.Key = sanitized.Key[:1024]
	}

	return sanitized
}

// GetValidationRules returns the current validation rules
func (v *Validator) GetValidationRules() map[string]interface{} {
	return map[string]interface{}{
		"max_message_size":   v.maxMessageSize,
		"allowed_topics":     len(v.allowedTopics),
		"topic_patterns":     len(v.topicPatterns),
		"required_headers":   v.requiredHeaders,
		"forbidden_patterns": len(v.forbiddenPatterns),
	}
}

// UpdateMaxMessageSize updates the maximum message size limit
func (v *Validator) UpdateMaxMessageSize(size int) {
	v.maxMessageSize = size
}

// AddAllowedTopic adds a topic to the allowed topics list
func (v *Validator) AddAllowedTopic(topic string) {
	if v.allowedTopics == nil {
		v.allowedTopics = make(map[string]bool)
	}
	v.allowedTopics[topic] = true
}

// RemoveAllowedTopic removes a topic from the allowed topics list
func (v *Validator) RemoveAllowedTopic(topic string) {
	delete(v.allowedTopics, topic)
}

// IsTopicAllowed checks if a topic is explicitly allowed
func (v *Validator) IsTopicAllowed(topic string) bool {
	if len(v.allowedTopics) == 0 {
		return true // No restrictions
	}
	return v.allowedTopics[topic]
}

// DefaultValidator returns a validator with sensible defaults
func DefaultValidator() *Validator {
	return &Validator{
		maxMessageSize:  1024 * 1024, // 1MB
		allowedTopics:   make(map[string]bool),
		requiredHeaders: []string{},
	}
}