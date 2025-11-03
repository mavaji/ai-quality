package producer

import (
	"regexp"
	"sync"
	"time"

	"sdd-kafka-producer/internal/models"
)

// ErrorType, ErrorSeverity, and RetryStrategy are re-exported from models
type ErrorType = models.ErrorType
type ErrorSeverity = models.ErrorSeverity

// Re-export error types and severities as constants
const (
	ErrorTypeNetwork        = models.ErrorTypeNetwork
	ErrorTypeTimeout        = models.ErrorTypeTimeout
	ErrorTypeBroker         = models.ErrorTypeBroker
	ErrorTypeAuthentication = models.ErrorTypeAuthentication
	ErrorTypeAuthorization  = models.ErrorTypeAuthorization
	ErrorTypeValidation     = models.ErrorTypeValidation
	ErrorTypeConfiguration  = models.ErrorTypeConfiguration
	ErrorTypeSerialization  = models.ErrorTypeSerialization
	ErrorTypeUnknown        = models.ErrorTypeUnknown
	ErrorTypeCustom         = models.ErrorTypeCustom

	ErrorSeverityLow      = models.ErrorSeverityLow
	ErrorSeverityMedium   = models.ErrorSeverityMedium
	ErrorSeverityHigh     = models.ErrorSeverityHigh
	ErrorSeverityCritical = models.ErrorSeverityCritical
)

// RetryStrategy defines different retry strategies
type RetryStrategy string

const (
	RetryStrategyNone        RetryStrategy = "none"
	RetryStrategyImmediate   RetryStrategy = "immediate"
	RetryStrategyFixed       RetryStrategy = "fixed"
	RetryStrategyLinear      RetryStrategy = "linear"
	RetryStrategyExponential RetryStrategy = "exponential"
)

// ErrorInfo contains categorized error information
type ErrorInfo struct {
	Type          ErrorType
	Severity      ErrorSeverity
	IsRetryable   bool
	Category      string
	OriginalError string
	CategorizedAt time.Time
}

// CategoryRule defines a rule for categorizing errors
type CategoryRule struct {
	Pattern     string        // Regex pattern to match error messages
	ErrorType   ErrorType     // Type to assign if pattern matches
	Severity    ErrorSeverity // Severity to assign if pattern matches
	IsRetryable bool          // Whether errors matching this pattern are retryable
}

// ErrorCategorizer categorizes errors based on patterns and rules
type ErrorCategorizer struct {
	rules         []CategoryRule
	compiledRules []compiledRule
	mutex         sync.RWMutex
}

type compiledRule struct {
	regex *regexp.Regexp
	rule  CategoryRule
}

// NewErrorCategorizer creates a new error categorizer with default rules
func NewErrorCategorizer() *ErrorCategorizer {
	categorizer := &ErrorCategorizer{
		rules:         make([]CategoryRule, 0),
		compiledRules: make([]compiledRule, 0),
	}

	// Add default categorization rules
	categorizer.addDefaultRules()

	return categorizer
}

// addDefaultRules adds the standard set of error categorization rules
func (ec *ErrorCategorizer) addDefaultRules() {
	defaultRules := []CategoryRule{
		// Network errors - usually retryable
		{
			Pattern:     "connection refused|network is unreachable|no route to host",
			ErrorType:   models.ErrorTypeNetwork,
			Severity:    models.ErrorSeverityHigh,
			IsRetryable: true,
		},
		{
			Pattern:     "connection reset|connection aborted|broken pipe",
			ErrorType:   models.ErrorTypeNetwork,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: true,
		},

		// Timeout errors - retryable
		{
			Pattern:     "context deadline exceeded|timeout|timed out",
			ErrorType:   models.ErrorTypeTimeout,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: true,
		},
		{
			Pattern:     "request timeout|operation timeout",
			ErrorType:   models.ErrorTypeTimeout,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: true,
		},

		// Kafka broker errors - usually retryable
		{
			Pattern:     "kafka:.*broker not available|kafka:.*no available brokers",
			ErrorType:   models.ErrorTypeBroker,
			Severity:    models.ErrorSeverityHigh,
			IsRetryable: true,
		},
		{
			Pattern:     "kafka:.*leader not available|kafka:.*no leader",
			ErrorType:   models.ErrorTypeBroker,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: true,
		},
		{
			Pattern:     "kafka:.*request timed out",
			ErrorType:   models.ErrorTypeTimeout,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: true,
		},
		{
			Pattern:     "kafka:.*partition.*not available",
			ErrorType:   models.ErrorTypeBroker,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: true,
		},

		// Authentication/Authorization errors - not retryable
		{
			Pattern:     "kafka:.*invalid credentials|authentication failed|auth.*failed",
			ErrorType:   models.ErrorTypeAuthentication,
			Severity:    models.ErrorSeverityCritical,
			IsRetryable: false,
		},
		{
			Pattern:     "kafka:.*not authorized|authorization failed|access denied|permission denied",
			ErrorType:   models.ErrorTypeAuthorization,
			Severity:    models.ErrorSeverityCritical,
			IsRetryable: false,
		},
		{
			Pattern:     "kafka:.*unauthorized",
			ErrorType:   models.ErrorTypeAuthorization,
			Severity:    models.ErrorSeverityCritical,
			IsRetryable: false,
		},

		// Validation errors - not retryable
		{
			Pattern:     "message.*too large|size exceeds|message size|payload.*large",
			ErrorType:   models.ErrorTypeValidation,
			Severity:    models.ErrorSeverityLow,
			IsRetryable: false,
		},
		{
			Pattern:     "topic.*invalid|invalid topic|topic validation failed",
			ErrorType:   models.ErrorTypeValidation,
			Severity:    models.ErrorSeverityLow,
			IsRetryable: false,
		},
		{
			Pattern:     "invalid.*format|malformed|validation.*failed|bad request",
			ErrorType:   models.ErrorTypeValidation,
			Severity:    models.ErrorSeverityLow,
			IsRetryable: false,
		},

		// Configuration errors - not retryable
		{
			Pattern:     "invalid.*configuration|config.*invalid|configuration.*error",
			ErrorType:   models.ErrorTypeConfiguration,
			Severity:    models.ErrorSeverityCritical,
			IsRetryable: false,
		},
		{
			Pattern:     "invalid broker|broker.*configuration|producer.*config",
			ErrorType:   models.ErrorTypeConfiguration,
			Severity:    models.ErrorSeverityCritical,
			IsRetryable: false,
		},

		// Serialization errors - not retryable
		{
			Pattern:     "serialization.*failed|serialize.*error|encoding.*failed|marshal.*error",
			ErrorType:   models.ErrorTypeSerialization,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: false,
		},
		{
			Pattern:     "json.*error|protobuf.*error|avro.*error",
			ErrorType:   models.ErrorTypeSerialization,
			Severity:    models.ErrorSeverityMedium,
			IsRetryable: false,
		},
	}

	for _, rule := range defaultRules {
		ec.addCompiledRule(rule)
	}
}

// addCompiledRule compiles and adds a rule to the categorizer
func (ec *ErrorCategorizer) addCompiledRule(rule CategoryRule) {
	regex, err := regexp.Compile("(?i)" + rule.Pattern) // Case-insensitive matching
	if err != nil {
		// Skip invalid patterns
		return
	}

	ec.rules = append(ec.rules, rule)
	ec.compiledRules = append(ec.compiledRules, compiledRule{
		regex: regex,
		rule:  rule,
	})
}

// AddRule adds a custom categorization rule
func (ec *ErrorCategorizer) AddRule(rule CategoryRule) {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	ec.addCompiledRule(rule)
}

// CategorizeError categorizes an error and returns detailed information
func (ec *ErrorCategorizer) CategorizeError(err error) ErrorInfo {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	errorMessage := err.Error()

	// Try to match against all rules
	for _, compiledRule := range ec.compiledRules {
		if compiledRule.regex.MatchString(errorMessage) {
			return ErrorInfo{
				Type:          compiledRule.rule.ErrorType,
				Severity:      compiledRule.rule.Severity,
				IsRetryable:   compiledRule.rule.IsRetryable,
				Category:      ec.getCategoryName(compiledRule.rule.ErrorType),
				OriginalError: errorMessage,
				CategorizedAt: time.Now(),
			}
		}
	}

	// Default categorization for unknown errors
	return ErrorInfo{
		Type:          models.ErrorTypeUnknown,
		Severity:      models.ErrorSeverityMedium,
		IsRetryable:   true, // Be optimistic about unknown errors
		Category:      "Unknown",
		OriginalError: errorMessage,
		CategorizedAt: time.Now(),
	}
}

// IsRetryable determines if an error type and severity combination is retryable
func (ec *ErrorCategorizer) IsRetryable(errorType ErrorType, severity ErrorSeverity) bool {
	// Critical authentication/authorization errors are never retryable
	if errorType == models.ErrorTypeAuthentication || errorType == models.ErrorTypeAuthorization {
		return false
	}

	// Validation and configuration errors are never retryable
	if errorType == models.ErrorTypeValidation || errorType == models.ErrorTypeConfiguration {
		return false
	}

	// Serialization errors are not retryable (fix the data first)
	if errorType == models.ErrorTypeSerialization {
		return false
	}

	// Network, timeout, and broker errors are generally retryable
	if errorType == models.ErrorTypeNetwork ||
		errorType == models.ErrorTypeTimeout ||
		errorType == models.ErrorTypeBroker {
		return true
	}

	// Unknown errors - be optimistic but careful with critical severity
	if errorType == models.ErrorTypeUnknown {
		return severity != models.ErrorSeverityCritical
	}

	// Custom error types - default to retryable unless critical
	return severity != models.ErrorSeverityCritical
}

// GetRetryStrategy returns the recommended retry strategy for an error type and severity
func (ec *ErrorCategorizer) GetRetryStrategy(errorType ErrorType, severity ErrorSeverity) RetryStrategy {
	// No retry for non-retryable errors
	if !ec.IsRetryable(errorType, severity) {
		return RetryStrategyNone
	}

	switch errorType {
	case models.ErrorTypeNetwork:
		if severity == models.ErrorSeverityLow {
			return RetryStrategyImmediate
		}
		return RetryStrategyLinear

	case models.ErrorTypeTimeout:
		return RetryStrategyExponential

	case models.ErrorTypeBroker:
		if severity == models.ErrorSeverityHigh {
			return RetryStrategyExponential
		}
		return RetryStrategyLinear

	case models.ErrorTypeUnknown:
		return RetryStrategyLinear

	default:
		return RetryStrategyLinear
	}
}

// getCategoryName returns a human-readable category name for an error type
func (ec *ErrorCategorizer) getCategoryName(errorType ErrorType) string {
	switch errorType {
	case models.ErrorTypeNetwork:
		return "Network"
	case models.ErrorTypeTimeout:
		return "Timeout"
	case models.ErrorTypeBroker:
		return "Kafka Broker"
	case models.ErrorTypeAuthentication:
		return "Authentication"
	case models.ErrorTypeAuthorization:
		return "Authorization"
	case models.ErrorTypeValidation:
		return "Validation"
	case models.ErrorTypeConfiguration:
		return "Configuration"
	case models.ErrorTypeSerialization:
		return "Serialization"
	case models.ErrorTypeCustom:
		return "Custom"
	case models.ErrorTypeUnknown:
		return "Unknown"
	default:
		return "Unspecified"
	}
}

// GetRules returns a copy of all current categorization rules
func (ec *ErrorCategorizer) GetRules() []CategoryRule {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	rules := make([]CategoryRule, len(ec.rules))
	copy(rules, ec.rules)
	return rules
}

// ClearRules clears all categorization rules (including defaults)
func (ec *ErrorCategorizer) ClearRules() {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	ec.rules = make([]CategoryRule, 0)
	ec.compiledRules = make([]compiledRule, 0)
}

// ResetToDefaults clears all rules and re-adds the default rules
func (ec *ErrorCategorizer) ResetToDefaults() {
	ec.ClearRules()
	ec.addDefaultRules()
}

// MatchingRules returns all rules that would match the given error
func (ec *ErrorCategorizer) MatchingRules(err error) []CategoryRule {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	errorMessage := err.Error()
	var matches []CategoryRule

	for _, compiledRule := range ec.compiledRules {
		if compiledRule.regex.MatchString(errorMessage) {
			matches = append(matches, compiledRule.rule)
		}
	}

	return matches
}

// GetStatistics returns statistics about error categorization
func (ec *ErrorCategorizer) GetStatistics() map[string]int {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	stats := make(map[string]int)
	stats["total_rules"] = len(ec.rules)

	// Count rules by type
	typeCount := make(map[ErrorType]int)
	severityCount := make(map[ErrorSeverity]int)
	retryableCount := 0

	for _, rule := range ec.rules {
		typeCount[rule.ErrorType]++
		severityCount[rule.Severity]++
		if rule.IsRetryable {
			retryableCount++
		}
	}

	for errorType, count := range typeCount {
		stats[string(errorType)+"_rules"] = count
	}

	for severity, count := range severityCount {
		stats[string(severity)+"_severity_rules"] = count
	}

	stats["retryable_rules"] = retryableCount
	stats["non_retryable_rules"] = len(ec.rules) - retryableCount

	return stats
}
