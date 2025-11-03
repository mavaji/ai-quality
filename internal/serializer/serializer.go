package serializer

import (
	"encoding/json"
	"fmt"
	"time"

	"sdd-kafka-producer/internal/models"
)

// SerializationFormat represents different serialization formats
type SerializationFormat string

const (
	FormatJSON   SerializationFormat = "json"
	FormatAvro   SerializationFormat = "avro"
	FormatBinary SerializationFormat = "binary"
	FormatString SerializationFormat = "string"
)

// Serializer interface defines serialization operations
type Serializer interface {
	Serialize(message *models.Message) ([]byte, error)
	Deserialize(data []byte) (*models.Message, error)
	GetFormat() SerializationFormat
	GetContentType() string
}

// JSONSerializer implements JSON serialization
type JSONSerializer struct{}

// NewJSONSerializer creates a new JSON serializer
func NewJSONSerializer() Serializer {
	return &JSONSerializer{}
}

// Serialize converts a message to JSON bytes
func (s *JSONSerializer) Serialize(message *models.Message) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	// Create a serializable representation
	data := struct {
		Topic     string            `json:"topic"`
		Key       string            `json:"key,omitempty"`
		Value     string            `json:"value"`
		Headers   map[string]string `json:"headers,omitempty"`
		Timestamp string            `json:"timestamp"`
	}{
		Topic:     message.Topic,
		Key:       message.Key,
		Value:     string(message.Value),
		Headers:   message.Headers,
		Timestamp: message.Timestamp.Format(time.RFC3339),
	}

	return json.Marshal(data)
}

// Deserialize converts JSON bytes back to a message
func (s *JSONSerializer) Deserialize(data []byte) (*models.Message, error) {
	if data == nil || len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	var jsonData struct {
		Topic     string            `json:"topic"`
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		Headers   map[string]string `json:"headers"`
		Timestamp string            `json:"timestamp"`
	}

	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, jsonData.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	message := &models.Message{
		Topic:     jsonData.Topic,
		Key:       jsonData.Key,
		Value:     []byte(jsonData.Value),
		Headers:   jsonData.Headers,
		Timestamp: timestamp,
	}

	return message, nil
}

// GetFormat returns the serialization format
func (s *JSONSerializer) GetFormat() SerializationFormat {
	return FormatJSON
}

// GetContentType returns the MIME content type
func (s *JSONSerializer) GetContentType() string {
	return "application/json"
}

// BinarySerializer implements binary serialization (pass-through)
type BinarySerializer struct{}

// NewBinarySerializer creates a new binary serializer
func NewBinarySerializer() Serializer {
	return &BinarySerializer{}
}

// Serialize for binary format just returns the raw value
func (s *BinarySerializer) Serialize(message *models.Message) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}
	
	// For binary format, we just return the raw value
	// Headers and metadata are handled separately by Kafka
	return message.Value, nil
}

// Deserialize for binary format creates a message with raw data
func (s *BinarySerializer) Deserialize(data []byte) (*models.Message, error) {
	if data == nil {
		return nil, fmt.Errorf("data cannot be nil")
	}

	// Create a basic message with binary data
	// Topic and other metadata would need to be provided separately
	message := &models.Message{
		Topic:     "", // Must be set by caller
		Key:       "",
		Value:     data,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	return message, nil
}

// GetFormat returns the serialization format
func (s *BinarySerializer) GetFormat() SerializationFormat {
	return FormatBinary
}

// GetContentType returns the MIME content type
func (s *BinarySerializer) GetContentType() string {
	return "application/octet-stream"
}

// StringSerializer implements string serialization
type StringSerializer struct{}

// NewStringSerializer creates a new string serializer
func NewStringSerializer() Serializer {
	return &StringSerializer{}
}

// Serialize converts message value to string
func (s *StringSerializer) Serialize(message *models.Message) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}
	
	// Convert value to string and return as bytes
	return message.Value, nil
}

// Deserialize converts string bytes to message
func (s *StringSerializer) Deserialize(data []byte) (*models.Message, error) {
	if data == nil {
		return nil, fmt.Errorf("data cannot be nil")
	}

	message := &models.Message{
		Topic:     "", // Must be set by caller
		Key:       "",
		Value:     data,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	return message, nil
}

// GetFormat returns the serialization format
func (s *StringSerializer) GetFormat() SerializationFormat {
	return FormatString
}

// GetContentType returns the MIME content type
func (s *StringSerializer) GetContentType() string {
	return "text/plain"
}

// SerializerManager manages different serializers
type SerializerManager struct {
	serializers map[SerializationFormat]Serializer
	defaultFormat SerializationFormat
}

// NewSerializerManager creates a new serializer manager
func NewSerializerManager(defaultFormat SerializationFormat) *SerializerManager {
	manager := &SerializerManager{
		serializers:   make(map[SerializationFormat]Serializer),
		defaultFormat: defaultFormat,
	}

	// Register built-in serializers
	manager.RegisterSerializer(FormatJSON, NewJSONSerializer())
	manager.RegisterSerializer(FormatBinary, NewBinarySerializer())
	manager.RegisterSerializer(FormatString, NewStringSerializer())

	return manager
}

// RegisterSerializer registers a serializer for a specific format
func (m *SerializerManager) RegisterSerializer(format SerializationFormat, serializer Serializer) {
	m.serializers[format] = serializer
}

// GetSerializer returns a serializer for the specified format
func (m *SerializerManager) GetSerializer(format SerializationFormat) (Serializer, error) {
	serializer, exists := m.serializers[format]
	if !exists {
		return nil, fmt.Errorf("no serializer registered for format: %s", format)
	}
	return serializer, nil
}

// GetDefaultSerializer returns the default serializer
func (m *SerializerManager) GetDefaultSerializer() Serializer {
	serializer, _ := m.GetSerializer(m.defaultFormat)
	return serializer
}

// SerializeWithFormat serializes a message using the specified format
func (m *SerializerManager) SerializeWithFormat(message *models.Message, format SerializationFormat) ([]byte, error) {
	serializer, err := m.GetSerializer(format)
	if err != nil {
		return nil, err
	}
	return serializer.Serialize(message)
}

// SerializeDefault serializes a message using the default format
func (m *SerializerManager) SerializeDefault(message *models.Message) ([]byte, error) {
	return m.SerializeWithFormat(message, m.defaultFormat)
}

// DeserializeWithFormat deserializes data using the specified format
func (m *SerializerManager) DeserializeWithFormat(data []byte, format SerializationFormat) (*models.Message, error) {
	serializer, err := m.GetSerializer(format)
	if err != nil {
		return nil, err
	}
	return serializer.Deserialize(data)
}

// GetSupportedFormats returns all supported serialization formats
func (m *SerializerManager) GetSupportedFormats() []SerializationFormat {
	formats := make([]SerializationFormat, 0, len(m.serializers))
	for format := range m.serializers {
		formats = append(formats, format)
	}
	return formats
}

// GetContentType returns the content type for a format
func (m *SerializerManager) GetContentType(format SerializationFormat) (string, error) {
	serializer, err := m.GetSerializer(format)
	if err != nil {
		return "", err
	}
	return serializer.GetContentType(), nil
}

// ValidateFormat checks if a format is supported
func (m *SerializerManager) ValidateFormat(format SerializationFormat) error {
	if _, exists := m.serializers[format]; !exists {
		return fmt.Errorf("unsupported serialization format: %s", format)
	}
	return nil
}

// SetDefaultFormat changes the default serialization format
func (m *SerializerManager) SetDefaultFormat(format SerializationFormat) error {
	if err := m.ValidateFormat(format); err != nil {
		return err
	}
	m.defaultFormat = format
	return nil
}

// GetDefaultFormat returns the current default format
func (m *SerializerManager) GetDefaultFormat() SerializationFormat {
	return m.defaultFormat
}

// MessageEnvelope represents a serialized message with metadata
type MessageEnvelope struct {
	Format      SerializationFormat `json:"format"`
	ContentType string              `json:"content_type"`
	Data        []byte              `json:"data"`
	Compressed  bool                `json:"compressed,omitempty"`
	Checksum    string              `json:"checksum,omitempty"`
	SerializedAt time.Time          `json:"serialized_at"`
}

// WrapMessage creates an envelope around a serialized message
func WrapMessage(data []byte, format SerializationFormat, contentType string) *MessageEnvelope {
	return &MessageEnvelope{
		Format:       format,
		ContentType:  contentType,
		Data:         data,
		Compressed:   false,
		SerializedAt: time.Now(),
	}
}

// Unwrap extracts the original data from an envelope
func (e *MessageEnvelope) Unwrap() []byte {
	return e.Data
}

// Size returns the size of the serialized data
func (e *MessageEnvelope) Size() int {
	return len(e.Data)
}

// Validate validates the envelope
func (e *MessageEnvelope) Validate() error {
	if e.Format == "" {
		return fmt.Errorf("format cannot be empty")
	}
	
	if e.Data == nil || len(e.Data) == 0 {
		return fmt.Errorf("data cannot be empty")
	}
	
	if e.SerializedAt.IsZero() {
		return fmt.Errorf("serialized_at timestamp cannot be zero")
	}
	
	return nil
}