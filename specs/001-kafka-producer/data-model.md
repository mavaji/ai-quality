# Data Model: Kafka Producer Service

**Date**: 2025-11-03  
**Purpose**: Define core entities and data structures for Kafka producer service

## Core Entities

### Message

Represents a single message to be published to Kafka.

**Fields**:
- `Topic` (string, required): Target Kafka topic name
- `PartitionKey` (string, optional): Key for partition assignment (enables key-based partitioning)
- `Payload` ([]byte, required): Message content (serialized data)
- `Headers` (map[string]string, optional): Key-value metadata pairs
- `Timestamp` (time.Time, optional): Message timestamp (auto-generated if not provided)
- `ID` (string, required): Unique message identifier for tracking

**Validation Rules**:
- Topic must not be empty and follow Kafka naming conventions (alphanumeric, hyphens, underscores)
- Payload must not exceed configured maximum message size (default: 1MB)
- Headers keys and values must be valid UTF-8 strings
- ID must be unique within the service instance

**Relationships**:
- One Message produces one DeliveryReceipt
- Multiple Messages can be grouped into one Batch

### ProducerConfiguration

Defines connection details, serialization format, and operational parameters.

**Fields**:
- `Brokers` ([]string, required): List of Kafka broker addresses
- `SerializationFormat` (enum, required): JSON, Avro, or Binary
- `BatchSettings` (BatchConfig, required): Batching optimization parameters
- `RetryPolicy` (RetryConfig, required): Retry behavior configuration  
- `TimeoutSettings` (TimeoutConfig, required): Connection and operation timeouts
- `SecurityConfig` (SecurityConfig, optional): Authentication and encryption settings
- `CompressionType` (enum, optional): None, GZIP, LZ4, Snappy, ZSTD

**Validation Rules**:
- At least one broker address must be provided
- Broker addresses must be valid host:port format
- Batch size must be between 1 and 1000 messages
- Retry max attempts must be between 0 and 10
- Timeout values must be positive durations

**Sub-entities**:

#### BatchConfig
- `MaxMessages` (int): Maximum messages per batch (1-1000)
- `MaxBytes` (int): Maximum batch size in bytes (1KB-1MB)
- `FlushInterval` (duration): Maximum time to wait before sending batch (1ms-10s)

#### RetryConfig  
- `MaxAttempts` (int): Maximum retry attempts (0-10)
- `InitialBackoff` (duration): Initial retry delay (10ms-1s)
- `MaxBackoff` (duration): Maximum retry delay (1s-300s)
- `BackoffMultiplier` (float64): Exponential backoff multiplier (1.0-5.0)

#### TimeoutConfig
- `ConnectionTimeout` (duration): Broker connection timeout (1s-30s)
- `RequestTimeout` (duration): Request timeout (1s-60s)
- `DeliveryTimeout` (duration): End-to-end delivery timeout (1s-300s)

#### SecurityConfig
- `Protocol` (enum): PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL
- `Username` (string): SASL username (if applicable)
- `Password` (string): SASL password (if applicable)
- `CertificatePath` (string): SSL certificate file path (if applicable)

### DeliveryReceipt

Records the delivery status and metadata for a published message.

**Fields**:
- `MessageID` (string, required): Reference to original message
- `Status` (enum, required): Success, Failed, Timeout, Rejected
- `Topic` (string, required): Target topic name
- `Partition` (int32, optional): Assigned partition (only for successful deliveries)
- `Offset` (int64, optional): Message offset in partition (only for successful deliveries)
- `AcknowledgmentLevel` (enum, required): None, Leader, AllReplicas
- `DeliveryLatency` (duration, required): Time from send to acknowledgment
- `Timestamp` (time.Time, required): Receipt generation time
- `ErrorDetails` (ErrorInfo, optional): Error information for failed deliveries

**Validation Rules**:
- MessageID must reference a valid sent message
- Partition and Offset must be non-negative for successful deliveries
- DeliveryLatency must be a positive duration
- ErrorDetails required for non-success statuses

**State Transitions**:
- Message → Sending → Success/Failed/Timeout/Rejected
- Failed messages may transition back to Sending (retry)

### ErrorReport

Captures comprehensive failure information for troubleshooting and monitoring.

**Fields**:
- `MessageID` (string, required): Reference to failed message
- `ErrorType` (enum, required): Network, Serialization, Broker, Timeout, Configuration, Unknown
- `ErrorMessage` (string, required): Human-readable error description
- `ErrorCode` (string, optional): Kafka-specific error code
- `RetryAttempt` (int, required): Current retry attempt number (0 for first failure)
- `Timestamp` (time.Time, required): Error occurrence time
- `BrokerAddress` (string, optional): Specific broker that returned the error
- `TopicPartition` (string, optional): Topic and partition where error occurred
- `RecoveryAction` (enum, required): Retry, Drop, DeadLetter, Manual

**Validation Rules**:
- MessageID must reference a valid message
- RetryAttempt must be non-negative
- ErrorMessage must not be empty
- RecoveryAction must be appropriate for ErrorType

**Relationships**:
- Multiple ErrorReports can exist for one Message (due to retries)
- ErrorReport triggers creation of new DeliveryReceipt

### Batch

Groups multiple messages for efficient transmission to Kafka.

**Fields**:
- `ID` (string, required): Unique batch identifier
- `Topic` (string, required): Target topic for all messages in batch
- `Messages` ([]Message, required): Array of messages to send together
- `TotalBytes` (int, computed): Sum of all message payload sizes
- `CreatedAt` (time.Time, required): Batch creation timestamp
- `SentAt` (time.Time, optional): Batch transmission timestamp
- `CompletedAt` (time.Time, optional): Batch completion timestamp

**Validation Rules**:
- All messages in batch must target the same topic
- Total batch size must not exceed broker limits
- Messages array must not be empty
- Timestamps must follow chronological order

**Computed Fields**:
- `MessageCount`: len(Messages)
- `AverageMessageSize`: TotalBytes / MessageCount
- `ProcessingDuration`: CompletedAt - CreatedAt

## Entity Relationships

```
Message (1) ────────────► (1) DeliveryReceipt
   │                            │
   │                            ▼
   │                     (0..*) ErrorReport
   │
   ▼
(*..*) Batch

ProducerConfiguration (1) ────► (*..*) Message
```

## Data Flow States

### Message Lifecycle
1. **Created**: Message instantiated with required fields
2. **Validated**: All validation rules passed
3. **Queued**: Added to internal processing queue
4. **Batched**: Grouped with other messages for transmission
5. **Serialized**: Payload converted to wire format
6. **Sent**: Transmitted to Kafka broker
7. **Acknowledged**: Broker confirmation received
8. **Completed**: Final delivery status recorded

### Error Handling States
- **Transient Error**: Network issues, broker unavailable → Retry
- **Permanent Error**: Invalid topic, message too large → Reject
- **Timeout Error**: No response within deadline → Retry with backoff
- **Configuration Error**: Invalid settings → Reject immediately

## Performance Considerations

### Memory Usage
- Message objects pooled and reused to minimize allocations
- Batch objects created on-demand and cleaned up after completion
- Configuration objects singleton pattern (loaded once at startup)
- ErrorReport objects bounded by retention policy

### Throughput Optimization
- Batch size optimized for network efficiency vs latency requirements
- Message validation performed once during creation
- DeliveryReceipt generation asynchronous to avoid blocking
- Configuration lookup cached to avoid repeated parsing

### Monitoring Fields
- All entities include timestamp fields for latency tracking
- Error counts aggregated by ErrorType for alerting
- Batch statistics collected for throughput monitoring
- Configuration changes logged for audit trail