# Kafka Producer Service API Documentation

## Overview

The Kafka Producer Service provides a high-performance REST API for publishing messages to Apache Kafka clusters. The service supports both individual and batch message publishing with guaranteed delivery, comprehensive error handling, and performance monitoring.

### Key Features

- **High Throughput**: Optimized for 10,000+ messages per second
- **Batch Processing**: Efficient batch publishing for optimal performance
- **Reliability**: Automatic retries with exponential backoff
- **Monitoring**: Built-in metrics and health checks
- **Flexible Partitioning**: Multiple partitioning strategies
- **Low Latency**: Sub-100ms p95 delivery latency

### Base URL

```
http://localhost:8080/api/v1
```

## Authentication

Currently, the service runs without authentication. JWT bearer token authentication is planned for future releases.

## Message Publishing

### Publish Single Message

Publishes a single message to a Kafka topic with delivery confirmation.

#### Request

```http
POST /messages
Content-Type: application/json
```

**Request Body:**
```json
{
  "topic": "user-events",
  "partitionKey": "user:12345",
  "payload": "{\"userId\": 12345, \"action\": \"login\", \"timestamp\": \"2025-11-03T10:00:00Z\"}",
  "headers": {
    "source": "user-service",
    "version": "1.0"
  },
  "timestamp": "2025-11-03T10:00:00Z"
}
```

**Parameters:**
- `topic` (required): Target Kafka topic name (1-249 characters, alphanumeric, dots, dashes, underscores)
- `payload` (required): Message content as JSON string or base64-encoded binary (max 1MB)
- `partitionKey` (optional): Key for partition assignment (max 1KB)
- `headers` (optional): Message headers as key-value pairs (max 10 headers)
- `timestamp` (optional): Message timestamp in ISO 8601 format

#### Response

**Success (201 Created):**
```json
{
  "messageId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "topic": "user-events",
  "timestamp": "2025-11-03T10:00:00.123Z"
}
```

**Error (400 Bad Request):**
```json
{
  "error": "INVALID_REQUEST",
  "message": "Topic name cannot be empty",
  "timestamp": "2025-11-03T10:00:00Z",
  "details": {
    "field": "topic",
    "value": ""
  }
}
```

### Publish Message Batch

Publishes multiple messages as a batch for optimal throughput.

#### Request

```http
POST /messages/batch
Content-Type: application/json
```

**Request Body:**
```json
{
  "messages": [
    {
      "topic": "user-events",
      "payload": "{\"userId\": 1, \"action\": \"signup\"}"
    },
    {
      "topic": "order-events", 
      "partitionKey": "user:1",
      "payload": "{\"orderId\": 101, \"amount\": 25.99}",
      "headers": {
        "source": "order-service"
      }
    }
  ]
}
```

**Parameters:**
- `messages` (required): Array of 1-100 message objects with the same format as single message publishing

#### Response

**Success (201 Created):**
```json
{
  "batchId": "550e8400-e29b-41d4-a716-446655440001",
  "messageCount": 2,
  "status": "accepted",
  "timestamp": "2025-11-03T10:00:00.123Z",
  "messages": [
    {
      "messageId": "550e8400-e29b-41d4-a716-446655440002",
      "status": "accepted",
      "topic": "user-events",
      "timestamp": "2025-11-03T10:00:00.124Z"
    },
    {
      "messageId": "550e8400-e29b-41d4-a716-446655440003", 
      "status": "accepted",
      "topic": "order-events",
      "timestamp": "2025-11-03T10:00:00.125Z"
    }
  ]
}
```

## Message Status Tracking

### Get Message Status

Retrieves the delivery status of a specific message.

#### Request

```http
GET /messages/{messageId}/status
```

**Path Parameters:**
- `messageId`: UUID returned from publish request

#### Response

**Success (200 OK):**
```json
{
  "messageId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "topic": "user-events",
  "partition": 3,
  "offset": 12345,
  "deliveryLatency": 25,
  "timestamp": "2025-11-03T10:00:00.148Z"
}
```

**Message Status Values:**
- `queued`: Message accepted but not yet sent
- `sending`: Message being transmitted to Kafka
- `success`: Message successfully delivered to Kafka
- `failed`: Message delivery failed permanently
- `timeout`: Message delivery timed out
- `rejected`: Message rejected by broker

**Not Found (404):**
```json
{
  "error": "MESSAGE_NOT_FOUND",
  "message": "Message with ID 550e8400-e29b-41d4-a716-446655440000 not found",
  "timestamp": "2025-11-03T10:00:00Z"
}
```

## Health and Monitoring

### Health Check

Checks service health including broker connectivity.

#### Request

```http
GET /health
```

#### Response

**Healthy (200 OK):**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-03T10:00:00Z",
  "checks": {
    "brokers": {
      "status": "healthy",
      "message": "All 3 brokers reachable",
      "latency": 5
    },
    "producer": {
      "status": "healthy",
      "message": "Producer active with 10 connections"
    }
  }
}
```

**Unhealthy (503 Service Unavailable):**
```json
{
  "status": "unhealthy", 
  "timestamp": "2025-11-03T10:00:00Z",
  "checks": {
    "brokers": {
      "status": "unhealthy",
      "message": "2 of 3 brokers unreachable",
      "latency": 5000
    }
  }
}
```

### Metrics

Retrieves performance metrics and statistics.

#### Request

```http
GET /metrics
Accept: application/json
```

#### Response

**Success (200 OK):**
```json
{
  "timestamp": "2025-11-03T10:00:00Z",
  "performance": {
    "averageLatency": 15,
    "p95Latency": 45,
    "p99Latency": 125
  },
  "throughput": {
    "messagesPerSecond": 15000,
    "bytesPerSecond": 2500000,
    "totalMessages": 1234567
  },
  "errors": {
    "totalErrors": 123,
    "errorRate": 0.1,
    "errorsByType": {
      "network": 45,
      "timeout": 67,
      "broker": 11
    }
  },
  "memory": {
    "heapSize": 134217728,
    "allocatedMemory": 89478485,
    "gcPauses": 15
  }
}
```

### Prometheus Metrics

The service also exposes metrics in Prometheus format for monitoring systems.

#### Request

```http
GET /metrics
Accept: text/plain
```

#### Response

```
# HELP kafka_producer_messages_total Total number of messages published
# TYPE kafka_producer_messages_total counter
kafka_producer_messages_total{topic="user-events"} 1234567

# HELP kafka_producer_latency_seconds Message publish latency in seconds
# TYPE kafka_producer_latency_seconds histogram
kafka_producer_latency_seconds_bucket{le="0.01"} 8500
kafka_producer_latency_seconds_bucket{le="0.05"} 9800
kafka_producer_latency_seconds_bucket{le="0.1"} 9950
kafka_producer_latency_seconds_bucket{le="+Inf"} 10000

# HELP kafka_producer_errors_total Total number of publish errors
# TYPE kafka_producer_errors_total counter
kafka_producer_errors_total{type="network"} 45
kafka_producer_errors_total{type="timeout"} 67
```

## Configuration

### Get Current Configuration

Retrieves the current producer configuration (excluding sensitive data).

#### Request

```http
GET /config
```

#### Response

**Success (200 OK):**
```json
{
  "brokers": ["broker1:9092", "broker2:9092"],
  "batchSettings": {
    "maxMessages": 100,
    "maxBytes": 16384,
    "flushInterval": "5ms"
  },
  "retryPolicy": {
    "maxAttempts": 3,
    "initialBackoff": "100ms",
    "maxBackoff": "30s"
  }
}
```

## Error Handling

### Error Response Format

All errors follow a consistent format:

```json
{
  "error": "ERROR_CODE",
  "message": "Human-readable error description",
  "timestamp": "2025-11-03T10:00:00Z",
  "details": {
    "additional": "context"
  }
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `INVALID_REQUEST` | 400 | Request validation failed |
| `MESSAGE_TOO_LARGE` | 413 | Message exceeds size limit |
| `BATCH_TOO_LARGE` | 413 | Batch exceeds size or count limit |
| `TOPIC_NOT_FOUND` | 404 | Kafka topic does not exist |
| `MESSAGE_NOT_FOUND` | 404 | Message ID not found |
| `BROKER_UNAVAILABLE` | 503 | All Kafka brokers unreachable |
| `RATE_LIMITED` | 429 | Request rate limit exceeded |
| `INTERNAL_ERROR` | 500 | Unexpected server error |

### Retry Guidelines

The service implements automatic retries for transient failures:

1. **Network Errors**: Automatic retry with exponential backoff (100ms → 200ms → 400ms)
2. **Broker Errors**: Circuit breaker pattern prevents cascading failures
3. **Client Errors**: No automatic retry (4xx responses)

For client applications:
- Implement retry logic for 5xx responses
- Use exponential backoff with jitter
- Set reasonable timeout values (30s recommended)
- Monitor error rates and adjust accordingly

## Rate Limiting

The service implements rate limiting to protect against abuse:

- **Per IP**: 1000 requests per minute
- **Per Endpoint**: Varies by endpoint complexity
- **Burst Capacity**: Short bursts allowed above steady rate

Rate limit headers are included in responses:
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999  
X-RateLimit-Reset: 1699027200
```

## Performance Characteristics

### Throughput Targets

- **Individual Messages**: 5,000 messages/second
- **Batch Publishing**: 10,000+ messages/second
- **Peak Burst**: 50,000 messages/second (short duration)

### Latency Targets

- **p50**: < 10ms
- **p95**: < 100ms  
- **p99**: < 250ms

### Resource Limits

- **Memory Usage**: < 512MB under normal load
- **Message Size**: 1MB maximum per message
- **Batch Size**: 100 messages or 16KB maximum per batch
- **Connection Pool**: 10 concurrent connections to Kafka

## Best Practices

### Message Design

1. **Keep Messages Small**: Smaller messages improve throughput
2. **Use Partition Keys**: Ensures message ordering within partitions
3. **Include Timestamps**: Enables time-based processing
4. **Add Source Headers**: Aids in debugging and tracing

### Batching Strategy

1. **Use Batch API**: Always prefer batch publishing for high throughput
2. **Optimal Batch Size**: 50-100 messages per batch
3. **Time-Based Flushing**: Don't wait for full batches in low-volume scenarios
4. **Topic Grouping**: Group messages by topic in batches when possible

### Error Handling

1. **Monitor Status**: Check message status for critical messages
2. **Handle Failures**: Implement appropriate retry logic
3. **Log Errors**: Capture error details for debugging
4. **Graceful Degradation**: Design for partial failures

### Monitoring

1. **Track Metrics**: Monitor throughput, latency, and error rates
2. **Set Alerts**: Alert on high error rates or latency spikes
3. **Health Checks**: Regular health check monitoring
4. **Capacity Planning**: Monitor resource usage trends

## SDKs and Client Libraries

### Go Client Library

A native Go client library is available at `pkg/client/client.go`:

```go
import "github.com/your-org/kafka-producer/pkg/client"

client := client.New("http://localhost:8080")

// Publish single message
receipt, err := client.PublishMessage(ctx, &client.Message{
    Topic:   "user-events",
    Payload: `{"userId": 123, "action": "login"}`,
})

// Publish batch
receipts, err := client.PublishBatch(ctx, messages)
```

### Other Languages

HTTP client libraries are available for most programming languages. Use the OpenAPI specification at `/specs/001-kafka-producer/contracts/api.yaml` to generate client code.

## Changelog

### Version 1.0.0

- Initial release
- Single and batch message publishing
- Message status tracking
- Health checks and metrics
- Performance optimization features

### Planned Features

- JWT authentication
- Message schemas and validation  
- Dead letter queue support
- Cross-region replication
- Advanced partitioning strategies