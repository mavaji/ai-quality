# Quickstart Guide: Kafka Producer Service

**Version**: 1.0.0  
**Last Updated**: 2025-11-03  
**Purpose**: Fast setup and testing guide for the Kafka Producer Service

## Prerequisites

- Go 1.21+ installed
- Kafka cluster running (local or remote)
- Basic understanding of Kafka topics and partitions

## Quick Setup

### 1. Install and Build

```bash
# Clone the repository
git clone <repository-url>
cd sdd-kafka-producer

# Install dependencies
go mod tidy

# Build the service
go build -o bin/kafka-producer cmd/kafka-producer/main.go

# Verify build
./bin/kafka-producer --version
```

### 2. Basic Configuration

Create a configuration file at `configs/kafka-producer.yaml`:

```yaml
# Minimal configuration for local development
kafka:
  brokers: ["localhost:9092"]
  producer:
    acks: 1
    compression: lz4
    batch_size: 100
    flush_frequency: 5ms
  retry:
    max_attempts: 3
    initial_backoff: 100ms
    max_backoff: 30s

server:
  host: "localhost"
  port: 8080

logging:
  level: info
  format: json
```

### 3. Start the Service

```bash
# Start with default configuration
./bin/kafka-producer

# Or specify custom config
./bin/kafka-producer --config=configs/kafka-producer.yaml

# Service should start on http://localhost:8080
```

Expected output:
```
{"level":"info","timestamp":"2025-11-03T10:00:00Z","message":"Starting Kafka Producer Service"}
{"level":"info","timestamp":"2025-11-03T10:00:00Z","message":"Connected to brokers: [localhost:9092]"}
{"level":"info","timestamp":"2025-11-03T10:00:00Z","message":"HTTP server listening on :8080"}
```

## Basic Usage

### Health Check

Verify the service is running and healthy:

```bash
curl http://localhost:8080/api/v1/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2025-11-03T10:00:00Z",
  "checks": {
    "brokers": {
      "status": "healthy",
      "message": "All 1 brokers reachable",
      "latency": 5
    },
    "producer": {
      "status": "healthy",
      "message": "Producer active with 1 connections"
    }
  }
}
```

### Publish Your First Message

#### Single Message

```bash
curl -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "test-events",
    "payload": "{\"userId\": 12345, \"action\": \"signup\", \"timestamp\": \"2025-11-03T10:00:00Z\"}"
  }'
```

Response:
```json
{
  "messageId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "topic": "test-events",
  "timestamp": "2025-11-03T10:00:00.123Z"
}
```

#### With Partition Key

```bash
curl -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user-events",
    "partitionKey": "user:12345",
    "payload": "{\"userId\": 12345, \"action\": \"login\"}",
    "headers": {
      "source": "web-app",
      "version": "1.0"
    }
  }'
```

#### Batch Publishing

```bash
curl -X POST http://localhost:8080/api/v1/messages/batch \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "topic": "user-events",
        "partitionKey": "user:1",
        "payload": "{\"userId\": 1, \"action\": \"signup\"}"
      },
      {
        "topic": "user-events", 
        "partitionKey": "user:2",
        "payload": "{\"userId\": 2, \"action\": \"login\"}"
      }
    ]
  }'
```

### Check Message Status

```bash
# Use the messageId from publish response
curl http://localhost:8080/api/v1/messages/550e8400-e29b-41d4-a716-446655440000/status
```

Response for successful delivery:
```json
{
  "messageId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "topic": "test-events",
  "partition": 0,
  "offset": 12345,
  "deliveryLatency": 25,
  "timestamp": "2025-11-03T10:00:00.148Z"
}
```

## Performance Testing

### Load Testing with curl

Generate test load:

```bash
# Send 100 messages quickly
for i in {1..100}; do
  curl -X POST http://localhost:8080/api/v1/messages \
    -H "Content-Type: application/json" \
    -d "{\"topic\": \"load-test\", \"payload\": \"{\\\"messageId\\\": $i}\"}" &
done
wait
```

### Monitor Performance

Check real-time metrics:

```bash
curl http://localhost:8080/api/v1/metrics
```

Response includes throughput and latency metrics:
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
    "totalMessages": 100
  },
  "errors": {
    "totalErrors": 0,
    "errorRate": 0.0,
    "errorsByType": {}
  }
}
```

## Configuration Options

### Environment Variables

Override configuration with environment variables:

```bash
# Set Kafka brokers
export KAFKA_BROKERS="broker1:9092,broker2:9092"

# Set log level
export LOG_LEVEL="debug"

# Set server port
export SERVER_PORT="8081"

# Start service
./bin/kafka-producer
```

### Production Configuration

Example production config with enhanced security and performance:

```yaml
kafka:
  brokers: ["broker1:9092", "broker2:9092", "broker3:9092"]
  producer:
    acks: all                    # Wait for all replicas (highest durability)
    compression: lz4             # Fast compression
    batch_size: 500             # Larger batches for higher throughput
    flush_frequency: 10ms       # Slightly higher latency for better batching
    max_message_bytes: 1048576  # 1MB max message size
  retry:
    max_attempts: 5
    initial_backoff: 200ms
    max_backoff: 60s
    backoff_multiplier: 2.0
  timeouts:
    connection: 30s
    request: 60s
    delivery: 300s
  security:
    protocol: SASL_SSL
    username: ${KAFKA_USERNAME}
    password: ${KAFKA_PASSWORD}
    certificate_path: /etc/ssl/kafka-ca.pem

server:
  host: "0.0.0.0"
  port: 8080
  read_timeout: 30s
  write_timeout: 30s
  shutdown_timeout: 60s

logging:
  level: info
  format: json
  output: /var/log/kafka-producer.log

monitoring:
  metrics_port: 9090
  health_check_interval: 30s
  profiling_enabled: true
  profiling_port: 6060
```

## Troubleshooting

### Common Issues

#### 1. Cannot Connect to Kafka

**Error**: `"status": "unhealthy", "checks": {"brokers": {"status": "unhealthy"}}`

**Solutions**:
- Verify Kafka is running: `kafka-topics.sh --list --bootstrap-server localhost:9092`
- Check broker addresses in configuration
- Verify network connectivity and firewall settings

#### 2. Messages Not Being Delivered

**Check message status**:
```bash
curl http://localhost:8080/api/v1/messages/{messageId}/status
```

**Common causes**:
- Topic doesn't exist (if auto-creation disabled)
- Insufficient permissions
- Message too large
- Broker capacity issues

#### 3. High Latency

**Check metrics**:
```bash
curl http://localhost:8080/api/v1/metrics | jq '.performance'
```

**Optimizations**:
- Reduce batch size for lower latency
- Increase batch size for higher throughput
- Adjust `flush_frequency` setting
- Check network latency to brokers

### Debug Mode

Enable detailed logging:

```bash
export LOG_LEVEL="debug"
./bin/kafka-producer
```

### Performance Profiling

Access pprof endpoints while service is running:

```bash
# CPU profile (60 seconds)
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=60

# Memory heap profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine profile
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

## Next Steps

1. **Production Deployment**: Review the deployment guide in `docs/deployment.md`
2. **Advanced Configuration**: See `docs/configuration.md` for all options
3. **Monitoring Setup**: Configure Prometheus/Grafana dashboards
4. **Client Libraries**: Use the Go client library in `pkg/client/`
5. **Schema Management**: Set up Avro schema registry integration

## Support

- **API Documentation**: OpenAPI spec in `specs/001-kafka-producer/contracts/api.yaml`
- **Monitoring**: Prometheus metrics on `:9090/metrics`
- **Health Checks**: HTTP endpoint at `/api/v1/health`
- **Profiling**: pprof endpoints on `:6060/debug/pprof/`

For issues or questions, check the project documentation or raise an issue in the repository.