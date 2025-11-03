# Quickstart Guide: Kafka Producer Service

**Version**: 1.0.0  
**Last Updated**: 2025-11-03  
**Purpose**: Fast setup and testing guide for the Kafka Producer Service

## Prerequisites

- Go 1.21+ installed
- Basic understanding of Kafka topics and partitions

**Note**: This quickstart uses a mock Kafka producer for demonstration. For production use with real Kafka clusters, see the configuration section for proper broker setup.

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
{"level":"info","timestamp":"2025-11-03T16:21:57.986+1100","caller":"kafka-producer/main.go:66","msg":"Starting Kafka Producer Service","version":"dev","build_time":"unknown","git_commit":"unknown","config_path":"configs/kafka-producer.yaml"}
{"level":"info","timestamp":"2025-11-03T16:21:57.987+1100","caller":"kafka-producer/main.go:117","msg":"All services started successfully"}
{"level":"info","timestamp":"2025-11-03T16:21:57.987+1100","caller":"server/server.go:63","msg":"Starting HTTP server","component":"server","address":"0.0.0.0:8080","read_timeout":30,"write_timeout":30}
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
  "timestamp": "2025-11-03T16:29:03.701284+11:00",
  "checks": {
    "service": {
      "status": "healthy",
      "message": "Service is running",
      "latency": 417
    }
  }
}
```

### Service Configuration

Check current service configuration and API limits:

```bash
curl http://localhost:8080/api/v1/config
```

Response:
```json
{
  "timestamp": "2025-11-03T16:22:25.452635+11:00",
  "version": "1.0.0",
  "endpoints": [
    "/api/v1/messages",
    "/api/v1/messages/batch",
    "/api/v1/metrics",
    "/api/v1/config",
    "/api/v1/health"
  ],
  "limits": {
    "max_message_size": 1048576,
    "max_batch_size": 100,
    "max_headers": 50
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
    "value": "{\"userId\": 12345, \"action\": \"signup\", \"timestamp\": \"2025-11-03T10:00:00Z\"}"
  }'
```

Response:
```json
{
  "message_id": "msg-1762147412008415000-1fbc141e",
  "status": "success",
  "topic": "test-events",
  "partition": 0,
  "offset": 123456,
  "delivered_at": "2025-11-03T16:23:32.008435+11:00"
}
```

#### With Partition Key and Headers

```bash
curl -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user-events",
    "key": "user:12345",
    "value": "{\"userId\": 12345, \"action\": \"login\"}",
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
        "key": "user:1",
        "value": "{\"userId\": 1, \"action\": \"signup\"}"
      },
      {
        "topic": "user-events", 
        "key": "user:2",
        "value": "{\"userId\": 2, \"action\": \"login\"}"
      }
    ]
  }'
```

Response:
```json
{
  "results": [
    {
      "message_id": "msg-1762147421763050000-ec172a22",
      "status": "success",
      "topic": "user-events",
      "partition": 0,
      "offset": 123457,
      "delivered_at": "2025-11-03T16:23:41.763062+11:00"
    },
    {
      "message_id": "msg-1762147421763064000-0b129a26",
      "status": "success",
      "topic": "user-events",
      "partition": 0,
      "offset": 123458,
      "delivered_at": "2025-11-03T16:23:41.763064+11:00"
    }
  ],
  "success_count": 2,
  "failure_count": 0,
  "total_count": 2
}
```

### Check Message Status

```bash
# Use the message_id from publish response
curl http://localhost:8080/api/v1/messages/msg-1762147412008415000-1fbc141e/status
```

Response for successful delivery:
```json
{
  "message_id": "msg-1762147412008415000-1fbc141e",
  "topic": "test-events",
  "current_state": "success",
  "receipt": {
    "message_id": "msg-1762147412008415000-1fbc141e",
    "topic": "test-events",
    "partition": 0,
    "offset": 123456,
    "delivered_at": "2025-11-03T16:23:32.008435+11:00",
    "status": "success"
  },
  "created_at": "2025-11-03T16:23:32.008400+11:00",
  "updated_at": "2025-11-03T16:23:32.008435+11:00"
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
    -d "{\"topic\": \"load-test\", \"value\": \"{\\\"messageId\\\": $i}\"}" &
done
wait
```

### Monitor Performance

Check real-time metrics:

```bash
curl http://localhost:8080/api/v1/metrics
```

Response includes service metrics:
```json
{
  "timestamp": "2025-11-03T16:22:33.430204+11:00",
  "status": "operational",
  "producer": {
    "status": "connected",
    "messages_sent_total": 0,
    "messages_failed_total": 0
  },
  "system": {
    "memory_usage": 0,
    "cpu_usage": 0,
    "goroutines": 0
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
curl http://localhost:8080/api/v1/messages/{message_id}/status
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