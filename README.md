# Event Streaming System (DEPS)

A lightweight, distributed event streaming system built in Go. DEPS (Distributed Event Processing System) provides a simple but powerful message broker for building event-driven architectures.

## Features

- **Topic-based Publishing**: Organize events into topics with multiple partitions
- **Partition Routing**: Automatic event routing using key-based hashing or round-robin
- **Consumer Groups**: Track consumer offsets per group for reliable message consumption
- **Persistent Storage**: All events are durably written to disk using binary serialization
- **RESTful API**: Simple HTTP endpoints for producers and consumers
- **CLI Tools**: Easy-to-use command-line tools for publishing and consuming events

## Architecture

### Components

1. **Broker**: Central message broker that manages topics, partitions, and consumer groups
2. **Metadata Manager**: Persists topic and partition information to `metadata.json`
3. **Log Storage Engine**: Handles binary serialization and file-based event storage
4. **Partition Manager**: Routes events to partitions and fetches events for consumers
5. **Offset Manager**: Tracks and persists consumer group offsets
6. **HTTP Server**: Exposes REST API for producers and consumers

### Data Flow

```
Producer
  ↓
HTTP API (POST /topics/events)
  ↓
Partition Manager (Route by key hash)
  ↓
Log Storage (Binary format to disk)
  ↓
Consumer
  ↓
HTTP API (GET /messages)
  ↓
Offset Manager (Commit offsets)
```

## Project Structure

```
deps/
├── cmd/
│   ├── broker/           # Broker server executable
│   │   └── main.go
│   ├── producer/         # Producer CLI tool
│   │   └── main.go
│   └── consumer/         # Consumer CLI tool
│       └── main.go
├── internal/
│   ├── broker/
│   │   ├── types.go      # Core types (Event, Partition, Topic, etc.)
│   │   ├── broker.go     # Main broker logic
│   │   ├── http.go       # HTTP server and endpoints
│   │   ├── metadata.go   # Metadata manager
│   │   ├── storage.go    # Log storage engine
│   │   ├── offsets.go    # Offset manager
│   │   └── http_test.go  # Tests
│   ├── log/              # Logging utilities (reserved)
│   └── protocol/         # Protocol definitions (reserved)
├── data/                 # Runtime data directory
│   └── metadata.json     # Topic metadata
├── Docs/
│   └── Architecture.md   # Architecture documentation
├── go.mod               # Go module definition
└── README.md           # This file
```

## Getting Started

### Prerequisites

- Go 1.21 or later
- macOS, Linux, or Windows

### Installation

```bash
# Clone the repository
git clone https://github.com/EricLarwa/2t3-DEPS.git
cd deps

# Build the broker
go build -o broker-server ./cmd/broker

# Build the producer
go build -o producer ./cmd/producer

# Build the consumer
go build -o consumer ./cmd/consumer
```

### Starting the Broker

```bash
# Create data directory
mkdir -p data

# Start the broker on port 8080
./broker-server --port 8080 --data-dir ./data
```

The broker will automatically create the following topics on startup:

- `orders` (3 partitions)
- `payments` (2 partitions)
- `shipments` (1 partition)

## Usage

### Publishing Events

#### Using the Producer CLI

```bash
# Publish a simple event
./producer \
  --broker localhost:8080 \
  --topic orders \
  --key user123 \
  --payload '{"amount": 100, "currency": "USD"}'
```

**Output:**

```
Event published successfully!
  Timestamp: 2026-01-15T16:12:12-05:00
  Topic:     orders
  Key:       user123
  Partition: 1
  Offset:    0
```

#### Using the HTTP API

```bash
curl -X POST http://localhost:8080/topics/events?topic=orders \
  -H "Content-Type: application/json" \
  -d '{
    "key": "user456",
    "payload": {
      "amount": 250,
      "currency": "USD"
    }
  }'
```

### Consuming Events

#### Using the Consumer CLI

```bash
# Consume from partition 0, starting at offset 0, fetch up to 10 messages
./consumer \
  --broker localhost:8080 \
  --topic orders \
  --group billing-service \
  --partition 0 \
  --offset 0 \
  --count 10
```

**Options:**

- `--broker`: Broker address (default: `localhost:8080`)
- `--topic`: Topic name to consume from (required)
- `--group`: Consumer group name (default: `default`)
- `--partition`: Partition ID to consume from (default: `0`)
- `--offset`: Starting offset (default: `0`)
- `--maxBytes`: Maximum bytes to fetch (default: `1048576` / 1MB)
- `--count`: Number of messages to consume (0 = continuous)
- `--commitInterval`: Interval to commit offsets (default: `5s`)

#### Using the HTTP API

```bash
# Fetch messages
curl "http://localhost:8080/messages?topic=orders&partition=0&offset=0&maxBytes=1048576"

# Commit offset
curl -X POST "http://localhost:8080/consumer-groups/offsets/commit?group=billing-service" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "partition": 0,
    "offset": 42
  }'
```

### Health Check

```bash
curl http://localhost:8080/health
```

### Metadata

```bash
# List all topics and their partitions
curl http://localhost:8080/metadata
```

## HTTP API Reference

### Broker Health

**GET /health**

- Returns broker health status
- Response: `{"status": "healthy"}`

### Metadata

**GET /metadata**

- Lists all topics and their partition counts
- Response:

```json
{
  "topics": [
    {
      "name": "orders",
      "partitions": 3
    },
    {
      "name": "payments",
      "partitions": 2
    }
  ]
}
```

### Publishing Events

**POST /topics/events?topic={topic}**

- Publishes an event to a topic
- Request body:

```json
{
  "key": "user123",
  "payload": {
    "amount": 100,
    "currency": "USD"
  }
}
```

- Response:

```json
{
  "partition": 1,
  "offset": 42
}
```

### Fetching Events

**GET /messages?topic={topic}&partition={partition}&offset={offset}&maxBytes={maxBytes}**

- Fetches events from a partition
- Query parameters:
  - `topic`: Topic name (required)
  - `partition`: Partition ID (required)
  - `offset`: Starting offset (required)
  - `maxBytes`: Maximum bytes to fetch (default: 1048576)
- Response:

```json
{
  "topic": "orders",
  "partition": 0,
  "messages": [
    {
      "offset": 0,
      "timestamp": 1705348332000000000,
      "key": "user123",
      "payload": "<base64-encoded-payload>"
    }
  ]
}
```

### Committing Offsets

**POST /consumer-groups/offsets/commit?group={group}**

- Commits an offset for a consumer group
- Query parameters:
  - `group`: Consumer group name (required)
- Request body:

```json
{
  "topic": "orders",
  "partition": 0,
  "offset": 42
}
```

- Response:

```json
{
  "status": "committed"
}
```

## Data Storage

### Metadata Format

Metadata is stored in `data/metadata.json`:

```json
{
  "orders": {
    "name": "orders",
    "numPartitions": 3,
    "partitions": {}
  },
  "payments": {
    "name": "payments",
    "numPartitions": 2,
    "partitions": {}
  }
}
```

### Log Format

Events are stored in binary format in `data/{topic}/partition-{id}.log`:

- **Offset** (8 bytes): Event offset as uint64
- **Timestamp** (8 bytes): Unix nanosecond timestamp
- **Key Length** (4 bytes): Length of key as uint32
- **Key** (variable): Event key string
- **Payload Length** (4 bytes): Length of payload as uint32
- **Payload** (variable): Event payload bytes

### Offsets Format

Consumer group offsets are stored in `data/offsets.json`:

```json
{
  "billing-service-orders-0": 42,
  "billing-service-orders-1": 35,
  "notification-service-payments-0": 100
}
```

## Partition Routing

Events are routed to partitions using the following logic:

1. **Hash-based routing** (if key is not empty):

   - Compute FNV-1a hash of the key
   - Modulo by number of partitions
   - Example: key="user123" → partition = hash("user123") % 3

2. **Round-robin routing** (if key is empty):
   - Select a random partition
   - Useful for unkeyed events

## Testing

```bash
# Run unit tests
go test ./internal/broker/

# Run with coverage
go test -cover ./internal/broker/
```

Current test coverage: **35.2%** of statements

Tests cover:

- Producer API endpoint (`handlePublishEvent`)
- Consumer API endpoint (`handleFetchMessages`)
- Offset commit endpoint (`handleCommitOffset`)

## Performance Considerations

- **Memory**: Events are loaded in memory per partition (not suitable for billions of events per partition)
- **Throughput**: Single-threaded per partition, suitable for moderate throughput
- **Latency**: Disk-based storage provides durability at the cost of latency
- **Scalability**: Design supports multiple brokers for distributed deployment (future enhancement)

## Future Enhancements

- [ ] Distributed broker cluster with replication
- [ ] Consumer group rebalancing
- [ ] Retention policies (time-based, size-based)
- [ ] Compression support
- [ ] Consumer lag monitoring
- [ ] Metrics and monitoring (Prometheus)
- [ ] Authentication and authorization
- [ ] Topic/partition deletion
- [ ] Transactional writes
- [ ] Stream processing capabilities

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is open source and available under the MIT License.

## Contact & Support

For questions, issues, or suggestions, please open an issue on the repository.

---

Built with ❤️ in Go | Event Streaming for Everyone
