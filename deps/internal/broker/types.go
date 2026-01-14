package broker

import (
	"sync"
	"time"
)

// Event represents a logical event that producers publish.
// It maps to what travels over the network (HTTP + JSON).
// Why this structure:
// - key: used to determine partition via hashing. If empty, round-robin is used.
// - payload: arbitrary JSON data that the producer sends.
type Event struct {
	Key     string                 `json:"key"`
	Payload map[string]interface{} `json:"payload"`
}

// StoredEvent is how events are persisted on disk in the log file.
// Binary format: [offset][timestamp][key][payload_length][payload_bytes]
// Why this structure:
// - Offset: sequential ID assigned by broker, never reused. Enables replay.
// - Timestamp: when broker received the event (useful for ordering, debugging).
// - Key: preserved for reference.
// - Payload: the actual event data as JSON bytes.
type StoredEvent struct {
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Payload   []byte `json:"payload"`
}

// Partition represents a single partition within a topic.
// Why partitions:
// - Each partition is append-only and has its own log file (immutable log structure).
// - Enables parallelism: producers can write to different partitions simultaneously.
// - Enables consumer parallelism: different consumers can read different partitions.
type Partition struct {
	// Topic and ID identify this partition uniquely.
	Topic string
	ID    int

	// mu protects all fields below (log, offset, events).
	mu sync.RWMutex

	// log holds the path to the partition's log file on disk.
	// Pattern: data/{topic}/partition-{id}.log
	logPath string

	// currentOffset is the next offset to assign to an event.
	// Starts at 0 and increments monotonically.
	currentOffset int64

	// events holds all events in memory (loaded from disk on startup).
	// In a production system, this would be limited (e.g., keep last N events).
	events []*StoredEvent
}

// Topic represents a topic and its partitions.
// Why this structure:
// - Topics are the abstraction for event streams (e.g., "orders", "payments").
// - Each topic is divided into partitions for parallelism and scalability.
type Topic struct {
	// Name uniquely identifies the topic.
	Name string

	// NumPartitions defines how many partitions this topic has.
	// Once set, this is fixed (dynamic topic creation is a non-goal).
	NumPartitions int

	// Partitions is a map of partition ID to Partition.
	Partitions map[int]*Partition

	// mu protects Partitions map access.
	mu sync.RWMutex
}

// ConsumerGroupOffsets tracks offsets per consumer group, topic, and partition.
// Structure: (consumerGroup, topic, partition) → lastCommittedOffset
// Why this design:
// - Each consumer group maintains independent offset tracking (different groups can replay).
// - Offsets are partition-specific (each partition has its own sequence).
// - Only advances when consumer explicitly commits (no auto-commit complexity for now).
type ConsumerGroupOffsets struct {
	// Key is "consumerGroup-topic-partition", Value is the last committed offset.
	mu      sync.RWMutex
	offsets map[string]int64
}

// ConsumerGroup tracks active consumers and partition assignments.
// Why this structure:
// - Enables scaling: partition → consumer mapping allows multiple consumers to process in parallel.
// - Enables recovery: when a consumer dies, rebalancing reassigns partitions.
type ConsumerGroup struct {
	// Name uniquely identifies the consumer group.
	Name string

	// ActiveConsumers tracks currently active consumer IDs.
	// In a real system, this would have heartbeat/session tracking.
	mu              sync.RWMutex
	ActiveConsumers map[string]bool // consumerID → is active

	// PartitionAssignments maps topic-partition to consumer ID.
	// Example: "orders-0" → "consumer-1"
	PartitionAssignments map[string]string // "{topic}-{partitionID}" → consumerID

	// LastRebalance tracks when the group last rebalanced.
	LastRebalance time.Time
}

// Broker is the main server struct coordinating all components.
// Why this structure:
// - Central coordinator: all requests flow through the broker.
// - Tracks all state: topics, partitions, consumer groups, offsets.
// - Manages the HTTP server and request handling.
type Broker struct {
	// Basic config
	port int

	// Topics indexed by name.
	mu     sync.RWMutex
	topics map[string]*Topic

	// Consumer groups indexed by name.
	consumerGroups map[string]*ConsumerGroup

	// Consumer group offsets tracking.
	offsets *ConsumerGroupOffsets

	// Data directory path (where log files are stored).
	dataDir string

	// HTTP server (will be set up in step 2).
	httpServer interface{} // We'll define this more specifically in step 2
}

// NewBroker creates a new broker instance with the given configuration.
// This will be expanded in step 2 when we set up the HTTP server.
func NewBroker(port int, dataDir string) *Broker {
	return &Broker{
		port:           port,
		topics:         make(map[string]*Topic),
		consumerGroups: make(map[string]*ConsumerGroup),
		offsets: &ConsumerGroupOffsets{
			offsets: make(map[string]int64),
		},
		dataDir: dataDir,
	}
}
