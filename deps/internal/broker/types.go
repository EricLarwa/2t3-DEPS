package broker

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

// Event represents a logical event that producers publish.
// maps to what travels over the network (HTTP + JSON).
type Event struct {
	Key     string                 `json:"key"`
	Payload map[string]interface{} `json:"payload"`
}

// Events are persisted on disk in the log file.
// Binary format: [offset][timestamp][key][payload_length][payload_bytes]
type StoredEvent struct {
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Payload   []byte `json:"payload"`
}

// Partition represents a single partition within a topic.
type Partition struct {
	// Topic and ID identify this partition uniquely.
	Topic string
	ID    int

	// mu protects all fields below (log, offset, events).
	mu sync.RWMutex

	// holds the path to the partition's log file on disk.
	// Pattern: data/{topic}/partition-{id}.log
	logPath string

	// next offset to assign to an event.
	// Starts at 0 and increments monotonically.
	currentOffset int64

	// hold all events in memory (loaded from disk on startup).
	// In a production system, this would be limited (e.g., keep last N events).
	events []*StoredEvent

	// LogStorage handles event storage for this partition.
	logStorage *LogStorage
}

type Topic struct {
	// Name uniquely identifies the topic.
	Name string

	// how many partitions this topic has.
	// Once set, this is fixed
	NumPartitions int

	// Partitions is a map of partition ID to Partition.
	Partitions map[int]*Partition

	// mu protects Partitions map access.
	mu sync.RWMutex
}

// track offsets per consumer group, topic, and partition.
// Structure: (consumerGroup, topic, partition) → lastCommittedOffset
type ConsumerGroupOffsets struct {
	// Key is "consumerGroup-topic-partition", Value is the last committed offset.
	mu      sync.RWMutex
	offsets map[string]int64
}

// Track active consumers and partition assignments.
type ConsumerGroup struct {
	// Name uniquely identifies the consumer group.
	Name string

	// In a real system, this would have heartbeat/session tracking.
	mu              sync.RWMutex
	ActiveConsumers map[string]bool // consumerID → is active

	// maps topic-partition to consumer ID.
	PartitionAssignments map[string]string // "{topic}-{partitionID}" → consumerID

	// LastRebalance tracks when the group last rebalanced.
	LastRebalance time.Time
}

// Routing and operations for partitions.
type PartitionManager struct {
	broker *Broker
}

func NewPartitionManager(broker *Broker) *PartitionManager {
	return &PartitionManager{
		broker: broker,
	}
}

// Determine the partition for an event based on its key.
// If the key is empty, use round-robin routing.
func (p *PartitionManager) RouteEvent(topic string, key string) (*Partition, error) {
	t := p.broker.GetTopic(topic)
	if t == nil {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	if key != "" {
		// Hash-based routing
		h := fnv.New32a()
		h.Write([]byte(key))
		partitionID := int(h.Sum32()) % t.NumPartitions
		return t.Partitions[partitionID], nil
	}

	// Round-robin routing
	partitionID := rand.Intn(t.NumPartitions)
	return t.Partitions[partitionID], nil
}

// Fetch events from a partition starting at a given offset.
func (p *PartitionManager) FetchEvents(partition *Partition, startOffset int64, maxBytes int) ([]*StoredEvent, error) {
	return partition.logStorage.Read(startOffset, maxBytes)
}

// Commit the offset for a consumer group, topic, and partition.
func (p *PartitionManager) CommitOffset(consumerGroup, topic string, partitionID int, offset int64) error {
	return nil
}
