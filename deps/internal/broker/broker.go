package broker

import (
	"fmt"
	"os"
)

// Initialize the broker and begin accepting HTTP requests.
func (b *Broker) Start() error {
	// Ensure data directory exists
	if err := os.MkdirAll(b.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// TODO: Load persisted metadata in step 3 (Metadata Manager)
	// This would:
	// - Read topics.json from disk
	// - Read consumer group offsets from disk
	// - Populate b.topics and b.offsets

	// Create HTTP server
	b.httpServer = NewHTTPServer(b, b.port)

	// Start listening (blocks until error or shutdown)
	return b.httpServer.Start()
}

// New topic with the specified number of partitions.
func (b *Broker) AddTopic(name string, numPartitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if topic already exists
	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	// Create topic and its partitions
	topic := &Topic{
		Name:          name,
		NumPartitions: numPartitions,
		Partitions:    make(map[int]*Partition),
	}

	// Create each partition
	for i := 0; i < numPartitions; i++ {
		partition := &Partition{
			Topic:         name,
			ID:            i,
			logPath:       fmt.Sprintf("%s/%s/partition-%d.log", b.dataDir, name, i),
			currentOffset: 0,
			events:        make([]*StoredEvent, 0),
		}

		topic.Partitions[i] = partition

		// TODO: In step 4 (Log Storage Engine), we'll:
		// - Create the log file on disk
		// - Load any existing events from disk
	}

	b.topics[name] = topic
	return nil
}

// GetTopic retrieves a topic by name.
// Returns nil if topic doesn't exist.
func (b *Broker) GetTopic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

// GetPartition retrieves a specific partition from a topic.
// Return error if topic or partition doesn't exist.
func (b *Broker) GetPartition(topic string, partitionID int) (*Partition, error) {
	t := b.GetTopic(topic)
	if t == nil {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	partition, exists := t.Partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found in topic %q", partitionID, topic)
	}

	return partition, nil
}
