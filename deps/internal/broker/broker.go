package broker

import (
	"fmt"
	"os"
	"sync"
)

// Broker manages topics, partitions, and consumer groups.
type Broker struct {
	port           int
	topics         map[string]*Topic
	consumerGroups map[string]*ConsumerGroup
	offsets        *ConsumerGroupOffsets
	dataDir        string
	httpServer     *HTTPServer
	metadata       *MetadataManager

	// Mutex to protect concurrent access to broker state
	mu sync.RWMutex
}

// NewBroker creates a new Broker instance.
func NewBroker(port int, dataDir string) *Broker {
	metadataPath := fmt.Sprintf("%s/metadata.json", dataDir)
	return &Broker{
		port:           port,
		topics:         make(map[string]*Topic),
		consumerGroups: make(map[string]*ConsumerGroup),
		offsets: &ConsumerGroupOffsets{
			offsets: make(map[string]int64),
		},
		dataDir:  dataDir,
		metadata: NewMetadataManager(metadataPath),
	}
}

// Initialize the broker and begin accepting HTTP requests.
func (b *Broker) Start() error {
	// Ensure data directory exists
	if err := os.MkdirAll(b.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Load metadata
	if err := b.metadata.Load(); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

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
	if _, exists := b.metadata.GetTopics()[name]; exists {
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

		// Initialize log storage for each partition
		logStorage, err := NewLogStorage(partition.logPath)
		if err != nil {
			return fmt.Errorf("failed to initialize log storage for partition %d: %w", i, err)
		}
		partition.logStorage = logStorage

		topic.Partitions[i] = partition
	}

	if err := b.metadata.AddTopic(name, topic); err != nil {
		return err
	}

	// Persist metadata
	if err := b.metadata.Save(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

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
