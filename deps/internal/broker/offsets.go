package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Handle tracking and committing consumer offsets.
type OffsetManager struct {
	mu      sync.RWMutex
	path    string
	offsets map[string]int64 // Key: "consumerGroup-topic-partition", Value: offset
}

func NewOffsetManager(path string) *OffsetManager {
	return &OffsetManager{
		path:    path,
		offsets: make(map[string]int64),
	}
}

// Commit the offset for a consumer group, topic, and partition.
func (o *OffsetManager) CommitOffset(consumerGroup, topic string, partitionID int, offset int64) error {
	key := fmt.Sprintf("%s-%s-%d", consumerGroup, topic, partitionID)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.offsets[key] = offset
	return o.save()
}

// Retrieve the committed offset for a consumer group, topic, and partition.
func (o *OffsetManager) GetOffset(consumerGroup, topic string, partitionID int) (int64, error) {
	key := fmt.Sprintf("%s-%s-%d", consumerGroup, topic, partitionID)
	o.mu.RLock()
	defer o.mu.RUnlock()
	offset, exists := o.offsets[key]
	if !exists {
		return 0, fmt.Errorf("offset not found for %s", key)
	}
	return offset, nil
}

// Persists the offsets to disk.
func (o *OffsetManager) save() error {
	file, err := os.Create(o.path)
	if err != nil {
		return fmt.Errorf("failed to create offsets file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(o.offsets); err != nil {
		return fmt.Errorf("failed to encode offsets: %w", err)
	}

	return nil
}

// Load the offsets from disk.
func (o *OffsetManager) load() error {
	file, err := os.Open(o.path)
	if os.IsNotExist(err) {
		// No offsets file exists; fresh start.
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to open offsets file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&o.offsets); err != nil {
		return fmt.Errorf("failed to decode offsets: %w", err)
	}

	return nil
}
