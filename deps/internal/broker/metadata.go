package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// track topics, partitions, and consumer groups
// persisted to and loaded from disk

// It persists metadata to disk and loads it on startup.
type MetadataManager struct {
	mu     sync.RWMutex
	path   string
	topics map[string]*Topic
}

// create a new MetadataManager.
func NewMetadataManager(path string) *MetadataManager {
	return &MetadataManager{
		path:   path,
		topics: make(map[string]*Topic),
	}
}

// Save persists the metadata to disk.
func (m *MetadataManager) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, err := os.Create(m.path)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(m.topics); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	return nil
}

// Load loads the metadata from disk.
func (m *MetadataManager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := os.Open(m.path)
	if os.IsNotExist(err) {
		// No metadata file exists; this is fine for a fresh start.
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&m.topics); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	return nil
}

// return a copy of all topics.
func (m *MetadataManager) GetTopics() map[string]*Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copy := make(map[string]*Topic, len(m.topics))
	for name, topic := range m.topics {
		copy[name] = topic
	}

	return copy
}

// add a new topic to the metadata.
func (m *MetadataManager) AddTopic(name string, topic *Topic) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	m.topics[name] = topic
	return nil
}
