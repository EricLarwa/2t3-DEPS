package broker

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Handle reading and writing events to partition log files.
// Each partition has its own LogStorage instance.
type LogStorage struct {
	file   *os.File
	path   string
	offset int64
}

// Create a new LogStorage instance for a partition.
func NewLogStorage(path string) (*LogStorage, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Get the current file size to determine the starting offset
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}

	return &LogStorage{
		file:   file,
		path:   path,
		offset: info.Size(),
	}, nil
}

// Write an event to the log file and returns its offset.
func (l *LogStorage) Append(event *StoredEvent) (int64, error) {
	// Serialize the event
	data, err := serializeEvent(event)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize event: %w", err)
	}

	// Write the serialized data to the file
	n, err := l.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write to log file: %w", err)
	}

	// Update the offset
	currentOffset := l.offset
	l.offset += int64(n)

	return currentOffset, nil
}

// Read reads events from the log file starting at the given offset.
func (l *LogStorage) Read(startOffset int64, maxBytes int) ([]*StoredEvent, error) {
	if _, err := l.file.Seek(startOffset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek log file: %w", err)
	}

	buffer := make([]byte, maxBytes)
	n, err := l.file.Read(buffer)
	if n == 0 {
		// Return empty slice for empty reads (EOF)
		return make([]*StoredEvent, 0), nil
	}
	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read from log file: %w", err)
	}

	events, err := deserializeEvents(buffer[:n])
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize events: %w", err)
	}

	return events, nil
}

// Close closes the log file.
func (l *LogStorage) Close() error {
	return l.file.Close()
}

// Convert a StoredEvent to binary format.
func serializeEvent(event *StoredEvent) ([]byte, error) {
	// Example serialization: [offset][timestamp][key][payload_length][payload_bytes]
	keyBytes := []byte(event.Key)
	payloadLength := len(event.Payload)
	totalSize := 8 + 8 + len(keyBytes) + 4 + payloadLength
	buffer := make([]byte, totalSize)

	binary.BigEndian.PutUint64(buffer[0:8], uint64(event.Offset))
	binary.BigEndian.PutUint64(buffer[8:16], uint64(event.Timestamp))
	copy(buffer[16:16+len(keyBytes)], keyBytes)
	binary.BigEndian.PutUint32(buffer[16+len(keyBytes):20+len(keyBytes)], uint32(payloadLength))
	copy(buffer[20+len(keyBytes):], event.Payload)

	return buffer, nil
}

// Convert binary data to a slice of StoredEvent.
func deserializeEvents(data []byte) ([]*StoredEvent, error) {
	// Example deserialization logic
	var events []*StoredEvent
	for len(data) > 0 {
		offset := int64(binary.BigEndian.Uint64(data[0:8]))
		timestamp := int64(binary.BigEndian.Uint64(data[8:16]))
		keyLength := 16 // Assume fixed key length for simplicity
		key := string(data[16 : 16+keyLength])
		payloadLength := int(binary.BigEndian.Uint32(data[16+keyLength : 20+keyLength]))
		payload := data[20+keyLength : 20+keyLength+payloadLength]

		events = append(events, &StoredEvent{
			Offset:    offset,
			Timestamp: timestamp,
			Key:       key,
			Payload:   payload,
		})

		data = data[20+keyLength+payloadLength:]
	}

	return events, nil
}
