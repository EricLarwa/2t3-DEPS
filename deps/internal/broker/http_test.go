package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestHandlePublishEvent(t *testing.T) {
	s := setupTestServer() // Assume this sets up a test HTTP server with a mock broker

	topic := "test-topic"
	event := map[string]interface{}{
		"key": "test-key",
		"payload": map[string]interface{}{
			"field1": "value1",
			"field2": 42,
		},
	}

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/topics/events?topic="+topic, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %v", rec.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if _, ok := response["partition"]; !ok {
		t.Errorf("Response missing 'partition' field")
	}

	if _, ok := response["offset"]; !ok {
		t.Errorf("Response missing 'offset' field")
	}
}

func TestHandleFetchMessages(t *testing.T) {
	s := setupTestServer()

	topic := "test-topic"

	req := httptest.NewRequest(http.MethodGet, "/messages?topic="+topic+"&partition=0&offset=0&maxBytes=1048576", nil)
	rec := httptest.NewRecorder()

	s.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %v", rec.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if _, ok := response["topic"]; !ok {
		t.Errorf("Response missing 'topic' field")
	}

	if _, ok := response["partition"]; !ok {
		t.Errorf("Response missing 'partition' field")
	}

	if _, ok := response["messages"]; !ok {
		t.Errorf("Response missing 'messages' field")
	}
}

func TestHandleCommitOffset(t *testing.T) {
	s := setupTestServer()

	consumerGroup := "test-group"
	commitRequest := map[string]interface{}{
		"topic":     "test-topic",
		"partition": 0,
		"offset":    100,
	}

	body, err := json.Marshal(commitRequest)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/consumer-groups/offsets/commit?group="+consumerGroup, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %v", rec.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if _, ok := response["status"]; !ok {
		t.Errorf("Response missing 'status' field")
	}
}

// setupTestServer sets up a test HTTP server with a mock broker.
func setupTestServer() *HTTPServer {
	broker := &Broker{
		topics: make(map[string]*Topic),
	}

	// Initialize partition manager with reference to broker
	broker.partitionManager = NewPartitionManager(broker)
	broker.offsetManager = NewOffsetManager("") // Mock with empty path

	// Create a test topic with partitions
	topic := &Topic{
		Name:          "test-topic",
		NumPartitions: 3,
		Partitions:    make(map[int]*Partition),
	}

	// Initialize partitions for the topic
	for i := 0; i < topic.NumPartitions; i++ {
		// Create a temporary log file for testing
		tmpfile, err := os.CreateTemp("", "test-partition-*.log")
		if err != nil {
			panic(err)
		}
		defer tmpfile.Close()

		logStorage, err := NewLogStorage(tmpfile.Name())
		if err != nil {
			panic(err)
		}

		partition := &Partition{
			Topic:         topic.Name,
			ID:            i,
			logPath:       tmpfile.Name(),
			currentOffset: 0,
			events:        make([]*StoredEvent, 0),
			logStorage:    logStorage,
		}
		topic.Partitions[i] = partition
	}

	broker.topics["test-topic"] = topic

	s := &HTTPServer{
		mux:    http.NewServeMux(),
		broker: broker,
	}
	s.registerRoutes()
	return s
}
