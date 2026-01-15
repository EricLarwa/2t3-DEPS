package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// wrap the broker and exposes it via HTTP endpoints.
type HTTPServer struct {
	broker *Broker
	mux    *http.ServeMux
	port   int
}

// New HTTP server for the broker.
// sets up all the route handlers that will process requests.
func NewHTTPServer(broker *Broker, port int) *HTTPServer {
	server := &HTTPServer{
		broker: broker,
		mux:    http.NewServeMux(),
		port:   port,
	}

	server.registerRoutes()

	return server
}

// registerRoutes sets up all the HTTP routes.
func (s *HTTPServer) registerRoutes() {

	s.mux.HandleFunc("/health", s.handleHealth)

	// Metadata endpoint (list topics and their partitions)
	s.mux.HandleFunc("/metadata", s.handleMetadata)

	// Producer: publish events to a topic
	s.mux.HandleFunc("/topics/events", s.handlePublishEvent)

	// Consumer: fetch messages from a partition
	s.mux.HandleFunc("/messages", s.handleFetchMessages)

	// Consumer group management: commit offsets
	s.mux.HandleFunc("/consumer-groups/offsets/commit", s.handleCommitOffset)
}

// return the server status.
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

// return information about all topics and their partitions.
func (s *HTTPServer) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.broker.mu.RLock()
	defer s.broker.mu.RUnlock()

	// Build response: list all topics and their partition counts
	type TopicInfo struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
	}

	type MetadataResponse struct {
		Topics []TopicInfo `json:"topics"`
	}

	topics := make([]TopicInfo, 0, len(s.broker.topics))
	for _, topic := range s.broker.topics {
		topics = append(topics, TopicInfo{
			Name:       topic.Name,
			Partitions: topic.NumPartitions,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(MetadataResponse{Topics: topics})
}

// Handle publishing events to a topic.
func (s *HTTPServer) handlePublishEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Missing topic", http.StatusBadRequest)
		return
	}

	var event Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid event format", http.StatusBadRequest)
		return
	}

	partition, err := s.broker.partitionManager.RouteEvent(topic, event.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		http.Error(w, "Failed to serialize payload", http.StatusInternalServerError)
		return
	}

	storedEvent := &StoredEvent{
		Offset:    partition.currentOffset,
		Timestamp: time.Now().UnixNano(),
		Key:       event.Key,
		Payload:   payloadBytes,
	}
	offset, err := partition.logStorage.Append(storedEvent)
	if err != nil {
		http.Error(w, "Failed to append event", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"partition": partition.ID,
		"offset":    offset,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleFetchMessages handles fetching messages from a partition.
func (s *HTTPServer) handleFetchMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	maxBytesStr := r.URL.Query().Get("maxBytes")

	if topic == "" || partitionStr == "" || offsetStr == "" {
		http.Error(w, "Missing required parameters: topic, partition, offset", http.StatusBadRequest)
		return
	}

	var partitionID int
	var startOffset int64
	var maxBytes int

	if _, err := fmt.Sscanf(partitionStr, "%d", &partitionID); err != nil {
		http.Error(w, "Invalid partition ID", http.StatusBadRequest)
		return
	}

	if _, err := fmt.Sscanf(offsetStr, "%d", &startOffset); err != nil {
		http.Error(w, "Invalid offset", http.StatusBadRequest)
		return
	}

	maxBytes = 1048576 // Default 1MB
	if maxBytesStr != "" {
		if _, err := fmt.Sscanf(maxBytesStr, "%d", &maxBytes); err != nil {
			http.Error(w, "Invalid maxBytes", http.StatusBadRequest)
			return
		}
	}

	t := s.broker.GetTopic(topic)
	if t == nil {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	partition, ok := t.Partitions[partitionID]
	if !ok {
		http.Error(w, "Partition not found", http.StatusNotFound)
		return
	}

	events, err := s.broker.partitionManager.FetchEvents(partition, startOffset, maxBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"topic":     topic,
		"partition": partitionID,
		"messages":  events,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleCommitOffset handles committing offsets for a consumer group.
func (s *HTTPServer) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	consumerGroup := r.URL.Query().Get("group")
	if consumerGroup == "" {
		http.Error(w, "Missing consumer group", http.StatusBadRequest)
		return
	}

	var commitRequest struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
	}

	if err := json.NewDecoder(r.Body).Decode(&commitRequest); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.broker.partitionManager.CommitOffset(consumerGroup, commitRequest.Topic, commitRequest.Partition, commitRequest.Offset); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status": "committed",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Begin listening for HTTP requests on the configured port.
func (s *HTTPServer) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	fmt.Printf("Broker HTTP server listening on %s\n", addr)
	return http.ListenAndServe(addr, s.mux)
}

// Allows the HTTPServer to be used directly as a handler.
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}
