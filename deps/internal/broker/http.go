package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
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

	// will be implemented in steps 7 and 8:
	// Producer: publish events to a topic
	// s.mux.HandleFunc("/topics/{topic}/events", s.handlePublishEvent)

	// Consumer: fetch messages from a partition
	// s.mux.HandleFunc("/topics/{topic}/partitions/{partition}/messages", s.handleFetchMessages)

	// Consumer group management
	// s.mux.HandleFunc("/consumer-groups/{group}/members", s.handleJoinGroup)
	// s.mux.HandleFunc("/consumer-groups/{group}/offsets/commit", s.handleCommitOffset)
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
