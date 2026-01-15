package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	// Command-line flags
	broker := flag.String("broker", "localhost:8080", "Broker address (host:port)")
	topic := flag.String("topic", "", "Topic name")
	key := flag.String("key", "", "Event key")
	payload := flag.String("payload", "{}", "Event payload (JSON)")
	flag.Parse()

	// Validate flags
	if *topic == "" {
		log.Fatal("Topic is required (use -topic)")
	}

	// Parse the payload JSON
	var payloadData map[string]interface{}
	if err := json.Unmarshal([]byte(*payload), &payloadData); err != nil {
		log.Fatalf("Invalid payload JSON: %v", err)
	}

	// Create the event
	event := map[string]interface{}{
		"key":     *key,
		"payload": payloadData,
	}

	// Marshal the event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("Failed to marshal event: %v", err)
	}

	// Send the event to the broker
	url := fmt.Sprintf("http://%s/topics/events?topic=%s", *broker, *topic)
	resp, err := http.Post(url, "application/json", bytes.NewReader(eventJSON))
	if err != nil {
		log.Fatalf("Failed to publish event: %v", err)
	}
	defer resp.Body.Close()

	// Read the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response: %v", err)
	}

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to publish event (status %d): %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	// Print success
	fmt.Printf("Event published successfully!\n")
	fmt.Printf("  Timestamp: %s\n", time.Now().Format(time.RFC3339))
	fmt.Printf("  Topic:     %s\n", *topic)
	fmt.Printf("  Key:       %s\n", *key)
	fmt.Printf("  Partition: %.0f\n", result["partition"])
	fmt.Printf("  Offset:    %.0f\n", result["offset"])
}
