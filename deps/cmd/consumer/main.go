package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func main() {
	// Command-line flags
	broker := flag.String("broker", "localhost:8080", "Broker address (host:port)")
	topic := flag.String("topic", "", "Topic name")
	group := flag.String("group", "default", "Consumer group name")
	partition := flag.Int("partition", 0, "Partition ID to consume from")
	offset := flag.Int64("offset", 0, "Starting offset (0 = earliest)")
	maxBytes := flag.Int("maxBytes", 1048576, "Maximum bytes to fetch (default 1MB)")
	count := flag.Int("count", 10, "Number of messages to fetch (0 = fetch once)")
	commitInterval := flag.Duration("commitInterval", 5*time.Second, "Interval to commit offsets")
	flag.Parse()

	// Validate flags
	if *topic == "" {
		log.Fatal("Topic is required (use -topic)")
	}

	fmt.Printf("Starting consumer...\n")
	fmt.Printf("  Broker:         %s\n", *broker)
	fmt.Printf("  Topic:          %s\n", *topic)
	fmt.Printf("  Group:          %s\n", *group)
	fmt.Printf("  Partition:      %d\n", *partition)
	fmt.Printf("  Starting offset: %d\n", *offset)
	fmt.Printf("  Max bytes:      %d\n", *maxBytes)
	fmt.Printf("\n")

	currentOffset := *offset
	messagesConsumed := 0
	commitTicker := time.NewTicker(*commitInterval)
	defer commitTicker.Stop()

	for {
		// Fetch messages
		url := fmt.Sprintf(
			"http://%s/messages?topic=%s&partition=%d&offset=%d&maxBytes=%d",
			*broker, *topic, *partition, currentOffset, *maxBytes,
		)

		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Failed to fetch messages: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Failed to read response: %v", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Failed to fetch messages (status %d): %s", resp.StatusCode, string(body))
			time.Sleep(1 * time.Second)
			continue
		}

		// Parse the response
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			log.Printf("Failed to parse response: %v", err)
			continue
		}

		// Process messages
		messages, ok := result["messages"].([]interface{})
		if !ok || len(messages) == 0 {
			// No messages, wait and retry
			fmt.Printf("No messages available. Waiting...\n")
			time.Sleep(1 * time.Second)
			continue
		}

		for _, msg := range messages {
			msgMap := msg.(map[string]interface{})
			offset := int64(msgMap["offset"].(float64))
			key := msgMap["key"].(string)
			payload := msgMap["payload"].(string) // Payload is bytes encoded as base64 string

			fmt.Printf("[%s] Offset: %d | Key: %s | Payload: %s\n",
				time.Now().Format("15:04:05"),
				offset,
				key,
				payload,
			)

			currentOffset = offset + 1
			messagesConsumed++
		}

		// Check if we should stop
		if *count > 0 && messagesConsumed >= *count {
			fmt.Printf("\nConsumed %d messages. Exiting.\n", messagesConsumed)
			break
		}

		// Try to commit offset
		select {
		case <-commitTicker.C:
			if err := commitOffset(*broker, *group, *topic, *partition, currentOffset); err != nil {
				log.Printf("Failed to commit offset: %v", err)
			} else {
				fmt.Printf("[%s] Committed offset: %d\n", time.Now().Format("15:04:05"), currentOffset)
			}
		default:
		}
	}

	// Final commit
	if err := commitOffset(*broker, *group, *topic, *partition, currentOffset); err != nil {
		log.Printf("Failed to commit final offset: %v", err)
	} else {
		fmt.Printf("Final offset committed: %d\n", currentOffset)
	}
}

// Send a commit offset request to the broker
func commitOffset(broker, group, topic string, partition int, offset int64) error {
	url := fmt.Sprintf("http://%s/consumer-groups/offsets/commit?group=%s", broker, group)

	commitData := map[string]interface{}{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	}

	commitJSON, err := json.Marshal(commitData)
	if err != nil {
		return fmt.Errorf("failed to marshal commit data: %w", err)
	}

	resp, err := http.Post(url, "application/json", strings.NewReader(string(commitJSON)))
	if err != nil {
		return fmt.Errorf("failed to send commit request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit failed (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}
