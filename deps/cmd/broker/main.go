package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"example.com/deps/internal/broker"
)

func main() {
	// Command-line flags
	port := flag.Int("port", 8080, "Port to listen on")
	dataDir := flag.String("data-dir", "./data", "Directory to store broker data")
	flag.Parse()

	// Validate flags
	if *port <= 0 || *port > 65535 {
		log.Fatal("Invalid port number")
	}

	// Convert to absolute path
	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("Failed to resolve data directory: %v", err)
	}

	fmt.Printf("Starting broker...\n")
	fmt.Printf("  Port: %d\n", *port)
	fmt.Printf("  Data directory: %s\n", absDataDir)

	// Create broker instance
	b := broker.NewBroker(*port, absDataDir)

	// Add some test topics
	testTopics := map[string]int{
		"orders":    3,
		"payments":  2,
		"shipments": 1,
	}

	for name, partitions := range testTopics {
		if err := b.AddTopic(name, partitions); err != nil {
			log.Fatalf("Failed to add topic %q: %v", name, err)
		}
		fmt.Printf("Created topic %q with %d partitions\n", name, partitions)
	}

	// Start the broker (blocks until error)
	if err := b.Start(); err != nil {
		log.Fatalf("Broker failed: %v", err)
	}
}
