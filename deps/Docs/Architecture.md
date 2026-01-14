# Distributed Event Streaming System Architecture

# 1. Goals & Non-Goals

## Goals
 - Build reliable, replayable event streaming system
 - Increase understanding of:
   - event-driven architecture 
   - log-structured storage
   - Consumer groups & offsets
   - Failure handling and recovery
- Keep system simple 

## Non-Goals
 - exactly-once delivery
 - Cross-datacenter replication
 - Dynamic topic creation via UI
 - Schema registry

# 2. High-Level System Overview 

Producer --> Broker --> Consumer </br>
Broker is system of record. 

# 3. Communication Model

## Protocol Choice:
HTTP + JSON

Why:
- Easy to debug
- Easy to implement in Go
- Swappable later for gRPC

## Communication Pattern
- Producers: push
- Consumers: pull
- Broker: stateful

# 4. Core Data Model
## Topics & Partitions 

```
Topic: orders
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
```
Each partition:
- append-only
- has its own log file
- maintains its own offset sequence

## Event Structure
Logical Event (in Memory / Over network):
```
{
  "key": "order-123",
  "payload": {...}
}
```
Stored event (on disk):
```
[offset][timestamp][key][payload_length][payload_bytes]
```

Offsets are:
- sequential 
- assigned by broker
- never reused 

# 5. Broker Architecture

## Internal Modules
```
Broker
 ├── HTTP Server
 ├── Metadata Manager
 ├── Partition Manager
 ├── Log Storage Engine
 ├── Offset Manager
 └── Coordinator (consumer groups)
```

## 5.1 Metadata Manager
Tracks:
 - Topics
 - Partitions per topic
 - consumer groups
Stored:
 - in memory
 - persisted on disk as JSON

## 5.2 Partition Manager
Responsibilites:
 - Route events to partitions 
 - Enforce partition ownership
 - Expose read/write APIs
Partition Selection:
```
hash(event.key) % numPartitions
```
if no key:
 - round-robin

## 5.3 Log Storage Engine


## Storage Strategy

- One file per partition

- Append-only writes

- Sequential reads

Example:
```
data/
 └── orders/
     ├── partition-0.log
     ├── partition-1.log
     └── partition-2.log
```
## Write Path

- Serialize event

- Append to file

- fsync

- Increment offset

- Acknowledge producer

Guarantee:

- At-least-once delivery

## 5.4 Offset Manager
Tracks:
```
(consumerGroup, topic, partition) → offset
```

Stored:
- In broker memory
- Persisted periodically to disk

Offsets only advance when:
- Consumer explicitly commits

## 5.5 Consumer Group Coordinator

Responsibilities:

- Track active consumers
- Assign partitions
- Handle rebalancing

Rules:
- One partition → one consumer per group
- If consumer dies → rebalance

Rebalancing strategy:

- Stop-the-world (simple, acceptable)

- Reassign partitions

- Resume from last committed offset

# 6. Producer Architecture
Producer Flow
```
Event → Partition Selection → HTTP POST → ACK
```

Responsibilities:
- Partition selection
- Retry on transient failure
- Optional batching (later)

Failure semantics:
- Retry may cause duplicates
- Broker guarantees durability, not uniqueness

# 7. Consumer Architecture
Consumer Flow
```
Poll → Receive batch → Process → Commit offset
```

Key design choice:
- Pull-based consumption

Why:
- Backpressure-friendly
- Simple failure recovery
- Mirrors Kafka

Consumer crash behavior:
- Offset not committed → replay

# 8. Failure Model & Recovery
## Broker Crash
- Logs persist on disk
- On startup:
  - Scan logs
  - Recover last offset per partition

## Consumer Crash
- Partition reassigned
- New consumer resumes from last committed offset

## Producer Crash
- Event may be resent
- Duplicates pos

# 9. Delivery Semantics
## Guarenteed:
- At-least-once delivery
- Ordered delivery per parition
## Not guarenteed:
- exactly-once
- global ordering

# 10. Observability
At minimum:
- Structured logs
- Metrics
  - Events/Sec
  - Lag per consumer group
  - partition sizes

# 11. Scalability Limits
Current Limits:
- single broker
- single-node storage
- fixed partitions
