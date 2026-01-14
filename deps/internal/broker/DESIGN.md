# Broker Package Design

## Overview
The broker package implements the core event streaming system. It's the "system of record" that producers push events to and consumers pull from.

## Key Design Decisions & Rationale

### 1. **Event vs StoredEvent** (Two representations)
- **Event**: The logical representation transmitted over HTTP (key + payload as JSON)
- **StoredEvent**: The on-disk binary format (offset + timestamp + key + payload bytes)

**Why separate?** 
- Network layer needs clean JSON (for HTTP/debugging)
- Disk storage needs efficiency (binary format, fixed offsets)
- Conversion happens at boundaries (HTTP handler → storage engine)

### 2. **Partition as Core Unit**
Each partition is:
- Append-only (immutable log structure)
- Isolated on disk (one file per partition)
- Independently trackable by offsets

**Why?**
- **Parallelism**: Multiple producers can write to different partitions simultaneously
- **Scalability**: Offsets are per-partition, not global (no bottleneck)
- **Failure recovery**: Partition can be replayed independently

### 3. **Mutex Protection Strategy**
Each component (Partition, Topic, ConsumerGroup, Broker) has its own `mu sync.RWMutex`

**Why?**
- **Fine-grained locking**: Different parts can operate concurrently
- **RWMutex choice**: Most operations are reads (fetching events), writes are rarer (publishing)
- **Prevents data races**: Go concurrency is safe when accessed through locks

### 4. **ConsumerGroupOffsets as Separate Struct**
Instead of embedding offsets in ConsumerGroup, it's its own tracked entity

**Why?**
- **Persistence boundary**: Offsets are committed/persisted separately from group membership
- **Simplifies tracking**: "(group, topic, partition) → offset" is the atomic unit
- **Enables recovery**: Can reload offset state independently of consumer connections

### 5. **Offset Assignment by Broker** (Not distributed)
The broker assigns offsets sequentially and durably

**Why?**
- **Simplicity**: No distributed consensus needed (stop-the-world model)
- **Correctness**: Offsets are never duplicated or skipped
- **Ordering**: Ensures events are processed in arrival order

## Next Steps
In Step 2, we'll implement the HTTP server that:
- Exposes the broker's capabilities via REST endpoints
- Handles JSON serialization/deserialization
- Routes requests to the appropriate broker components
