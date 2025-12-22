# Core Systems

This document provides a detailed technical overview of the three major subsystems that comprise cursus's core functionality:
- the Server System, Topic Management System, and Disk Persistence System.

Each system operates semi-independently while integrating through well-defined interfaces to provide a complete message broker implementation

For details about message flow through these systems, see [Message Flow](./message-flow.md). For information about getting started with cursus, see [Getting Started](../user-guide/README.md).

## System Architecture Overview

The cursus architecture consists of three primary systems that work together to provide message broker functionality:

| System                   | Primary Component                                      | Responsibility                                       |
|--------------------------|---------------------------------------------------------|------------------------------------------------------|
| Server System            | `pkg/server/main.go`                                      | TCP connection handling, worker pool management, command routing |
| Topic Management System  | `pkg/topic/manager.go`, `pkg/topic/topic.go`               | Message routing, partition management, consumer group coordination |
| Disk Persistence System  | `pkg/disk/flush_common.go`, `pkg/disk/manager.go`          | Asynchronous disk writes, segment management, message durability |


## Core System Integration

### Key Data Structures and Their Roles

The following table maps the primary data structures to their locations in the codebase:

| Structure     | Key Fields                                         | Purpose                                               |
|---------------|----------------------------------------------------|-------------------------------------------------------|
| TopicManager  | topics, dedupMap, hp                               | Central registry for all topics, handles deduplication |
| Topic         | Name, Partitions, consumerGroups, counter          | Manages partitions and consumer groups for a topic     |
| Partition     | id, ch, subs, dh                                   | Handles message distribution for one partition         |
| DiskManager   | handlers, cfg                                      | Registry for DiskHandler instances                     |
| DiskHandler   | writeCh, file, writer, CurrentSegment              | Manages writes to a single topic-partition             |

### Inter-System Communication Interfaces

The three systems communicate through well-defined interfaces. The following table documents the key integration points:

| From System     | To System            | Interface/Method                     | Purpose                           |
|-----------------|--------------------|-------------------------------------|-----------------------------------|
| Server          | Topic Management    | `TopicManager.Publish()`              | Submit message for routing        |
| Server          | Topic Management    | `TopicManager.RegisterConsumerGroup()`| Register consumer group            |
| Server          | Disk Persistence    | `DiskHandler.ReadMessages()`          | Read messages from disk           |
| Topic Management| Disk Persistence    | `HandlerProvider.GetHandler()`        | Obtain DiskHandler instance       |
| Partition       | DiskHandler         | `DiskAppender.AppendMessage()`        | Write message to disk             |


## Configuration Integration

All three systems are configured through the central config.Config structure. The following table shows which configuration parameters affect each system:

| System             | Configuration Parameters                             | Purpose                                      |
|-------------------|-------------------------------------------------------|----------------------------------------------|
| Server System      | BrokerPort (default: 9000)                            | TCP listener port                             |
| Server System      | HealthCheckPort (default: 9080)                       | Health check HTTP endpoint                    |
| Server System      | ExporterPort (default: 9100)                          | Prometheus metrics endpoint                   |
| Server System      | UseTLS, TLSCert                                       | TLS encryption configuration                  |
| Server System      | EnableGzip                                            | Message compression                           |
| Topic Management   | CleanupInterval (default: 60s)                        | Deduplication map cleanup frequency           |
| Topic Management   | N/A                                                   | Partition buffer size is hardcoded to 10000  |
| Topic Management   | N/A                                                   | Consumer channel buffer is hardcoded to 1000 |
| Disk Persistence   | LogDir                                                | Directory for segment files                   |
| Disk Persistence   | BatchSize                                             | Messages per batch before flush               |
| Disk Persistence   | Linger                                                | Max time to wait before flushing partial batch|
| Disk Persistence   | SegmentSize                                           | Max segment file size (default: 1MB)         |


## Concurrency and Thread Safety

Each system employs different concurrency patterns:

### Server System Concurrency

- **Worker Pool Pattern**: 1000 goroutines pre-spawned to handle connections
- **Channel-based Distribution**: `workerCh := make(chan net.Conn, maxWorkers)` distributes connections to workers
- **Per-Connection Goroutine**: Each `HandleConnection()` call runs in its own goroutine

### Topic Management Concurrency

- **TopicManager.mu**: `sync.RWMutex` protects the topics map during create/delete operations
- **Topic.mu**: `sync.RWMutex` protects partition counter and consumer groups
- **Partition.mu**: `sync.RWMutex` protects subscriber map and closed flag
- **dedupMap**: `sync.Map` for lock-free concurrent deduplication checks
- **Per-Partition Goroutine**: Each partition runs a goroutine executing `run()` to distribute messages

### Disk Persistence Concurrency

- **DiskHandler.mu**: `sync.Mutex` protects metadata (segment numbers, offsets)
- **DiskHandler.ioMu**: `sync.Mutex` serializes file I/O operations
- **Asynchronous Write Path**: Each DiskHandler runs `flushLoop()` in a dedicated goroutine
- **Channel-based Writes**: writeCh (unbuffered) passes messages from `AppendMessage()` to `flushLoop()`
- **Dual Lock Strategy***: Separate locks prevent deadlock between metadata updates and I/O operations

# System Lifecycle Management

## Startup

- DiskManager initialized first (creates handler registry)
- TopicManager initialized with DiskManager as HandlerProvider
- `TopicManager.cleanupLoop()` goroutine started for deduplication cleanup
- `RunServer()` starts TCP listener and worker pool

## Shutdown

- `TopicManager.Stop()` closes stopCh, stopping cleanup loop
- Each `DiskHandler.Close()` closes done channel, triggering drain logic in `flushLoop()`
- `flushLoop()` drains writeCh, flushes remaining batches, syncs and closes files

# Performance Characteristics

Each system has different performance characteristics based on its design:

| System             | Throughput Bottleneck                                     | Latency Characteristics                                    |
|-------------------|------------------------------------------------------------|------------------------------------------------------------|
| Server             | Worker pool size (1000) and connection accept rate       | Bounded by TCP round-trip time                             |
| Topic Management   | Partition channel buffer (10000) and distribution logic | O(1) partition selection, O(n) fan-out to subscribers     |
| Disk Persistence   | Disk I/O bandwidth and fsync latency                     | Batching amortizes fsync cost across up to 500 messages   |


# Error Handling and Resilience

Each system has specific error handling strategies:

## Server System

- **Connection Errors**: Logged and connection closed, does not crash broker
- **Read Deadline**: 5-minute timeout on idle connections
- **Write Errors**: Logged, connection closed, client must reconnect

## Topic Management

- **Missing Topic**: `GetTopic()` returns nil, caller checks before use
- **Duplicate Messages**: Silently dropped via `dedupMap.LoadOrStore()`
- **Closed Partition**: `Enqueue()` checks closed flag before enqueuing

## Disk Persistence

- **Write Errors**: Logged with `log.Printf("ERROR: ...")`, batch operation continues
- **Segment Rotation Errors**: Accumulated in error slice, returned to caller
- **Shutdown Drain**: Best-effort flush of pending messages before close


For detailed information about each individual system, see:

[Server System](server.md) - TCP server implementation details
[Topic Management System](./topic/topic-management.md) - Topics, partitions, and consumer groups
[Disk Persistence System](./storage/disk-persistence.md) - Write path, segment management, and optimizations
