# Message Flow

This document explains how messages flow through go-broker from publication to consumption. It covers the wire protocol, message routing, deduplication, partition selection, and the dual-path distribution mechanism that ensures both durability and low latency.

For detailed information about specific aspects of message flow:

- **Publishing mechanics and partition selection**: see [Publishing Messages](./message-flow.md)
- **Consumer registration and message streaming**: see [Consuming Messages](./message-flow.md)
- **Command protocol and processing**: see [Command Processing](../reference/api-reference.md)
- **Disk persistence implementation**: see [Disk Persistence System](./storage/disk-persistence.md)

## Wire Protocol

All communication between clients and the broker uses a TCP-based length-prefixed protocol. Each message on the wire consists of a 4-byte big-endian length prefix followed by the message payload.

The server reads messages using this protocol in HandleConnection:

- Read 4 bytes for message length
- Parse length as big-endian uint32
- Read exact message payload 
- Optionally decompress if gzip is enabled

Responses follow the same format using `writeResponse`, which writes a 4-byte length prefix followed by the response payload.

## Deduplication Mechanism

Go-broker implements automatic message deduplication to prevent duplicate processing. The TopicManager maintains a `dedupMap` (sync.Map) that tracks message IDs for 30 minutes.

The deduplication implementation is in `TopicManager.Publish`:

- Generate message ID from payload: `msg.ID = util.GenerateID(msg.Payload)`
- Attempt to store with timestamp: `dedupMap.LoadOrStore(msg.ID, now)`
    - If loaded=true, message already exists - return immediately
    - If loaded=false, continue with publication

The cleanup loop runs every 60 seconds (configurable via `CleanupInterval`) and removes entries older than 30 minutes.

## Partition Selection Strategy

Messages are routed to partitions using one of two strategies, determined by the presence of a message key.

The implementation is in `Topic.Publish()`:

### Key-based routing:

1. Uses `util.GenerateID(msg.Key)` to hash the key
2. Calculates `idx = keyID % partitionCount`
3. Guarantees messages with the same key go to the same partition
4. Maintains ordering for related messages

### Round-robin routing:

1. Uses `counter % partitionCount` where counter is atomically incremented
2. Distributes messages evenly across all partitions
3. Maximizes throughput and load balancing

Both strategies are protected by a mutex to ensure thread-safety during partition count access.

## Dual-Path Distribution

When a message is enqueued to a partition, it follows two parallel paths simultaneously: the in-memory path for low-latency delivery to active consumers, and the disk path for durability.

### In-Memory Path:
- Partition's run goroutine continuously reads from p.ch
- Distributes each message to all registered consumer group channels
- Consumer groups then forward to individual consumers based on partition ID modulo consumer count

### Disk Path:
- Messages are sent to unbuffered writeCh
- `flushLoop` goroutine batches messages (up to 500 or 100ms timeout)
- `writeBatch` writes length-prefixed messages to disk with `writer.Flush()` and `file.Sync()`
- Provides durability while minimizing I/O operations

## Consumption Flow

Message consumption follows a different path than publication, reading messages directly from disk rather than from in-memory channels.

The consumption flow is handled by `HandleConsumeCommand()`:

- **Parse command**: Extract topic, partition, and offset
- **Get handler**: Retrieve DiskHandler for topic-partition
- **Read messages**: Call `ReadMessages(offset, 8192)` to read up to 8192 bytes
- **Stream to client**: Write each message using length-prefixed protocol

The server detects the CONSUME command and returns `STREAM_DATA_SIGNAL`, which triggers streaming mode.

## Batching and Flushing

The disk persistence layer uses batching to optimize I/O operations. The `flushLoop` goroutine manages batch accumulation and flushing based on size and time thresholds.

The batching logic in `flushLoop`:

- **Message accumulation**: Messages from writeCh are added to a batch slice 
- **Size trigger**: Flush when batch reaches 500 messages
- **Time trigger**: Flush after 100ms timeout if batch is non-empty
- **Graceful shutdown**: Drain remaining messages on done signal

The `writeBatch` function:

- Acquires both mutexes for thread-safety 
- Opens segment if needed
- Writes each message with 4-byte length prefix
- Rotates segment if 1MB limit is reached
- Flushes buffered writer and syncs to disk

---

# Publishing Messages

## Purpose and Scope

This document describes the complete path a message takes from publication to persistence in go-broker. It covers the TCP ingestion protocol, deduplication logic, partition selection strategies, asynchronous disk writes, and in-memory distribution to active consumers.

For information about consuming messages from disk, see [Consuming Messages](./message-flow.md). For details on the disk segment format and read operations, see [Segment Management](./storage/segment-management.md).

## Message Ingestion Protocol

### TCP Connection Handling

The broker accepts TCP connections on port 9000 (configurable via `BrokerPort`) and processes them using a bounded worker pool pattern with 1000 workers defined by the maxWorkers constant.

Each incoming connection is handled by `HandleConnection`, which processes messages in a loop until the connection closes or an error occurs. The function uses a 5-minute read deadline to prevent indefinite blocking on idle connections.

### Length-Prefixed Message Framing

All messages use a length-prefixed binary protocol:

| Field   | Size    | Description                                        |
|---------|---------|---------------------------------------------------|
| Length  | 4 bytes | Big-endian uint32 indicating message payload size |
| Payload | N bytes | Message data (optionally gzip-compressed)         |

The server reads messages in two stages:

- Read 4-byte length prefix using `io.ReadFull`
- Read exact payload bytes based on length

### Message Routing Logic

After decoding, the server determines whether the payload is a command or a message to publish:
- **Command**: Starts with keywords `CREATE`, `DELETE`, `LIST`, `SUBSCRIBE`, `PUBLISH`, or `CONSUME`
- **Direct message**: Any other payload is treated as a message to publish to the topic

For direct messages, the server calls `tm.Publish(topicName, msg)` and immediately responds with "OK". For commands, it delegates to `CommandHandler.HandleCommand`.

# Deduplication Mechanism

## Purpose and Implementation

The TopicManager implements message deduplication using a `sync.Map` that tracks message IDs for 30 minutes. This prevents duplicate processing when clients retry failed operations or experience network issues.

Each message is assigned an ID by hashing its payload using the FNV-1a 64-bit hash function implemented in `util.GenerateID`.

## Cleanup Process

A background goroutine runs periodically to remove expired deduplication entries:

- **Cleanup interval**: 60 seconds (configurable via `CleanupInterval`)
- **Expiry window**: 30 minutes
- **Implementation**: cleanupLoop goroutine iterates through `dedupMap` and deletes entries older than 30 minutes

The cleanup increments the `metrics.CleanupCount` counter for observability.

## Partition Selection Strategies

The `Topic.Publish` method selects a target partition using one of two strategies based on whether the message has a key.

### Key-Based Routing

When a message has a non-empty Key field, the broker uses consistent hashing to ensure all messages with the same key go to the same partition:

```
partitionIndex = hash(key) % partitionCount
```

The hash function (`util.GenerateID`) uses FNV-1a 64-bit hashing, ensuring deterministic and uniformly distributed partition assignment.

### Round-Robin Distribution

For messages without a key, the topic uses a simple round-robin counter to distribute load evenly across all partitions:

```
partitionIndex = counter % partitionCount
counter++
```

The counter is a uint64 field on the Topic struct, protected by the topic's mutex during selection.

## Disk Persistence

### Asynchronous Write Path

Messages are persisted to disk asynchronously to avoid blocking publishers. The DiskHandler implements a batching write strategy:

- Messages are sent to an unbuffered `writeCh` channel
- A dedicated flushLoop goroutine consumes from this channel
- Messages are batched up to `batchSize` (default 500) or flushed after `linger time` (default 100ms)
- Batches are written to disk in a single `fsync` operation

### Batch Accumulation Logic

The `flushLoop` uses a ticker and channel select to implement the batching logic:

| Trigger                     | Action                                    |
|------------------------------|------------------------------------------|
| Message arrives via writeCh   | Add to batch; flush if len(batch) >= batchSize |
| Ticker fires (every linger ms)| Flush batch if non-empty                  |
| Done signal received          | Drain remaining messages and flush       |

### Write Batch Implementation

The `writeBatch` method writes all messages in a batch atomically:

- **Acquire locks**: First mu (metadata lock), then ioMu (I/O lock)
- **Check segment size**: Rotate to new segment if current would exceed 1MB
- **Write each message**: 4-byte length prefix + message data
- **Flush and sync**: Call `writer.Flush()` and `file.Sync()` to ensure durability

The dual-mutex strategy allows metadata reads to proceed while I/O operations are in progress.

## Segment Rotation

When writing a message would cause the current segment to exceed `SegmentSize` (1MB), the handler rotates to a new segment:

- Flush and close current file
- Increment `CurrentSegment` counter
- Reset CurrentOffset to 00
- Open new segment file with incremented number
- Segment files are named: `{topic}_{partition}_{segmentNumber}.log`

## Partition Channel Architecture

Each partition maintains a buffered channel with capacity 10,000 (configurable via `PartitionChannelBufSize`). 

The `Partition.Enqueue` method sends messages to this channel immediately after queuing them for disk persistence.

A dedicated goroutine (`Partition.run`) consumes from this channel and distributes messages to all registered consumer group subscriptions.

### Partition-to-Consumer Assignment

When a consumer group is registered, partitions are assigned to consumers using `modulo` arithmetic:

```
consumerIndex = partitionID % consumerCount
```

This ensures each partition's messages go to exactly one consumer within the group, maintaining ordering guarantees while load-balancing across consumers.

A goroutine bridges each partition's group subscription channel to the assigned consumer's MsgCh

## Performance Characteristics

### Tunable Parameters

The following configuration parameters affect publishing performance:

| Parameter          | Config Field              | Default | Description                          |
|-------------------|---------------------------|---------|--------------------------------------|
| Disk batch size    | DiskFlushBatchSize        | 500     | Messages per disk flush               |
| Linger time        | LingerMS                  | 100ms   | Max wait before flush                 |
| Partition buffer   | PartitionChannelBufSize   | 10000   | Partition channel capacity            |
| Consumer buffer    | ConsumerChannelBufSize    | 1000    | Consumer channel capacity             |

### Latency vs Throughput Trade-offs

The default values balance these concerns for typical workloads. Adjust based on your specific requirements.

- **Lower batch size / linger time**: Reduces latency but increases syscall overhead
- **Higher batch size / linger time**: Increases throughput but adds latency
- **Larger buffers**: Prevents backpressure but increases memory usage

### Metrics

The broker exposes Prometheus metrics for publish operations:

- **messages_processed_total**: Counter of successfully published messages
- **message_latency_seconds**: Histogram of publish operation duration

These metrics are incremented in `TopicManager.Publish()` after successfully enqueueing the message.

# Consuming Messages

## Purpose and Scope

This page explains how consumers retrieve messages from go-broker, covering both in-memory consumption via consumer groups and on-demand consumption from disk. 

For information about how messages are published and stored to disk, see [Publishing Messages](./message-flow.md). For details about consumer group architecture and load balancing, see [Consumer Groups](./topic/consumer-groups.md).

There are two primary consumption patterns in go-broker:
- **In-memory consumption via consumer groups**: Active consumers receive messages in real-time through channels as they are published
- **On-demand consumption via `CONSUME` command**: Clients read historical messages directly from disk at a specific offset

This page focuses primarily on the on-demand consumption mechanism via the `CONSUME` command, which allows clients to read persisted messages from disk.

## Consumption Architecture

The consumption system consists of several layers working together to retrieve messages from disk and stream them to clients over TCP.

The consumption flow is synchronous and blocking: 
- when a client issues a `CONSUME` command, the server immediately begins reading messages from disk and streaming them back over the same TCP connection.

## The CONSUME Command Protocol

### Command Syntax

The `CONSUME` command follows a specific syntax:

**Syntax:**  `CONSUME <topic> <partition> <offset>`

**Parameters:**

- `topic`: The name of the topic to consume from  
- `partition`: The partition ID (integer, 0-indexed)  
- `offset`: The number of messages to skip before starting consumption  

**Example:**  `CONSUME orders 0 100`  

Reads from the "orders" topic, partition 0, skipping the first 100 messages.

### Command Detection and Routing

When the server receives a command, it distinguishes between regular commands and streaming commands:

The key distinction is the `STREAM_DATA_SIGNAL` constant which signals to `HandleConnection` that it should invoke `HandleConsumeCommand` to stream data directly over the connection.

### Command Parsing

The `HandleConsumeCommand` function parses the command arguments:

| Argument Position | Name       | Type    | Validation              |
|------------------|-----------|--------|------------------------|
| 0                | Command   | String | Must be "CONSUME"       |
| 1                | Topic     | String | Must exist              |
| 2                | Partition | Integer| Must be valid integer   |
| 3                | Offset    | Integer| Must be valid integer   |

If parsing fails, an error is returned immediately and the connection is terminated with an error message.

## Reading Messages from Disk

### DiskHandler Retrieval

The `CommandHandler` retrieves the appropriate DiskHandler for the requested topic-partition pair:

```
dh, err := ch.DiskManager.GetHandler(topicName, partition)
```

Each topic-partition pair has a dedicated DiskHandler instance that manages its segment files. The DiskManager acts as a registry, creating handlers on-demand if they don't exist.

### Message Format on Disk

Messages are stored in a length-prefixed format:

```
[4-byte length][message data][4-byte length][message data]...
```

- **Length prefix**: 4 bytes, big-endian unsigned integer representing message length
- **Message data**: Variable-length payload bytes

The ReadMessages method reads this format by:
- Reading the 4-byte length prefix 
- Parsing it as a big-endian uint32 
- Reading that many bytes for the message payload 
- Repeating until max messages are read or end-of-file is reached


## Memory-Mapped I/O

The system uses memory-mapped file I/O via the `golang.org/x/exp/mmap` package:

```
reader, err := mmap.Open(filePath)
if err != nil {
    return nil, fmt.Errorf("mmap open failed: %w", err)
}
defer reader.Close()
```

Memory mapping allows the operating system to manage file I/O efficiently by mapping file contents directly into the process's address space. This provides:
- **Efficient random access**: No need for explicit seek operations
- **OS-level caching**: The kernel can cache pages automatically
- **Low overhead**: Minimal copying between kernel and user space

The ReadAt method performs reads at specific offsets without maintaining cursor state.

## Offset Handling

The offset parameter specifies how many messages to skip before returning data:

```
if offset > 0 {
    offset--
    continue
}
messages = append(messages, msg)
```

This allows clients to implement pagination or resume consumption from a specific point. The offset is message-based, **not byte-based**.

## Batch Size Limit

Each ReadMessages call reads up to 8192 messages:

```
messages, err := dh.ReadMessages(offset, 8192)
```

This limit prevents unbounded memory usage when reading large segments. Clients that need more messages must issue multiple `CONSUME` commands with adjusted offsets.

## Message Streaming Protocol

### Length-Prefixed Framing

After reading messages from disk,   `HandleConsumeCommand` streams them back to the client using length-prefixed framing:

```
for _, msg := range messages {
    msgBytes := []byte(msg.Payload)
    if err := util.WriteWithLength(conn, msgBytes); err != nil {
        return streamedCount, fmt.Errorf("failed to stream message: %w", err)
    }
    streamedCount++
}
```

The `util.WriteWithLength` helper writes a 4-byte big-endian length prefix followed by the message data, matching the TCP protocol used throughout go-broker.

### Connection Lifecycle

The `CONSUME` command is terminal for the TCP connection:

After `HandleConsumeCommand` completes (successfully or with error), the `HandleConnection` function returns which triggers the deferred `conn.Close()` 

This design ensures that:
- Each `CONSUME` command gets a fresh connection
- No state is maintained between consume operations
- Resource cleanup is automatic

### Error Handling

If an error occurs during streaming, the server:
1. Logs the error 
2. Sends an error response to the client 
3. Closes the connection 

## Consumer Groups vs CONSUME Command

It's important to distinguish between two consumption mechanisms:

### Consumer Groups (In-Memory)

Consumer groups are registered via the `SUBSCRIBE` command and receive messages in real-time:

This mechanism:
- Provides low latency (in-memory channels)
- Requires the consumer to be connected when messages are published
- Uses load balancing: each partition's messages go to one consumer in the group

### CONSUME Command (Disk-Based)

The `CONSUME` command reads historical messages from disk:

This mechanism:

- Reads from persisted segment files
- Allows replay of historical messages
- Does not require prior subscription
- Supports offset-based positioning

The two mechanisms are complementary:

- Use consumer groups for real-time event processing
- Use `CONSUME` for batch processing, replay, or catching up after downtime

## Platform-Specific Optimizations

### Linux: sendfile() System Call

On Linux, the DiskHandler provides an optimized method for sending entire segments over TCP using the `sendfile()` system call:

```
func (d *DiskHandler) SendCurrentSegmentToConn(conn net.Conn) error
```

This method bypasses normal read/write operations by transferring data directly from the file descriptor to the socket in kernel space:

The implementation:
- Flushes any pending writes to ensure data consistency 
- Obtains the raw socket file descriptor 
- Calls `unix.Sendfile` in a loop until all data is transferred 

This optimization provides:
- **Zero-copy**: Data is not copied to user space
- **CPU efficiency**: Minimal CPU usage for data transfer
- **Performance**: Significantly faster for large segments

Note: This method is currently not used by the standard `CONSUME` command flow, which uses `ReadMessages` with mmap. It is available as an alternative for future optimizations.

### Linux: Sequential Read-Ahead Hints

When opening segment files for writing, Linux builds apply the `FADVISE_SEQUENTIAL` hint:

```
_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
```

This hints to the kernel that the file will be read sequentially, allowing it to:

- Increase the read-ahead window
- Optimize page cache behavior
- Prefetch data before it's requested

### Configuration Parameters

Several configuration parameters affect consumption behavior:

| Parameter             | Config Field                  | Default | Description                                         |
|-----------------------|-------------------------------|---------|-----------------------------------------------------|
| Read deadline         | readDeadline constant         | 5 minutes | Maximum idle time before connection timeout        |
| Max messages per read | Hard-coded in HandleConsumeCommand | 8192    | Maximum messages returned per CONSUME call         |
| Segment size          | DiskHandler.SegmentSize       | 1 MB    | Size of individual segment files                   |

The read deadline applies to all TCP operations ensuring that inactive connections are eventually closed and resources are freed.

## Error Scenarios and Recovery

### Topic or Partition Not Found

If the requested topic or partition doesn't exist, `DiskManager.GetHandler` returns an error:

```
dh, err := ch.DiskManager.GetHandler(topicName, partition)
if err != nil {
    return 0, fmt.Errorf("failed to get disk handler: %w", err)
}
```

The error is returned to the client as a formatted error message, and the connection is closed.

### Segment File Not Found

If the segment file cannot be opened for memory mapping:

```
reader, err := mmap.Open(filePath)
if err != nil {
    return nil, fmt.Errorf("mmap open failed: %w", err)
}
```

This typically occurs when:

- The segment file was deleted
- File permissions are incorrect
- The disk is unavailable

### Read Errors During Streaming

If a write fails while streaming messages to the client:

```
if err := util.WriteWithLength(conn, msgBytes); err != nil {
    return streamedCount, fmt.Errorf("failed to stream message: %w", err)
}
```

This can happen if:

- The client disconnects
- Network errors occur
- The socket buffer is full

The error is logged, and the partial count of successfully streamed messages is returned.

## Summary

The consumption system in go-broker provides:

- **On-demand disk reads**: Historical messages can be retrieved at any time using the CONSUME command
- **Memory-mapped I/O**: Efficient file access using OS-level memory mapping
- **Length-prefixed streaming**: Standard TCP framing for message boundaries
- **Offset-based positioning**: Clients can skip messages or resume from specific points
- **Platform optimizations**: Linux-specific optimizations for sequential reads and zero-copy transfers
- **Batch limiting**: Maximum of 8192 messages per request to prevent unbounded memory usage

The key classes involved are:

- **CommandHandler** - Parses and routes the CONSUME command
- **DiskHandler** - Manages segment files and provides ReadMessages
- **DiskManager** - Acts as a registry for DiskHandler instances per topic-partition

For information about how these messages were originally published and written to disk, see [Publishing Messages](./message-flow.md). For details about real-time consumption via consumer groups, see [Consumer Groups](./topic/consumer-groups.md).

