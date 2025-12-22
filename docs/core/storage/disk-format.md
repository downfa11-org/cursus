# Disk-format

This document describes the on-disk storage format used by cursus to persist messages. It covers the segment file structure, message encoding scheme, write and read mechanics, and platform-specific optimizations.

For details on the asynchronous write path and batching behavior, see [DiskHandler and Write Path](./disk-persistence.md).

## Purpose and Scope

cursus stores messages in append-only log files organized into segments. Each topic-partition pair maintains its own set of segment files, allowing for parallel I/O operations and independent management. This document specifies:

- Segment file naming conventions and directory layout
- Message encoding format (length-prefixed binary)
- Write mechanics (batching, buffering, sync policy)
- Read mechanics (memory-mapped I/O, offset resolution)
- Segment rotation and management
- Directory Structure and File Naming

## Directory Layout

Messages are organized in a hierarchical directory structure based on topic and partition:

```
{LogDir}/
  {topicName}/
    partition_{partitionID}_segment_0.log
    partition_{partitionID}_segment_1.log
    partition_{partitionID}_segment_N.log
```

Example:

```
./logs/
  orders/
    partition_0_segment_0.log
    partition_0_segment_1.log
    partition_0_segment_2.log
  users/
    partition_0_segment_0.log
    partition_1_segment_0.log
```

## File Naming Convention

| Component     | Format                                           | Example                     |
|---------------|--------------------------------------------------|------------------------------|
| Base path     | `{LogDir}/{topicName}/partition_{partitionID}`     | `./logs/orders/partition_0`    |
| Segment file  | `{base}_segment_{segmentNumber}.log`               | `partition_0_segment_0.log`    |



The base path is constructed using platform-specific path separators (`os.PathSeparator`) to ensure cross-platform compatibility.

# Segment Management

## Segment Size and Rotation

Each segment file has a maximum size of 1 MB (1,048,576 bytes). 

When the current segment would exceed this limit after writing a message, the DiskHandler automatically rotates to a new segment.

## Segment Metadata

The DiskHandler tracks segment state using the following fields:

| Field           | Type   | Purpose                                         |
|-----------------|--------|-------------------------------------------------|
| CurrentSegment  | int    | Active segment number (0, 1, 2, ...)           |
| CurrentOffset   | int    | Write position within current segment (bytes)   |
| SegmentSize     | int    | Maximum segment size (1 MB)                     |
| BaseName        | string | Base path for segment files                     |


# Message Encoding Format

## Length-Prefixed Binary Format

Messages are stored using a simple length-prefixed binary format. Each message consists of:

## 4-byte length prefix (big-endian uint32)

### Message payload (variable length bytes)

```
┌─────────────┬──────────────────┬─────────────┬──────────────────┐
│ Length (4B) │ Message Data (N) │ Length (4B) │ Message Data (M) │ ...
└─────────────┴──────────────────┴─────────────┴──────────────────┘
```

Example:

```
Message 1: "Hello"
  [0x00, 0x00, 0x00, 0x05] [0x48, 0x65, 0x6C, 0x6C, 0x6F]
   ^--- Length = 5          ^--- "Hello" in ASCII
```

```
Message 2: "World!"
  [0x00, 0x00, 0x00, 0x06] [0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21]
   ^--- Length = 6          ^--- "World!" in ASCII
```

## Encoding Implementation

The encoding process follows these steps:

1. Convert message string to []byte
2. Calculate message length
3. Encode length as 4-byte big-endian uint32 using `binary.BigEndian.PutUint32()`
4. Write length prefix to `bufio.Writer`
5. Write message data to `bufio.Writer`
6. Update CurrentOffset by (4 + messageLength)

### Byte Order

The length prefix uses big-endian (network byte order) encoding. 

This ensures consistent interpretation across different hardware architectures and allows for straightforward binary inspection.

### Batching and Buffering

The write path employs a multi-level buffering strategy to optimize disk I/O:

## Write Mechanics

| Stage    | Component           | Configuration            | Behavior                                 |
|----------|----------------------|---------------------------|-------------------------------------------|
| Enqueue  | writeCh channel      | ChannelBufferSize         | Non-blocking with timeout retry           |
| Batch    | flushLoop goroutine  | DiskFlushBatchSize (500)  | Accumulates messages up to batch size     |
| Linger   | `time.Ticker`          | LingerMS (100ms)          | Flushes partial batches after timeout     |
| Buffer   | `bufio.Writer`         | Default (4KB)             | In-memory buffering before syscall        |
| Sync     | `file.Sync()`          | After each batch          | Forces kernel flush to disk               |


## Flush Triggers

The flushLoop goroutine flushes accumulated messages under three conditions:

- **Batch size reached**: When len(batch) >= batchSize
- **Linger timeout**: Every linger duration (100ms default)
- **Shutdown signal**: On done channel close, draining remaining messages

# Durability Guarantees

After writing each batch, the system ensures durability through:

- `bufio.Writer.Flush()` - Writes buffered data to file descriptor
- `file.Sync()` - Invokes `fsync()` syscall to flush kernel page cache

This guarantees that messages are physically written to disk before acknowledging completion.

## Memory-Mapped I/O

cursus uses memory-mapped I/O for reading messages, leveraging the `golang.org/x/exp/mmap` package. 

This approach provides efficient random access to segment files without requiring explicit read syscalls for each message.

## Offset Resolution

The ReadMessages function implements logical offset handling:

1. Start reading from the beginning of the segment file (`pos = 0`)
2. Parse each message sequentially
3. If `offset > 0`, skip the message and decrement offset
4. Once `offset == 0`, begin collecting messages
5. Continue until max messages collected or end of file

This approach trades some read efficiency for simplicity, as it doesn't maintain an index structure. For workloads with small offsets relative to segment size, the cost is minimal due to memory-mapped I/O.

# Concurrency Control

## Dual Mutex Strategy

The DiskHandler uses two mutexes to allow concurrent reads while protecting critical sections:

| Mutex | Protects                     | Acquired By         | Purpose            |
|-------|-------------------------------|----------------------|--------------------|
| mu    | CurrentSegment, CurrentOffset | Read/Write operations| Metadata consistency |
| ioMu  | file, writer                  | Write operations only| I/O serialization    |

## Lock Ordering

Write operations acquire locks in this order to prevent deadlocks:

- mu (metadata lock)
- ioMu (I/O lock)

Read operations only acquire mu briefly to read CurrentSegment, then release it before opening the mmap reader.

# Platform-Specific Implementations

## Linux Optimizations

On Linux builds, the segment opening includes platform-specific optimizations:

From `flush_linux.go` (not shown in provided files)
- Uses `O_DIRECT` or `O_SYNC` flags for write-through
- Applies fadvise(SEQUENTIAL) for read-ahead hints
- Can leverage `sendfile(2)` for zero-copy transfers

## Windows Implementation

The Windows implementation (`flush_window.go`) uses standard file operations without advanced optimizations:

- Standard flags (`os.O_CREATE`, `os.O_RDWR`, `os.O_APPEND`)
- No `sendfile()` equivalent (uses `io.Copy()` instead)
- No `fadvise()` hints available

# Summary Table

| Aspect             | Specification                                   |
|--------------------|--------------------------------------------------|
| Segment size       | 1 MB (1,048,576 bytes)                           |
| File naming        | `{topic}/partition_{id}_segment_{n}.log`           |
| Message format     | 4-byte big-endian length + payload               |
| Write buffering    | `bufio.Writer` + batch accumulation                |
| Batch size         | Configurable (default: 500 messages)             |
| Linger time        | Configurable (default: 100ms)                    |
| Read mechanism     | Memory-mapped I/O (mmap)                         |
| Durability         | `fsync()` after each batch                         |
| Concurrency        | Dual mutex (mu + ioMu)                           |
| Segment rotation   | Automatic at 1 MB boundary                       |