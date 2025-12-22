# Topic Management

## Purpose and Scope

The Topic Management System is the core routing and distribution layer of cursus. It orchestrates how messages flow from publishers to consumers through topics, partitions, and consumer groups. 

This system handles partition selection, message deduplication, consumer group registration, and in-memory message distribution to active consumers.

For detailed information about partition selection strategies and ordering guarantees, see [Topics and Partitions](topics-and-partitions.md). For consumer group mechanics and load balancing, see [Consumer Groups](./consumer-groups.md). For disk persistence of messages, see [Disk Persistence System](../storage/disk-persistence.md).

## Core Components

### TopicManager

The TopicManager serves as the central registry and coordinator for all topics in the system. It maintains the global deduplication map and periodically cleans expired entries.

| Field   | Type           | Purpose                                                                 |
|---------|----------------|-------------------------------------------------------------------------|
| topics  | map[string]*Topic | Registry of all topics by name                                         |
| dedupMap| sync.Map       | Global message deduplication (30-minute window)                         |
| cleanupInt | time.Duration | Interval for cleanup loop (default: 60s)                                |
| stopCh  | chan struct{}  | Signal channel to stop cleanup goroutine                                 |
| hp      | HandlerProvider| Provides disk handlers for partitions                                    |
| mu      | sync.RWMutex   | Protects topics map                                                      |

The TopicManager is initialized with a HandlerProvider interface that supplies DiskHandler instances for each topic-partition combination. 

Key operations:

- **CreateTopic**: Creates new topics or adds partitions to existing topics 
- **Publish**: Performs deduplication check before routing to topic 
- **RegisterConsumerGroup**: Delegates to topic for consumer registration 
- **CleanupDedup**: Removes entries older than 30 minutes from dedupMap 

### Topic

A Topic represents a logical message stream divided into multiple partitions. It handles partition selection during message publication and maintains consumer group registrations.

| Field          | Type                   | Purpose                                        |
|----------------|-----------------------|------------------------------------------------|
| Name           | string                | Topic identifier                               |
| Partitions     | []*Partition          | Array of partition instances                   |
| counter        | uint64                | Round-robin counter for partition selection   |
| consumerGroups | map[string]*ConsumerGroup | Registered consumer groups by name          |
| mu             | sync.RWMutex          | Protects counter and consumer groups          |


The `Topic.Publish()` method implements two partition selection strategies 

- **Key-based**: If `msg.Key != ""`, uses `util.GenerateID(msg.Key)`
- **Round-robin**: If no key, uses `counter % partitionCount` and increments counter

### Partition

Each Partition represents an independent message queue within a topic. It runs a dedicated goroutine that distributes incoming messages to registered consumer groups.

| Field  | Type                     | Purpose                                 |
|--------|--------------------------|-----------------------------------------|
| id     | int                      | Partition identifier (0-based index)    |
| topic  | string                   | Parent topic name                        |
| ch     | chan types.Message       | Message queue (capacity: 10,000)       |
| subs   | map[string]chan types.Message | Per-group distribution channels     |
| mu     | sync.RWMutex             | Protects subs map                        |
| dh     | interface{}              | DiskHandler for persistence              |
| closed | bool                     | Partition state flag                     |


The partition's `run()` goroutine continuously reads from ch and distributes to all registered group channels 

```
for msg := range p.ch {
    for _, subCh := range p.subs {
        subCh <- msg  // Fan-out to all consumer groups
    }
}
```

### ConsumerGroup and Consumer

A ConsumerGroup contains multiple Consumer instances that collectively consume messages from a topic. Each partition is assigned to exactly one consumer within a group using modulo arithmetic.


**ConsumerGroup Structure:**

| Field      | Type                       | Purpose                               |
|------------|----------------------------|---------------------------------------|
| Name       | string                     | Group identifier                       |
| Consumers  | []*Consumer                | Slice of Consumer pointers             |

**Consumer Structure:**

| Field  | Type               | Purpose                                  |
|--------|------------------|------------------------------------------|
| ID     | int               | Consumer index within the group (0-based)|
| MsgCh  | chan types.Message | Buffered channel (capacity: 1,000) for receiving messages |


Partition-to-consumer assignment uses: `consumerID = partitionID % consumerCount` 

## Deduplication Mechanism

The TopicManager implements a global deduplication system using a sync.Map to track message IDs. This prevents duplicate processing when messages are retransmitted due to network issues or client retries.

### Deduplication Flow

- **ID Generation**: Each message receives a 64-bit FNV-1a hash of its payload via `util.GenerateID()`
- **Atomic Check**: `dedupMap.LoadOrStore(msg.ID, time.Now())` atomically checks and stores 
- **Drop if Duplicate**: If loaded=true, the message is silently dropped 
- **Periodic Cleanup**: Background goroutine removes entries older than 30 minutes 

## Cleanup Configuration


The cleanup loop runs at an interval specified by `config.CleanupInterval` (default: 60 seconds) 

```
expireBefore := time.Now().Add(-30 * time.Minute)
dedupMap.Range(func(key, value any) bool {
    if ts, ok := value.(time.Time); ok && ts.Before(expireBefore) {
        dedupMap.Delete(key)
        metrics.CleanupCount.Inc()
    }
    return true
})
```

## Topic Lifecycle Management

### Topic Creation

Topics are created via `TopicManager.CreateTopic(name, partitionCount)` 

The method handles three scenarios:

| Scenario                    | Action        | Result                                           |
|-----------------------------|---------------|-------------------------------------------------|
| New topic                   | Create with N partitions | New Topic with partition array              |
| Existing topic, same count  | No-op          | Return existing topic                           |
| Existing topic, increase count | Add partitions | Expanded partition array                       |
| Existing topic, decrease count | Reject       | Warning printed, no change                      |


### Topic Deletion

The DeleteTopic(name) method removes a topic from the registry 

Note that this only removes the topic from memory; it does not clean up disk files or close partition goroutines.

## Consumer Group Registration

Consumer groups are registered per-topic using `Topic.RegisterConsumerGroup(groupName, consumerCount)` 

The registration process:

1. **Group Creation**: Allocate ConsumerGroup with N Consumer instances
2. **Channel Allocation**: Each consumer gets a buffered channel (capacity: 1,000)
3. **Partition Binding**: For each partition, create a group-specific channel
4. **Distribution Goroutines**: Start goroutines to forward messages to assigned consumers
5. **Partition-to-Consumer Mapping**

The system uses deterministic modulo arithmetic to assign partitions to consumers:

```
for pid, p := range t.Partitions {
    groupCh := p.RegisterGroup(groupName)
    target := pid % consumerCount  // Deterministic assignment
    go func(ch <-chan types.Message, consumer *Consumer) {
        for msg := range ch {
            consumer.MsgCh <- msg
        }
    }(groupCh, group.Consumers[target])
}
```

This ensures:

- Each partition sends to exactly one consumer per group
- Load is balanced when partitionCount >= consumerCount
- Assignment is stable unless consumer count changes

## Channel Capacities

The system uses buffered channels extensively to prevent blocking during message distribution. Channel capacities are hard-coded throughout the topic management layer:

| Channel                     | Capacity | Purpose                           |
|-----------------------------|----------|-----------------------------------|
| Partition channel           | 10,000   | Partition message queue           |
| Group distribution channel  | 10,000   | Per-group message buffer in partition |
| Consumer message channel    | 1,000    | Consumer's message receive buffer |

## Individual consumer receive buffer

These capacities provide backpressure tolerance but can fill up if consumers fall behind. Once full, the sending goroutine will block until space is available.


## Integration with Other Systems

The Topic Management System integrates with:

- Disk Persistence: Each partition holds a DiskHandler interface and calls AppendMessage() on enqueue 
- Metrics: TopicManager records metrics via metrics.MessagesProcessed and metrics.LatencyHist 
- Server Layer: Command handlers call TopicManager methods for PUBLISH, SUBSCRIBE, and CONSUME operations (see [Server System](../server.md))
- Configuration: TopicManager reads config.CleanupInterval for deduplication cleanup 

## Thread Safety

All components in the topic management system are designed for concurrent access:

- **TopicManager**: Uses sync.RWMutex to protect the topics map and sync.Map for lock-free deduplication
- **Topic**: Uses sync.RWMutex to protect the counter and consumer groups map
- **Partition**: Uses sync.RWMutex to protect the subs map and closed flag

The `run()` goroutine in each partition holds a read lock only while iterating over subscriber channels, allowing concurrent message enqueuing.
