# Platform-Specific Optimizations

## Purpose and Scope

This document describes the platform-specific optimizations implemented in cursus's disk persistence layer. The system uses Go's build tag mechanism to provide optimized implementations for Linux systems while maintaining compatibility with Windows through standard I/O operations.

For general information about the disk persistence system architecture, see [Disk Persistence System](./disk-persistence.md). For segment rotation and management, see [Segment Management](./segment-management.md).

## Build Tag System

cursus uses build tags (`//go:build`) to compile different implementations based on the target operating system. This allows the system to leverage platform-specific system calls on Linux while providing a fallback implementation for Windows.

The DiskHandler struct is defined in common code, but the methods `openSegment()` and `SendCurrentSegmentToConn()` have platform-specific implementations that are selected at compile time.

## Linux Optimizations

### Sequential Access Advisory: fadvise

When opening a segment file on Linux, cursus provides a hint to the kernel about the expected access pattern using unix.`Fadvise()` with the `FADV_SEQUENTIAL` flag.

The fadvise call provides performance benefits:

| Optimization                  | Description                                   | Benefit                                              |
|-------------------------------|-----------------------------------------------|------------------------------------------------------|
| Aggressive Read-Ahead         | Kernel pre-fetches more data from disk        | Reduces read latency for sequential scans            |
| Reduced Page Cache Priority   | Pages are evicted sooner after use            | Frees memory for more valuable cached data           |
| I/O Scheduling Hint           | Kernel can optimize disk I/O scheduling       | Better disk throughput for sequential workloads      |

The signature used is:

```
unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
```

Where:

- fd: File descriptor from `f.Fd()`
- offset: 0 (start of file)
- length: 0 (entire file)
- advice: `unix.FADV_SEQUENTIAL` (sequential access pattern)

### Zero-Copy Transfer: sendfile

The most significant optimization on Linux is the use of the `sendfile()` system call to transfer segment data directly from the file descriptor to a network socket without copying data through userspace.

### Step 1: Connection Type Check

The code first checks if the connection is a `*net.TCPConn`. If not, it falls back to `io.Copy`:

```
sysConn, ok := conn.(*net.TCPConn)
if !ok {
    if _, err := d.file.Seek(0, 0); err != nil {
        return err
    }
    _, err := io.Copy(conn, d.file)
    return err
}
```

### Step 2: Access Raw Socket

For TCP connections, the code obtains the underlying system socket:

```
rawConn, err := sysConn.SyscallConn()
```

### Step 3: sendfile System Call

Using `rawConn.Control()`, the code executes the sendfile system call with direct access to the file descriptor:

The sendfile loop implementation:

| Variable | Type    | Purpose                                              |
|----------|---------|-------------------------------------------------------|
| inFd     | int     | File descriptor of source segment file               |
| outFd    | int     | File descriptor of destination TCP socket            |
| offset   | *int64  | Current position in source file (updated by kernel)  |
| size     | int64   | Total bytes to transfer                              |
| n        | int     | Bytes transferred in this call                       |

The loop continues until `offset >= size` or an error occurs. The offset pointer is automatically updated by the kernel after each successful transfer.

# Key Differences from Linux

| Aspect            | Linux Implementation                     | Windows Implementation                |
|-------------------|-------------------------------------------|----------------------------------------|
| System Call       | `unix.Sendfile()`                           | Standard `io.Copy()`                     |
| Data Path         | Kernel-to-kernel transfer                 | Kernel → User → Kernel copies          |
| Buffer Usage      | No userspace buffering needed             | Data copied through userspace buffer   |
| CPU Overhead      | Minimal (DMA transfer)                    | Higher (multiple memory copies)        |
| Advisory Hints    | fadvise(SEQUENTIAL)                       | None available                         |
| Code Complexity   | More complex (raw socket handling)        | Simpler (standard library)             |


The Windows implementation:

```
if _, err := d.file.Seek(0, 0); err != nil {
    return err
}
_, err := io.Copy(conn, d.file)
return err
```

This performs a standard buffered copy operation that reads data from the file into a userspace buffer and then writes it to the network socket.

# When Optimizations Matter

The platform-specific optimizations are most beneficial in these scenarios:

| Scenario                     | Impact | Reason                                                   |
|------------------------------|--------|----------------------------------------------------------|
| Large segment transfers      | High   | sendfile eliminates multiple GB copies                  |
| High consumer count          | High   | Each consumer streams segments; CPU savings multiply    |
| Memory-constrained systems   | Medium | Reduced userspace buffer requirements                   |
| Network-saturated workloads  | Medium | Lower CPU overhead allows higher network utilization    |
| Small message workloads      | Low    | System call overhead may dominate                       |


For most use cases, the batching and asynchronous write path (covered in DiskHandler and Write Path) has a larger performance impact than the platform-specific optimizations. 

However, for high-throughput segment streaming to consumers, the Linux optimizations provide measurable benefits.


# Usage in DiskManager

The DiskManager orchestrates handler creation but does not directly interact with platform-specific code. It simply calls methods on DiskHandler that resolve to platform-specific implementations at compile time.

The GetHandler method creates handlers without knowledge of platform-specific implementations:

```
func (dm *DiskManager) GetHandler(topic string, partitionID int) (*DiskHandler, error) {
    dm.mu.Lock()
    defer dm.mu.Unlock()
    
    key := fmt.Sprintf("%s_%d", topic, partitionID)
    if dh, ok := dm.handlers[key]; ok {
        return dh, nil
    }
    
    // Creates handler; platform-specific methods linked at compile time
    dh, err := NewDiskHandler(dm.cfg, topic, partitionID, segmentSize)
    if err != nil {
        return nil, err
    }
    
    dm.handlers[key] = dh
    return dh, nil
}
```

# Summary

cursus's platform-specific optimizations leverage Linux kernel features for high-performance segment transfers while maintaining Windows compatibility through standard I/O operations:

## Linux Optimizations:

- fadvise(SEQUENTIAL) hints sequential access patterns
- `sendfile` enables zero-copy file-to-socket transfers
- Raw socket access via `SyscallConn()` for direct system call invocation

## Windows Implementation:

- Standard `os.OpenFile` and `bufio.Writer`
- `io.Copy` for segment transfers 
- Simpler implementation with broader compatibility

The build tag system ensures zero runtime overhead from platform detection—each platform compiles only its specific implementation.