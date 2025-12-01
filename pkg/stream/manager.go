package stream

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type StreamManager struct {
	streams   map[string]*StreamConnection // key: "topic:partition:group"
	mu        sync.RWMutex
	maxConn   int
	timeout   time.Duration
	heartbeat time.Duration
}

type StreamConnection struct {
	conn      net.Conn
	topic     string
	partition int
	group     string

	mu         sync.RWMutex
	offset     uint64
	lastActive time.Time

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewStreamManager(maxConn int, timeout, heartbeat time.Duration) *StreamManager {
	return &StreamManager{
		streams:   make(map[string]*StreamConnection),
		maxConn:   maxConn,
		timeout:   timeout,
		heartbeat: heartbeat,
	}
}

// NewStreamConnection creates a new stream connection
func NewStreamConnection(conn net.Conn, topic string, partition int, group string, offset uint64) *StreamConnection {
	sc := &StreamConnection{
		conn:       conn,
		topic:      topic,
		partition:  partition,
		group:      group,
		offset:     offset,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
	}
	sc.SetLastActive(time.Now())
	return sc
}

func (sm *StreamManager) AddStream(key string, stream *StreamConnection) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.streams) >= sm.maxConn {
		return fmt.Errorf("maximum connections (%d) reached", sm.maxConn)
	}

	sm.streams[key] = stream
	go sm.monitorConnection(key, stream)
	return nil
}

func (sm *StreamManager) RemoveStream(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if stream, ok := sm.streams[key]; ok {
		delete(sm.streams, key)
		stream.Stop()
	}
}

func (sm *StreamManager) monitorConnection(key string, stream *StreamConnection) {
	ticker := time.NewTicker(sm.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-stream.stopCh:
			stream.closeConn()
			return
		case <-ticker.C:
			if time.Since(stream.LastActive()) > sm.timeout {
				sm.RemoveStream(key)
				return
			}
		}
	}
}

func (sc *StreamConnection) closeConn() {
	sc.stopOnce.Do(func() {
		close(sc.stopCh)
		if sc.conn != nil {
			_ = sc.conn.Close()
			sc.conn = nil
		}
	})
}

func (sm *StreamManager) StopStream(key string) {
	sm.mu.RLock()
	stream, ok := sm.streams[key]
	sm.mu.RUnlock()
	if !ok {
		return
	}

	close(stream.stopCh)
}

func (sm *StreamManager) GetStreamsForPartition(topic string, partitionID int) []*StreamConnection {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var streams []*StreamConnection
	for _, stream := range sm.streams {
		if stream.topic == topic && stream.partition == partitionID {
			streams = append(streams, stream)
		}
	}
	return streams
}

func (sc *StreamConnection) Topic() string  { return sc.topic }
func (sc *StreamConnection) Partition() int { return sc.partition }
func (sc *StreamConnection) Group() string  { return sc.group }
func (sc *StreamConnection) Conn() net.Conn { return sc.conn }

func (sc *StreamConnection) Offset() uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.offset
}

func (sc *StreamConnection) SetOffset(o uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.offset = o
}

func (sc *StreamConnection) IncrementOffset() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.offset++
}

func (sc *StreamConnection) SetLastActive(t time.Time) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastActive = t
}

func (sc *StreamConnection) LastActive() time.Time {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.lastActive
}

func (sc *StreamConnection) StopCh() <-chan struct{} { return sc.stopCh }

func (sc *StreamConnection) Stop() {
	sc.stopOnce.Do(func() {
		close(sc.stopCh)
	})
}
