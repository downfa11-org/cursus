package bench

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ConsumerMetrics struct {
	MsgCount        int64
	ProcessedCount  int64
	StartTime       time.Time
	PartitionCounts map[int]*int64
	TargetMessages  int64

	mu sync.RWMutex
}

func NewConsumerMetrics(target int64) *ConsumerMetrics {
	return &ConsumerMetrics{
		StartTime:       time.Now(),
		PartitionCounts: make(map[int]*int64),
		TargetMessages:  target,
	}
}

func (cm *ConsumerMetrics) RecordMessage(partition int) {
	cm.mu.RLock()
	ptr, ok := cm.PartitionCounts[partition]
	cm.mu.RUnlock()

	if !ok {
		cm.mu.Lock()
		ptr, ok = cm.PartitionCounts[partition]
		if !ok {
			p := new(int64)
			*p = 0
			cm.PartitionCounts[partition] = p
			ptr = p
		}
		cm.mu.Unlock()
	}

	atomic.AddInt64(ptr, 1)
	atomic.AddInt64(&cm.MsgCount, 1)
}

func (cm *ConsumerMetrics) RecordProcessed(count int64) {
	atomic.AddInt64(&cm.ProcessedCount, count)
}

func (m *ConsumerMetrics) IsDone() bool {
	return atomic.LoadInt64(&m.MsgCount) >= m.TargetMessages
}

func (m *ConsumerMetrics) PrintSummaryTo(w io.Writer) {
	duration := time.Since(m.StartTime)
	total := atomic.LoadInt64(&m.MsgCount)
	processed := atomic.LoadInt64(&m.ProcessedCount)
	tps := float64(total) / duration.Seconds()
	processedTps := float64(processed) / duration.Seconds()

	fmt.Fprintln(w, "=== CONSUMER BENCHMARK SUMMARY ===")
	fmt.Fprintf(w, "Total messages consumed: %d\n", total)
	fmt.Fprintf(w, "Actually processed messages: %d\n", processed)
	fmt.Fprintf(w, "Duplicate messages filtered: %d\n", total-processed)
	fmt.Fprintf(w, "Consume elapsed time: %v\n", duration)
	fmt.Fprintf(w, "Consume Throughput: %.2f msg/s\n", tps)
	fmt.Fprintf(w, "Processed Throughput: %.2f msg/s\n", processedTps)

	fmt.Fprintln(w, "--- Partition Message Counts ---")
	for pid, count := range m.PartitionCounts {
		if count != nil {
			fmt.Fprintf(w, "Partition [%d]: %d messages\n", pid, atomic.LoadInt64(count))
		}
	}
	fmt.Fprintln(w, "==================================")
}

func (m *ConsumerMetrics) PrintSummary() {
	m.PrintSummaryTo(io.Writer(os.Stdout))
}
