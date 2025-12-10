package bench

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const sep = "========================================"

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
	m.mu.RLock()
	defer m.mu.RUnlock()

	duration := time.Since(m.StartTime)
	total := atomic.LoadInt64(&m.MsgCount)
	processed := atomic.LoadInt64(&m.ProcessedCount)
	tps := float64(total) / duration.Seconds()
	processedTps := float64(processed) / duration.Seconds()

	fmt.Fprint(w, "\r\n")
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, "BENCHMARK SUMMARY")
	fmt.Fprintf(w, "%-28s : %d\n", "Total messages consumed", total)
	fmt.Fprintf(w, "%-28s : %d\n", "Actually processed messages", processed)
	fmt.Fprintf(w, "%-28s : %d\n", "Duplicate messages filtered", total-processed)
	fmt.Fprintf(w, "%-28s : %v\n", "Consume elapsed time", duration)
	fmt.Fprintf(w, "%-28s : %.2f msg/s\n", "Consume Throughput", tps)
	fmt.Fprintf(w, "%-28s : %.2f msg/s\n", "Processed Throughput", processedTps)
	fmt.Fprint(w, "\r\n")

	fmt.Fprintln(w, "--- Partition Message Counts ---")
	for pid, count := range m.PartitionCounts {
		if count != nil {
			fmt.Fprintf(w, "Partition [%d] : %d messages\n", pid, atomic.LoadInt64(count))
		}
	}
	fmt.Fprintln(w, sep)
}

func (m *ConsumerMetrics) PrintSummary() {
	m.PrintSummaryTo(os.Stdout)
}
