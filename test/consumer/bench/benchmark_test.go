package bench_test

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/downfa11-org/go-broker/consumer/bench"
)

func TestConsumerMetrics_RecordMessage(t *testing.T) {
	metrics := bench.NewConsumerMetrics(10)

	metrics.RecordMessage(0)
	metrics.RecordMessage(1)
	metrics.RecordMessage(0)

	if atomic.LoadInt64(&metrics.MsgCount) != 3 {
		t.Errorf("Expected total MsgCount=3, got %d", metrics.MsgCount)
	}

	if metrics.PartitionCounts[0] == nil || atomic.LoadInt64(metrics.PartitionCounts[0]) != 2 {
		t.Errorf("Expected partition 0 count=2, got %v", metrics.PartitionCounts[0])
	}
	if metrics.PartitionCounts[1] == nil || atomic.LoadInt64(metrics.PartitionCounts[1]) != 1 {
		t.Errorf("Expected partition 1 count=1, got %v", metrics.PartitionCounts[1])
	}
}

func TestConsumerMetrics_IsDone(t *testing.T) {
	metrics := bench.NewConsumerMetrics(3)
	if metrics.IsDone() {
		t.Errorf("Expected not done initially")
	}

	metrics.RecordMessage(0)
	metrics.RecordMessage(1)
	if metrics.IsDone() {
		t.Errorf("Expected not done yet")
	}

	metrics.RecordMessage(2)
	if !metrics.IsDone() {
		t.Errorf("Expected done after reaching target")
	}
}

func TestConsumerMetrics_PrintSummary(t *testing.T) {
	metrics := bench.NewConsumerMetrics(2)
	metrics.RecordMessage(0)
	metrics.RecordMessage(1)

	var buf bytes.Buffer
	metrics.PrintSummaryTo(&buf)

	output := buf.String()
	if !strings.Contains(output, "Total messages consumed: 2") {
		t.Errorf("Summary output missing total messages: %s", output)
	}
	if !strings.Contains(output, "Partition [0]: 1 messages") ||
		!strings.Contains(output, "Partition [1]: 1 messages") {
		t.Errorf("Summary output missing partition counts: %s", output)
	}
}

func TestConsumerMetrics_Concurrency(t *testing.T) {
	metrics := bench.NewConsumerMetrics(100)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				metrics.RecordMessage(pid)
			}
		}(i)
	}
	wg.Wait()

	if atomic.LoadInt64(&metrics.MsgCount) != 100 {
		t.Errorf("Expected MsgCount=100, got %d", metrics.MsgCount)
	}

	for i := 0; i < 10; i++ {
		if c := atomic.LoadInt64(metrics.PartitionCounts[i]); c != 10 {
			t.Errorf("Partition %d expected count=10, got %d", i, c)
		}
	}
}
