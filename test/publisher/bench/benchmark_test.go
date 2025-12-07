package bench_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/publisher/bench"
)

func TestGenerateMessage(t *testing.T) {
	tests := []struct {
		size   int
		seqNum int
		want   string
	}{
		{0, 1, "Hello World! #1"},
		{5, 1, "msg-1-"},
		{15, 2, "msg-2-xxxxxxx"},
	}

	for _, tt := range tests {
		got := bench.GenerateMessage(tt.size, tt.seqNum)
		if !strings.HasPrefix(got, "msg-") && tt.size > 0 {
			t.Errorf("GenerateMessage(%d, %d) = %q; want prefix 'msg-'", tt.size, tt.seqNum, got)
		}
		if tt.size <= 0 && got != tt.want {
			t.Errorf("GenerateMessage(%d, %d) = %q; want %q", tt.size, tt.seqNum, got, tt.want)
		}
		if tt.size > 0 && len(got) < tt.size {
			t.Errorf("GenerateMessage(%d, %d) length = %d; want >= %d", tt.size, tt.seqNum, len(got), tt.size)
		}
	}
}

func TestPrintBenchmarkSummaryFixed(t *testing.T) {
	stats := []bench.PartitionStat{
		{PartitionID: 0, BatchCount: 10, AvgDuration: 20 * time.Millisecond},
		{PartitionID: 1, BatchCount: 15, AvgDuration: 25 * time.Millisecond},
	}

	sentMessages := 500
	totalDuration := 2 * time.Second

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, sentMessages, totalDuration)

	got := buf.String()
	if !strings.Contains(got, "Total Batches") {
		t.Errorf("Output missing 'Total Batches': %s", got)
	}
	if !strings.Contains(got, "Publish Message Throughput") {
		t.Errorf("Output missing 'Publish Message Throughput': %s", got)
	}
}
