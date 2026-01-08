package bench_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/downfa11-org/cursus/test/publisher/bench"
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
		if tt.size <= 0 {
			if got != tt.want {
				t.Errorf("GenerateMessage(%d, %d) = %q; want %q", tt.size, tt.seqNum, got, tt.want)
			}
		} else {
			if !strings.HasPrefix(got, "msg-") {
				t.Errorf("GenerateMessage(%d, %d) = %q; want prefix 'msg-'", tt.size, tt.seqNum, got)
			}
			if len(got) < tt.size {
				t.Errorf("GenerateMessage(%d, %d) length = %d; want >= %d", tt.size, tt.seqNum, len(got), tt.size)
			}
		}
	}
}

func TestPrintBenchmarkSummaryFixedTo(t *testing.T) {
	stats := []bench.PartitionStat{
		{PartitionID: 0, BatchCount: 10, AvgDuration: 20 * time.Millisecond},
	}

	latencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		latencies[i] = time.Duration(i) * time.Millisecond
	}

	totalTarget := 1000
	uniqueSent := 800
	totalPublished := 950
	totalDuration := 2 * time.Second
	errSummary := map[string]uint64{"timeout": 200}

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, totalPublished, uniqueSent, totalTarget, totalDuration, errSummary, latencies)

	got := buf.String()

	expectedKeywords := []string{
		"PRODUCER BENCHMARK SUMMARY",
		"Total Messages",
		"Latency P95",
		"Latency P99",
		"rate: 80.00%",
		"Failed messages",
		"Retry Count",
	}

	for _, keyword := range expectedKeywords {
		if !strings.Contains(got, keyword) {
			t.Errorf("Output missing keyword '%s'", keyword)
		}
	}
}

func TestPrintBenchmarkSummaryNoErrors(t *testing.T) {
	stats := []bench.PartitionStat{{PartitionID: 0, BatchCount: 5, AvgDuration: 10 * time.Millisecond}}
	totalTarget := 100
	uniqueSent := 100
	totalPublished := 100
	testDuration := time.Second
	testLatencies := []time.Duration{10 * time.Millisecond}

	errSummary := make(map[string]uint64)

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, totalPublished, uniqueSent, totalTarget, testDuration, errSummary, testLatencies)

	got := buf.String()
	if strings.Contains(got, "Error Root Cause Analysis") {
		t.Error("Output should NOT contain Error Analysis section when there are no errors")
	}
}
