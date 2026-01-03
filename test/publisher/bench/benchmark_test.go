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
		{PartitionID: 1, BatchCount: 15, AvgDuration: 25 * time.Millisecond},
	}

	totalTarget := 1000
	sentMessages := 800
	totalDuration := 2 * time.Second

	errSummary := map[string]uint64{
		"connection timeout": 150,
		"broker full":        50,
	}

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, sentMessages, totalTarget, totalDuration, errSummary)

	got := buf.String()

	expectedKeywords := []string{
		"BENCHMARK SUMMARY",
		"Total Target",
		"Successfully Sent",
		"Failed Messages",
		"Throughput",
		"Partition Breakdown",
	}

	for _, keyword := range expectedKeywords {
		if !strings.Contains(got, keyword) {
			t.Errorf("Output missing keyword '%s'", keyword)
		}
	}

	if !strings.Contains(got, "Error Root Cause Analysis") {
		t.Error("Output missing 'Error Root Cause Analysis' section")
	}
	if !strings.Contains(got, "connection timeout") {
		t.Error("Output missing specific error message 'connection timeout'")
	}

	if !strings.Contains(got, "20.00%") {
		t.Errorf("Output should contain correct failure percentage '20.00%%', got: %s", got)
	}
}

func TestPrintBenchmarkSummaryNoErrors(t *testing.T) {
	stats := []bench.PartitionStat{{PartitionID: 0, BatchCount: 5, AvgDuration: 10 * time.Millisecond}}
	totalTarget := 100
	sentMessages := 100
	errSummary := make(map[string]uint64)

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, sentMessages, totalTarget, time.Second, errSummary)

	got := buf.String()
	if strings.Contains(got, "Error Root Cause Analysis") {
		t.Error("Output should NOT contain Error Analysis section when there are no errors")
	}
}
