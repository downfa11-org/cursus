package bench

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const sep = "========================================"

type PartitionStat struct {
	PartitionID int
	BatchCount  int
	AvgDuration time.Duration
}

func GenerateMessage(size int, seqNum int) string {
	if size <= 0 {
		return fmt.Sprintf("%s #%d", "Hello World!", seqNum)
	}

	header := fmt.Sprintf("msg-%d-", seqNum)
	paddingSize := size - len(header)
	if paddingSize < 0 {
		paddingSize = 0
	}

	padding := strings.Repeat("x", paddingSize)
	return header + padding
}

func PrintBenchmarkSummaryFixedTo(w io.Writer, partitionStats []PartitionStat, sentMessages int, totalDuration time.Duration) {
	totalBatches := 0
	for _, ps := range partitionStats {
		totalBatches += ps.BatchCount
	}

	seconds := totalDuration.Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}
	batchesPerSec := float64(totalBatches) / seconds
	messagesPerSec := float64(sentMessages) / seconds

	fmt.Fprint(w, "\r\n")
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, "BENCHMARK SUMMARY")
	fmt.Fprintf(w, "%-28s : %d\n", "Partitions", len(partitionStats))
	fmt.Fprintf(w, "%-28s : %d\n", "Total Batches", totalBatches)
	fmt.Fprintf(w, "%-28s : %d\n", "Total messages published", sentMessages)
	fmt.Fprintf(w, "%-28s : %.3fs\n", "Publish elapsed Time", totalDuration.Seconds())
	fmt.Fprintf(w, "%-28s : %.2f batches/s\n", "Publish Batch Throughput", batchesPerSec)
	fmt.Fprintf(w, "%-28s : %.2f msg/s\n", "Publish Message Throughput", messagesPerSec)
	fmt.Fprint(w, "\r\n")

	fmt.Fprintln(w, "Partition Breakdown:")
	for _, ps := range partitionStats {
		fmt.Fprintf(w, "  #%-3d  batches=%-5d  avg_batch=%.3fms\n", ps.PartitionID, ps.BatchCount, float64(ps.AvgDuration.Microseconds())/1000.0)
	}
	fmt.Fprintln(w, sep)
}

func PrintBenchmarkSummaryFixed(partitionStats []PartitionStat, sentMessages int, totalDuration time.Duration) {
	PrintBenchmarkSummaryFixedTo(os.Stdout, partitionStats, sentMessages, totalDuration)
}
