package bench

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

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

	fmt.Fprintln(w, "=== BENCHMARK SUMMARY ===")
	fmt.Fprintf(w, "Partitions                 : %d\n", len(partitionStats))
	fmt.Fprintf(w, "Total Batches              : %d\n", totalBatches)
	fmt.Fprintf(w, "Total messages published   : %d\n", sentMessages)
	fmt.Fprintf(w, "Publish elapsed Time       : %.3fs\n", totalDuration.Seconds())
	fmt.Fprintf(w, "Publish Batch Throughput   : %.2f batches/s\n", batchesPerSec)
	fmt.Fprintf(w, "Publish Message Throughput : %.2f msg/s\n", messagesPerSec)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "Partition Breakdown:")
	for _, ps := range partitionStats {
		fmt.Fprintf(
			w,
			"  #%d  batches=%d  avg_batch=%.3fms\n",
			ps.PartitionID,
			ps.BatchCount,
			float64(ps.AvgDuration.Microseconds())/1000.0,
		)
	}
	fmt.Fprintln(w, "========================================")
}

func PrintBenchmarkSummaryFixed(partitionStats []PartitionStat, sentMessages int, totalDuration time.Duration) {
	PrintBenchmarkSummaryFixedTo(io.Writer(os.Stdout), partitionStats, sentMessages, totalDuration)
}
