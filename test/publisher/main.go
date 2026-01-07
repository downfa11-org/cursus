package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/downfa11-org/go-broker/publisher/bench"
	"github.com/downfa11-org/go-broker/publisher/config"
	"github.com/downfa11-org/go-broker/publisher/producer"
	"github.com/downfa11-org/go-broker/util"
)

func main() {
	cfg, err := config.LoadPublisherConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		util.Error("Failed to marshal config: %v", err)
	} else {
		util.Info("Configuration:\n%s", string(data))
	}

	pub, err := producer.NewPublisher(cfg)
	if err != nil {
		util.Fatal("Failed to create publisher: %v", err)
	}
	defer pub.Close()

	total := cfg.NumMessages
	start := time.Now()

	var (
		wg         sync.WaitGroup
		errSummary sync.Map
	)

	numWorkers := runtime.NumCPU()
	chunkSize := total / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startIdx := workerID * chunkSize
			endIdx := startIdx + chunkSize
			if workerID == numWorkers-1 {
				endIdx = total
			}

			for i := startIdx; i < endIdx; i++ {
				var msg string
				if cfg.EnableBenchmark {
					msg = bench.GenerateMessage(cfg.MessageSize, i)
				} else {
					msg = fmt.Sprintf("hello-%d", i)
				}

				if _, err := pub.PublishMessage(msg); err != nil {
					errMsg := err.Error()
					newCounter := new(uint64)
					*newCounter = 1

					actual, loaded := errSummary.LoadOrStore(errMsg, newCounter)
					if loaded {
						atomic.AddUint64(actual.(*uint64), 1)
					}

					util.Debug("publish failed: %v", err)
				}
			}
		}(w)
	}

	wg.Wait()

	if !cfg.EnableBenchmark {
		pub.Flush()
	} else {
		pub.FlushBenchmark(total)
		duration := time.Since(start)

		totalSent := pub.GetPublishedCount()
		publishedMessages := pub.GetUniqueAckCount()
		targetCount := total

		errors := make(map[string]uint64)
		errSummary.Range(func(key, value interface{}) bool {
			errors[key.(string)] = atomic.LoadUint64(value.(*uint64))
			return true
		})

		if err := pub.VerifySentSequences(total); err != nil {
			util.Info("verify failed: %v", err)
		}

		stats := pub.GetPartitionStats()
		latencies := pub.GetLatencies()

		util.Info("🎉 Benchmark completed successfully!")
		bench.PrintBenchmarkSummaryFixedTo(os.Stdout, stats, totalSent, publishedMessages, targetCount, duration, errors, latencies)
		os.Exit(0)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	pub.Close()
	os.Exit(0)
}
