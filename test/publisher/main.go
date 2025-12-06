package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/downfa11-org/go-broker/publisher/bench"
	"github.com/downfa11-org/go-broker/publisher/config"
	"github.com/downfa11-org/go-broker/publisher/producer"
)

func main() {
	cfg, err := config.LoadPublisherConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal config: %v", err)
	} else {
		log.Printf("Configuration:\n%s", string(data))
	}

	pub := producer.NewPublisher(cfg)
	defer pub.Close()

	total := cfg.NumMessages
	msgs := make([]string, 0, total)
	for i := 0; i < total; i++ {
		var msg string
		if cfg.EnableBenchmark {
			msg = bench.GenerateMessage(cfg.MessageSize, i)
		} else {
			msg = fmt.Sprintf("hello-%d", i)
		}
		msgs = append(msgs, msg)
	}

	start := time.Now()

	batchSize := cfg.BatchSize
	for i := 0; i < len(msgs); i += batchSize {
		end := i + batchSize
		if end > len(msgs) {
			end = len(msgs)
		}
		batch := msgs[i:end]

		for _, m := range batch {
			if _, err := pub.PublishMessage(m); err != nil {
				log.Printf("[ERROR] publish failed: %v", err)
			}
		}

		if cfg.PublishDelayMS > 0 {
			time.Sleep(time.Duration(cfg.PublishDelayMS) * time.Millisecond)
		}
	}

	log.Println("All messages queued, flushing...")

	if !cfg.EnableBenchmark {
		pub.Flush()
	} else {
		pub.FlushBenchmark(total)
		duration := time.Since(start)

		if err := pub.VerifySentSequences(total); err != nil {
			log.Printf("verify: %v", err)
		}

		partitionStats := make([]bench.PartitionStat, 0, pub.GetPartitionCount())
		stats := pub.GetPartitionStats()
		partitionStats = append(partitionStats, stats...)

		sentMessages := pub.GetSentMessageCount()

		bench.PrintBenchmarkSummaryFixed(partitionStats, sentMessages, duration)
		os.Exit(0)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	pub.Close()
	os.Exit(0)
}
