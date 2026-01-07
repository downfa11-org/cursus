package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/downfa11-org/go-broker/consumer/config"
	"github.com/downfa11-org/go-broker/consumer/subscriber"
	"github.com/downfa11-org/go-broker/util"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		util.Error("Failed to marshal config: %v", err)
	} else {
		util.Info("Configuration:\n%s", string(data))
	}

	c, err := subscriber.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		if err := c.Start(); err != nil {
			errCh <- err
		}
	}()

	select {
	case sig := <-sigCh:
		util.Info("Received signal: %v, shutting down...", sig)
	case err := <-errCh:
		util.Error("Consumer error: %v", err)
	case <-c.Done():
		util.Info("Benchmark completed successfully.")
	}
	if err := c.Close(); err != nil {
		util.Error("❌ Error closing consumer: %v", err)
	}

	util.Info("✅ Consumer closed gracefully")

	if cfg.EnableBenchmark {
		c.GetMetrics().PrintSummary()
	}
	os.Exit(0)
}
