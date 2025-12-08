package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/downfa11-org/go-broker/consumer/config"
	"github.com/downfa11-org/go-broker/consumer/handler"
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

	c, err := subscriber.NewConsumer(*cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	handler := handler.NewMemoryIdempotentHandler(cfg.EnableBenchmark)
	c.SetBatchHandler(handler)

	if err := c.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	if err := c.Close(); err != nil {
		util.Error("Error closing consumer: %v", err)
	}
}
