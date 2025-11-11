package main

import (
	"fmt"
	"log"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/server"
	"github.com/downfa11-org/go-broker/pkg/topic"
)

func main() {
	// Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	fmt.Printf("🚀 Starting broker on port %d\n", cfg.BrokerPort)
	fmt.Printf("🧠 Benchmark: %v | 📊 Exporter: %v\n", cfg.EnableBenchmark, cfg.EnableExporter)

	// Initialization
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm)

	if err := server.RunServer(cfg, tm, dm); err != nil {
		log.Fatalf("❌ Broker failed: %v", err)
	}
}
