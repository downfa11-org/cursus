package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/downfa11-org/go-broker/pkg/bench"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "broker address")
	topicName := flag.String("topic", "bench-topic", "topic name for benchmark")
	partitions := flag.Int("partitions", 12, "number of partitions")
	producers := flag.Int("producers", 1, "number of producers")
	consumers := flag.Int("consumers", 12, "number of consumers")
	messages := flag.Int("messages", 1000, "messages per producer")
	flag.Parse()

	runner := bench.NewBenchmarkRunner(*addr, *topicName, *partitions, *producers, *consumers, *messages, false)

	if err := func() error {
		runner.Run()
		return nil
	}(); err != nil {
		fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
		os.Exit(1)
	}
}
