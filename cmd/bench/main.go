package main

import (
	"flag"

	"github.com/downfa11-org/go-broker/pkg/bench"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "broker address")
	topicName := flag.String("topic", "bench-topic", "topic name for benchmark")
	partitions := flag.Int("partitions", 12, "number of partitions")
	producers := flag.Int("producers", 12, "number of producers")
	consumers := flag.Int("consumers", 12, "number of consumers")
	messages := flag.Int("messages", 100, "messages per producer")
	flag.Parse()

	runner := bench.NewBenchmarkRunner(*addr, *topicName, *partitions, *producers, *consumers, *messages, false)
	runner.Run()
}
