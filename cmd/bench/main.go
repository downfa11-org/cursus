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
	messages := flag.Int("messages", 10000, "messages per producer")
	messageSize := flag.Int("message-size", 1024, "message size in bytes")

	batchSize := flag.Int("batch-size", 1, "batch size for sending messages")
	lingerMS := flag.Int("linger-ms", 0, "linger time in milliseconds")
	maxInflight := flag.Int("max-inflight", 1, "max inflight requests for async mode")
	useAsync := flag.Bool("async", false, "use async sending mode")

	flag.Parse()

	runner := bench.NewBenchmarkRunner(*addr, *topicName, *partitions, *producers, *consumers, *messages, *messageSize, false)
	runner.BatchSize = *batchSize
	runner.LingerMS = *lingerMS
	runner.MaxInflight = *maxInflight
	runner.UseAsync = *useAsync

	runner.Run()
}
