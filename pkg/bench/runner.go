package bench

import (
	"fmt"
	"time"
)

type BenchmarkRunner struct {
	Addr                string
	NumProducers        int
	NumConsumers        int
	MessagesPerProducer int
	Partitions          int
	EnableGzip          bool
	Topic               string
}

func NewBenchmarkRunner(addr, topicName string, partitions, producers, consumers, messages int, gzip bool) *BenchmarkRunner {
	return &BenchmarkRunner{
		Addr:                addr,
		NumProducers:        producers,
		NumConsumers:        consumers,
		MessagesPerProducer: messages,
		EnableGzip:          gzip,
		Topic:               topicName,
		Partitions:          partitions,
	}
}

// Run executes the full producer -> consumer benchmark workflow.
func (b *BenchmarkRunner) Run() {
	totalMessagesProduced := b.NumProducers * b.MessagesPerProducer
	if b.NumProducers == 0 || b.MessagesPerProducer == 0 {
		fmt.Println("Error: NumProducers or MessagesPerProducer cannot be zero.")
		return
	}

	initClient := &BenchClient{
		Addr:        b.Addr,
		EnableGzip:  b.EnableGzip,
		NumMessages: 0,
		Topic:       b.Topic,
		Partitions:  b.Partitions,
	}

	fmt.Printf("\nInitializing Topic '%s' with %d partitions...\n", b.Topic, b.Partitions)
	if err := initClient.RunTopicCreationPhase(); err != nil {
		fmt.Printf("Topic Initialization error: %v\n", err)
		return
	}

	fmt.Printf("\nStarting Producer Phase (%d Producers, %d Total Messages)\n", b.NumProducers, totalMessagesProduced)
	producerStart := time.Now()

	if err := b.RunConcurrentProducerPhase(); err != nil {
		fmt.Printf("Concurrent Producer Phase error: %v\n", err)
		return
	}

	producerDuration := time.Since(producerStart)
	fmt.Printf("Producer Phase Finished in %v\n", producerDuration)

	if b.NumConsumers > 0 {
		fmt.Printf("\nStarting Consumer Phase (%d Consumers)\n", b.NumConsumers)
		consumerStart := time.Now()

		consumerClient := &BenchClient{
			Addr:        b.Addr,
			EnableGzip:  b.EnableGzip,
			NumMessages: totalMessagesProduced,
			Topic:       b.Topic,
			Partitions:  b.Partitions,
		}

		if err := consumerClient.RunConsumerPhase(b.NumConsumers); err != nil {
			fmt.Printf("Consumer Phase error: %v\n", err)
		}

		consumerDuration := time.Since(consumerStart)
		fmt.Printf("Consumer Phase Finished in %v\n", consumerDuration)

		totalDuration := producerDuration + consumerDuration
		throughput := float64(totalMessagesProduced) / totalDuration.Seconds()

		fmt.Printf("\n🧪 BENCHMARK RESULT [disk] 🧪\n")
		fmt.Printf("-------------------------------------\n")
		fmt.Printf(" Topic                 : %s\n", b.Topic)
		fmt.Printf(" Partitions            : %d\n", b.Partitions)
		fmt.Printf(" Producers             : %d\n", b.NumProducers)
		fmt.Printf(" Consumers             : %d\n", b.NumConsumers)
		fmt.Printf(" Total Messages        : %d\n", totalMessagesProduced)
		fmt.Printf(" Producer Duration     : %v\n", producerDuration)
		fmt.Printf(" Consumer Duration     : %v\n", consumerDuration)
		fmt.Printf(" Total Duration (P+C)  : %v\n", totalDuration)
		fmt.Printf(" Throughput (Combined) : %.2f msg/sec\n", throughput)
		fmt.Printf("-------------------------------------\n")
	} else {
		throughput := float64(totalMessagesProduced) / producerDuration.Seconds()

		fmt.Printf("\n🧪 BENCHMARK RESULT [disk] (PRODUCE ONLY) 🧪\n")
		fmt.Printf("-------------------------------------\n")
		fmt.Printf(" Topic                 : %s\n", b.Topic)
		fmt.Printf(" Partitions            : %d\n", b.Partitions)
		fmt.Printf(" Producers             : %d\n", b.NumProducers)
		fmt.Printf(" Total Messages        : %d\n", totalMessagesProduced)
		fmt.Printf(" Duration              : %v\n", producerDuration)
		fmt.Printf(" Throughput (Produce)  : %.2f msg/sec\n", throughput)
		fmt.Printf("-------------------------------------\n")
	}
}
