package bench

import (
	"fmt"
	"sync"
	"time"
)

type BenchmarkRunner struct {
	Addr                string
	NumProducers        int
	NumConsumers        int
	MessagesPerProducer int
	MessageSize         int
	Partitions          int
	EnableGzip          bool
	Topic               string

	BatchSize   int
	LingerMS    int
	MaxInflight int
	UseAsync    bool
}

func NewBenchmarkRunner(addr, topicName string, partitions, producers, consumers, messages, messageSize int, gzip bool) *BenchmarkRunner {
	return &BenchmarkRunner{
		Addr:                addr,
		NumProducers:        producers,
		NumConsumers:        consumers,
		MessagesPerProducer: messages,
		MessageSize:         messageSize,
		EnableGzip:          gzip,
		Topic:               topicName,
		Partitions:          partitions,

		BatchSize:   1,
		LingerMS:    0,
		MaxInflight: 1,
		UseAsync:    false,
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
		MessageSize: b.MessageSize,
		BatchSize:   b.BatchSize,
		LingerMS:    b.LingerMS,
		MaxInflight: b.MaxInflight,
		UseAsync:    b.UseAsync,
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

	var consumerDuration time.Duration

	if b.NumConsumers > 0 {
		fmt.Printf("\nStarting Consumer Phase (%d Consumers)\n", b.NumConsumers)
		consumerStart := time.Now()

		consumerClient := &BenchClient{
			Addr:        b.Addr,
			EnableGzip:  b.EnableGzip,
			NumMessages: b.MessagesPerProducer,
			Topic:       b.Topic,
			Partitions:  b.Partitions,
			MessageSize: b.MessageSize,
			BatchSize:   b.BatchSize,
			LingerMS:    b.LingerMS,
			MaxInflight: b.MaxInflight,
			UseAsync:    b.UseAsync,
		}

		if err := consumerClient.RunConsumerPhase(b.NumConsumers); err != nil {
			fmt.Printf("Consumer Phase error: %v\n", err)
		}

		consumerDuration = time.Since(consumerStart)
		fmt.Printf("Consumer Phase Finished in %v\n", consumerDuration)
	}

	if b.NumConsumers > 0 {
		b.printBenchmarkResults(totalMessagesProduced, producerDuration, consumerDuration)
	} else {
		b.printProduceOnlyResults(totalMessagesProduced, producerDuration)
	}
}

func (b *BenchmarkRunner) printBenchmarkResults(totalMessagesProduced int, producerDuration, consumerDuration time.Duration) {
	totalDuration := producerDuration + consumerDuration
	combinedThroughput := float64(totalMessagesProduced) / totalDuration.Seconds()
	producerThroughput := float64(totalMessagesProduced) / producerDuration.Seconds()

	var consumerThroughput float64
	if consumerDuration.Seconds() > 0 {
		consumerThroughput = float64(totalMessagesProduced) / consumerDuration.Seconds()
	}

	fmt.Printf("\n🧪 BENCHMARK RESULT [disk] 🧪\n")
	fmt.Printf("=====================================\n")

	fmt.Printf("📊 CONFIGURATION\n")
	fmt.Printf(" Topic                 : %s\n", b.Topic)
	fmt.Printf(" Partitions            : %d\n", b.Partitions)
	fmt.Printf(" Producers             : %d\n", b.NumProducers)
	fmt.Printf(" Consumers             : %d\n", b.NumConsumers)
	fmt.Printf(" Total Messages        : %d\n", totalMessagesProduced)
	fmt.Printf(" Message Size          : %d bytes\n", b.MessageSize)

	fmt.Printf("\n🚀 TRANSMISSION MODE\n")
	if b.UseAsync && b.BatchSize > 1 {
		fmt.Printf(" Mode                  : Async+Batch (Size: %d, Max Inflight: %d)\n", b.BatchSize, b.MaxInflight)
	} else if b.UseAsync {
		fmt.Printf(" Mode                  : Async (Max Inflight: %d)\n", b.MaxInflight)
	} else if b.BatchSize > 1 {
		fmt.Printf(" Mode                  : Batch (Size: %d, Linger: %dms)\n", b.BatchSize, b.LingerMS)
	} else {
		fmt.Printf(" Mode                  : Synchronous\n")
	}
	fmt.Printf(" Compression           : %v\n", b.EnableGzip)

	fmt.Printf("\n⏱️  PERFORMANCE METRICS\n")
	fmt.Printf(" Producer Duration     : %v\n", producerDuration)
	fmt.Printf(" Consumer Duration     : %v\n", consumerDuration)
	fmt.Printf(" Total Duration (P+C)  : %v\n", totalDuration)

	fmt.Printf("\n📈 THROUGHPUT\n")
	fmt.Printf(" Producer Throughput   : %.2f msg/sec\n", producerThroughput)
	if consumerThroughput > 0 {
		fmt.Printf(" Consumer Throughput   : %.2f msg/sec\n", consumerThroughput)
	}
	fmt.Printf(" Combined Throughput   : %.2f msg/sec\n", combinedThroughput)

	fmt.Printf("\n📋 ANALYSIS\n")
	avgMsgPerProducer := float64(b.MessagesPerProducer)
	fmt.Printf(" Messages per Producer : %.0f\n", avgMsgPerProducer)

	if producerDuration.Seconds() > 0 {
		avgProducerLatency := producerDuration.Seconds() / avgMsgPerProducer * 1000
		fmt.Printf(" Avg Producer Latency  : %.2f ms/msg\n", avgProducerLatency)
	}

	if b.NumProducers > 0 {
		throughputPerProducer := producerThroughput / float64(b.NumProducers)
		fmt.Printf(" Throughput per Producer: %.2f msg/sec\n", throughputPerProducer)
	}

	fmt.Printf("=====================================\n")
}

func (b *BenchmarkRunner) printProduceOnlyResults(totalMessagesProduced int, producerDuration time.Duration) {
	producerThroughput := float64(totalMessagesProduced) / producerDuration.Seconds()

	fmt.Printf("\n🧪 BENCHMARK RESULT [disk] (PRODUCE ONLY) 🧪\n")
	fmt.Printf("=====================================\n")

	fmt.Printf("📊 CONFIGURATION\n")
	fmt.Printf(" Topic                 : %s\n", b.Topic)
	fmt.Printf(" Partitions            : %d\n", b.Partitions)
	fmt.Printf(" Producers             : %d\n", b.NumProducers)
	fmt.Printf(" Total Messages        : %d\n", totalMessagesProduced)
	fmt.Printf(" Message Size          : %d bytes\n", b.MessageSize)

	fmt.Printf("\n🚀 TRANSMISSION MODE\n")
	if b.UseAsync {
		fmt.Printf(" Mode                  : Async (Max Inflight: %d)\n", b.MaxInflight)
	} else if b.BatchSize > 1 {
		fmt.Printf(" Mode                  : Batch (Size: %d, Linger: %dms)\n", b.BatchSize, b.LingerMS)
	} else {
		fmt.Printf(" Mode                  : Synchronous\n")
	}
	fmt.Printf(" Compression           : %v\n", b.EnableGzip)

	fmt.Printf("\n⏱️  PERFORMANCE METRICS\n")
	fmt.Printf(" Duration              : %v\n", producerDuration)
	fmt.Printf(" Producer Throughput   : %.2f msg/sec\n", producerThroughput)

	fmt.Printf("\n📋 ANALYSIS\n")
	avgMsgPerProducer := float64(b.MessagesPerProducer)
	fmt.Printf(" Messages per Producer : %.0f\n", avgMsgPerProducer)

	if producerDuration.Seconds() > 0 {
		avgProducerLatency := producerDuration.Seconds() / avgMsgPerProducer * 1000
		fmt.Printf(" Avg Producer Latency  : %.2f ms/msg\n", avgProducerLatency)
	}

	if b.NumProducers > 0 {
		throughputPerProducer := producerThroughput / float64(b.NumProducers)
		fmt.Printf(" Throughput per Producer: %.2f msg/sec\n", throughputPerProducer)
	}

	fmt.Printf("=====================================\n")
}

func (b *BenchmarkRunner) RunConcurrentProducerPhase() error {
	var pWg sync.WaitGroup
	var producerErrors []error
	var mu sync.Mutex

	for i := 0; i < b.NumProducers; i++ {
		pWg.Add(1)
		go func(pid int) {
			defer pWg.Done()
			client := &BenchClient{
				Addr:        b.Addr,
				EnableGzip:  b.EnableGzip,
				NumMessages: b.MessagesPerProducer,
				Topic:       b.Topic,
				Partitions:  b.Partitions,
			}

			if err := client.RunMessageProductionPhase(pid); err != nil {
				mu.Lock()
				producerErrors = append(producerErrors, fmt.Errorf("producer %d error: %w", pid, err))
				mu.Unlock()
			}
		}(i)
	}
	pWg.Wait()

	if len(producerErrors) > 0 {
		return fmt.Errorf("%d producer(s) failed, first error: %w", len(producerErrors), producerErrors[0])
	}
	return nil
}
