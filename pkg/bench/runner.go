package bench

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/util"
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

	util.Info("Initializing Topic '%s' with %d partitions...", b.Topic, b.Partitions)
	if err := initClient.RunTopicCreationPhase(); err != nil {
		util.Info("Topic Initialization error: %v", err)
		return
	}

	util.Info("Starting Producer Phase (%d Producers, %d Total Messages)", b.NumProducers, totalMessagesProduced)
	producerStart := time.Now()

	if err := b.RunConcurrentProducerPhase(); err != nil {
		util.Info("Concurrent Producer Phase error: %v", err)
		return
	}

	producerDuration := time.Since(producerStart)
	util.Info("Producer Phase Finished in %v", producerDuration)

	var consumerDuration time.Duration

	if b.NumConsumers > 0 {
		util.Info("Starting Consumer Phase (%d Consumers)", b.NumConsumers)
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
			util.Info("Consumer Phase error: %v", err)
		}

		consumerDuration = time.Since(consumerStart)
		util.Info("Consumer Phase Finished in %v", consumerDuration)
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

	util.Info("🧪 BENCHMARK RESULT [disk] 🧪")
	util.Info("=====================================")

	util.Info("📊 CONFIGURATION")
	util.Info(" Topic                 : %s", b.Topic)
	util.Info(" Partitions            : %d", b.Partitions)
	util.Info(" Producers             : %d", b.NumProducers)
	util.Info(" Consumers             : %d", b.NumConsumers)
	util.Info(" Total Messages        : %d", totalMessagesProduced)
	util.Info(" Message Size          : %d bytes", b.MessageSize)

	util.Info("🚀 TRANSMISSION MODE")
	if b.UseAsync && b.BatchSize > 1 {
		util.Info(" Mode                  : Async+Batch (Size: %d, Max Inflight: %d)", b.BatchSize, b.MaxInflight)
	} else if b.UseAsync {
		util.Info(" Mode                  : Async (Max Inflight: %d)", b.MaxInflight)
	} else if b.BatchSize > 1 {
		util.Info(" Mode                  : Batch (Size: %d, Linger: %dms)", b.BatchSize, b.LingerMS)
	} else {
		util.Info(" Mode                  : Synchronous")
	}
	util.Info(" Compression           : %v", b.EnableGzip)

	util.Info("⏱️  PERFORMANCE METRICS")
	util.Info(" Producer Duration     : %v", producerDuration)
	util.Info(" Consumer Duration     : %v", consumerDuration)
	util.Info(" Total Duration (P+C)  : %v", totalDuration)

	util.Info("📈 THROUGHPUT")
	util.Info(" Producer Throughput   : %.2f msg/sec", producerThroughput)
	if consumerThroughput > 0 {
		util.Info(" Consumer Throughput   : %.2f msg/sec", consumerThroughput)
	}
	util.Info(" Combined Throughput   : %.2f msg/sec", combinedThroughput)

	util.Info("📋 ANALYSIS")
	avgMsgPerProducer := float64(b.MessagesPerProducer)
	util.Info(" Messages per Producer : %.0f", avgMsgPerProducer)

	if producerDuration.Seconds() > 0 {
		avgProducerLatency := producerDuration.Seconds() / avgMsgPerProducer * 1000
		util.Info(" Avg Producer Latency  : %.2f ms/msg", avgProducerLatency)
	}

	if b.NumProducers > 0 {
		throughputPerProducer := producerThroughput / float64(b.NumProducers)
		util.Info(" Throughput per Producer: %.2f msg/sec", throughputPerProducer)
	}

	util.Info("=====================================")
}

func (b *BenchmarkRunner) printProduceOnlyResults(totalMessagesProduced int, producerDuration time.Duration) {
	producerThroughput := float64(totalMessagesProduced) / producerDuration.Seconds()

	util.Info("🧪 BENCHMARK RESULT [disk] (PRODUCE ONLY) 🧪")
	util.Info("=====================================")

	util.Info("📊 CONFIGURATION")
	util.Info(" Topic                 : %s", b.Topic)
	util.Info(" Partitions            : %d", b.Partitions)
	util.Info(" Producers             : %d", b.NumProducers)
	util.Info(" Total Messages        : %d", totalMessagesProduced)
	util.Info(" Message Size          : %d bytes", b.MessageSize)

	util.Info("🚀 TRANSMISSION MODE")
	if b.UseAsync {
		util.Info(" Mode                  : Async (Max Inflight: %d)", b.MaxInflight)
	} else if b.BatchSize > 1 {
		util.Info(" Mode                  : Batch (Size: %d, Linger: %dms)", b.BatchSize, b.LingerMS)
	} else {
		util.Info(" Mode                  : Synchronous")
	}
	util.Info(" Compression           : %v", b.EnableGzip)

	util.Info("⏱️  PERFORMANCE METRICS")
	util.Info(" Duration              : %v", producerDuration)
	util.Info(" Producer Throughput   : %.2f msg/sec", producerThroughput)

	util.Info("📋 ANALYSIS")
	avgMsgPerProducer := float64(b.MessagesPerProducer)
	util.Info(" Messages per Producer : %.0f", avgMsgPerProducer)

	if producerDuration.Seconds() > 0 {
		avgProducerLatency := producerDuration.Seconds() / avgMsgPerProducer * 1000
		util.Info(" Avg Producer Latency  : %.2f ms/msg", avgProducerLatency)
	}

	if b.NumProducers > 0 {
		throughputPerProducer := producerThroughput / float64(b.NumProducers)
		util.Info(" Throughput per Producer: %.2f msg/sec", throughputPerProducer)
	}

	util.Info("=====================================")
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
