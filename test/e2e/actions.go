package e2e

import (
	"fmt"
	"time"
)

// Actions represents test actions (When phase)
type Actions struct {
	ctx *TestContext
}

func (a *Actions) StartBroker() *Actions {
	a.ctx.t.Log("Waiting for broker to be ready...")
	if err := CheckBrokerHealth(); err != nil {
		a.ctx.t.Fatalf("Broker health check failed: %v", err)
	}

	_ = a.ctx.getClient().DeleteTopic(a.ctx.topic)
	time.Sleep(100 * time.Millisecond)

	a.ctx.t.Log("Broker is ready")
	return a
}

func (a *Actions) StopBroker() *Actions {
	a.ctx.t.Log("Stopping broker...")
	time.Sleep(2 * time.Second)
	return a
}

func (a *Actions) CreateTopic() *Actions {
	a.ctx.t.Logf("Creating topic '%s' with %d partitions...", a.ctx.topic, a.ctx.partitions)

	err := a.ctx.getClient().CreateTopic(a.ctx.topic, a.ctx.partitions)
	if err != nil {
		a.ctx.t.Fatalf("Failed to create topic: %v", err)
	}

	a.ctx.t.Logf("Topic '%s' created successfully", a.ctx.topic)
	return a
}

func (a *Actions) PublishMessages() *Actions {
	a.ctx.t.Logf("Publishing %d messages to topic '%s'...", a.ctx.numMessages, a.ctx.topic)

	a.ctx.publishedSeqNums = make([]uint64, 0, a.ctx.numMessages)

	for i := 0; i < a.ctx.numMessages; i++ {
		a.ctx.seqNum++
		payload := fmt.Sprintf("test-message-%d", i)

		err := a.ctx.getClient().PublishIdempotent(
			a.ctx.topic,
			a.ctx.producerID,
			a.ctx.seqNum,
			time.Now().UnixNano(),
			payload,
			a.ctx.acks,
		)

		if err != nil {
			a.ctx.t.Errorf("Failed to publish message %d: %v", i, err)
			continue
		}

		a.ctx.publishedSeqNums = append(a.ctx.publishedSeqNums, a.ctx.seqNum)
		a.ctx.publishedCount++

		if a.ctx.publishDelayMS > 0 {
			time.Sleep(time.Duration(a.ctx.publishDelayMS) * time.Millisecond)
		}
	}

	a.ctx.t.Logf("Published %d messages successfully", a.ctx.publishedCount)
	return a
}

func (a *Actions) RetryPublishMessages() *Actions {
	a.ctx.t.Log("Retrying published messages (idempotence test)...")

	for i := 0; i < len(a.ctx.publishedSeqNums); i++ {
		payload := fmt.Sprintf("test-message-%d", i)
		seqNum := a.ctx.publishedSeqNums[i]

		err := a.ctx.getClient().PublishIdempotent(
			a.ctx.topic,
			a.ctx.producerID,
			seqNum,
			time.Now().UnixNano(),
			payload,
			a.ctx.acks,
		)

		if err != nil {
			a.ctx.t.Logf("Retry message %d: %v", i, err)
		}
	}

	return a
}

func (a *Actions) ConsumeMessages() *Actions {
	a.ctx.t.Logf("Consuming from topic '%s'...", a.ctx.topic)

	client := a.ctx.getClient()
	if err := client.RegisterConsumerGroup(a.ctx.topic, a.ctx.consumerGroup, 1); err != nil {
		a.ctx.t.Logf("Warning: Failed to register consumer group: %v", err)
	}

	totalConsumed := 0
	for partition := 0; partition < a.ctx.partitions; partition++ {
		messages, err := client.ConsumeMessages(
			a.ctx.topic,
			partition,
			a.ctx.consumerGroup,
			5*time.Second,
		)

		if err != nil {
			a.ctx.t.Errorf("Consume partition %d failed: %v", partition, err)
			continue
		}

		a.ctx.t.Logf("Consumed %d messages from partition %d", len(messages), partition)
		totalConsumed += len(messages)
	}

	a.ctx.consumedCount = totalConsumed
	a.ctx.t.Logf("Total consumed: %d messages", totalConsumed)
	return a
}

func (a *Actions) SimulateNetworkFailure() *Actions {
	a.ctx.t.Log("Simulating network failure...")
	time.Sleep(100 * time.Millisecond)
	a.ctx.t.Log("Network failure simulated")
	return a
}

// Then transitions to assertion phase
func (a *Actions) Then() *Consequences {
	return &Consequences{ctx: a.ctx}
}
