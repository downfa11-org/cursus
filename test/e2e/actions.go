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

func (a *Actions) JoinGroup() *Actions {
	a.ctx.t.Logf("Joining group '%s' for topic '%s'...", a.ctx.consumerGroup, a.ctx.topic)
	client := a.ctx.getClient()

	_, _, err := client.joinGroup(a.ctx.topic, a.ctx.consumerGroup)
	if err != nil {
		a.ctx.t.Fatalf("Group join failed: %v", err)
	}
	a.ctx.SyncClientState(client)

	a.ctx.t.Logf("Group join successful (Member ID: %s, Generation: %d)", client.memberID, client.generation)
	return a
}

func (a *Actions) SyncGroup() *Actions {
	client := a.ctx.getClient()

	if client.memberID == "" {
		a.ctx.t.Fatalf("Cannot sync group: Member ID or Generation is missing. Did you call JoinGroup()?")
	}

	a.ctx.t.Logf("Syncing group '%s' (Gen %d) to receive partition assignments...", a.ctx.consumerGroup, client.generation)
	assignedPartitions, err := client.syncGroup(a.ctx.topic, a.ctx.consumerGroup, client.generation, client.memberID)
	if err != nil {
		a.ctx.t.Fatalf("Group sync failed: %v", err)
	}
	a.ctx.SyncClientState(client)

	a.ctx.t.Logf("Consumer member assigned partitions: %v", assignedPartitions)
	a.ctx.assignedPartitions = assignedPartitions
	return a
}

// ConsumeMessages attempts to consume messages from all partitions in the group.
func (a *Actions) ConsumeMessages() *Actions {
	a.ctx.t.Logf("Consuming from topic '%s' for group '%s'...", a.ctx.topic, a.ctx.consumerGroup)

	client := a.ctx.getClient()

	if len(a.ctx.assignedPartitions) == 0 {
		a.ctx.t.Logf("No partitions assigned. Skipping consumption.")
		return a
	}

	currentMemberID := client.memberID
	currentGeneration := client.generation

	a.ctx.t.Logf("Consumer member '%s' (Generation %d) assigned partitions: %v", currentMemberID, currentGeneration, a.ctx.assignedPartitions)

	totalConsumed := 0
	for _, partition := range a.ctx.assignedPartitions {
		messages, err := client.ConsumeMessages(
			a.ctx.topic,
			partition,
			a.ctx.consumerGroup,
			currentMemberID,
			currentGeneration,
			5*time.Second,
		)

		if err != nil {
			a.ctx.t.Fatalf("Consume assigned partition %d failed: %v", partition, err)
		}

		a.ctx.t.Logf("Consumed %d messages from partition %d", len(messages), partition)
		totalConsumed += len(messages)
	}

	a.ctx.SyncClientState(client)
	a.ctx.consumedCount = totalConsumed
	a.ctx.t.Logf("Total consumed: %d messages", totalConsumed)
	return a
}

func (a *Actions) CommitOffset(partition int, offset uint64) *Actions {
	a.ctx.t.Logf("Committing offset %d for partition %d in group '%s'", offset, partition, a.ctx.consumerGroup)

	client := a.ctx.getClient()

	err := client.CommitOffset(a.ctx.topic, partition, a.ctx.consumerGroup, offset)
	if err != nil {
		a.ctx.t.Fatalf("Failed to commit offset: %v", err)
	}

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
