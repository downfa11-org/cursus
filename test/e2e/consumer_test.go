package e2e

import (
	"testing"
)

// TestConsumerGroupJoin verifies consumer group join functionality
func TestConsumerGroupJoin(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("consumer-group-test").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

func TestWithDefaultConsumerGroup(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("default-group-test").
		WithDefaultConsumerGroup().
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

func TestWithCustomConsumerGroup(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("custom-group-test").
		WithConsumerGroup("my-custom-group").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

func TestDefaultGroupOffsetSharing(t *testing.T) {
	ctx1 := Given(t)
	defer ctx1.Cleanup()

	ctx1.WithTopic("shared-topic").
		WithDefaultConsumerGroup().
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))

	ctx2 := Given(t)

	ctx2.WithTopic("shared-topic").
		WithDefaultConsumerGroup().
		WithPartitions(1).
		When().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(0))
}

// TestConsumerOffsetCommit verifies offset commit functionality
func TestConsumerOffsetCommit(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("offset-test").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(OffsetsCommitted())
}

// TestConsumerHeartbeat verifies heartbeat mechanism
func TestConsumerHeartbeat(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("heartbeat-test").
		WithPartitions(1).
		WithNumMessages(5).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(HeartbeatsSent())
}
