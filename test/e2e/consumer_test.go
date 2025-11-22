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
