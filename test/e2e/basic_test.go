package e2e

import (
	"testing"
)

// TestBasicPubSub verifies basic publish-subscribe functionality
func TestBasicPubSub(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("test-topic").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(BrokerIsHealthy(StandAloneHealthCheckAddr)).
		And(MessagesPublished(10)).
		And(MessagesConsumed(10))
}

// TestConfigValidation verifies configuration validation
func TestConfigValidation(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("config-test").
		WithPartitions(1).
		WithNumMessages(5).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		Then().
		Expect(MessagesPublishedSince(5, ctx.startTime))
}

// TestMultiPartition verifies multi-partition behavior
func TestMultiPartition(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("multi-partition-test").
		WithPartitions(4).
		WithNumMessages(20).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesPublished(20)).
		And(MessagesConsumed(20))
}
