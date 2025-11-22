package e2e

import (
	"testing"
)

func TestBasicPubSub(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("test-topic").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(BrokerIsHealthy()).
		And(MessagesPublished(10)).
		And(MessagesConsumed(10))
}

func TestConfigValidation(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("config-test").
		WithPartitions(1).
		WithNumMessages(5).
		When().
		StartBroker().
		PublishMessages().
		Then().
		Expect(MessagesPublishedSince(5, ctx.startTime))
}
