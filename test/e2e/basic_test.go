package e2e

import (
	"testing"
)

func TestBasicPubSub(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("test-topic").
		WithPartitions(4).
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

	ctx.
		WithTopic("config-test").
		WithPartitions(2).
		WithNumMessages(5).
		When().
		StartBroker().
		PublishMessages().
		Then().
		Expect(MessagesPublished(5))
}
