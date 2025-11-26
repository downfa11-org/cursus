package e2e

import (
	"testing"
)

// TestPublisherConfigOptions verifies publisher configuration options work correctly
func TestPublisherConfigOptions(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("publisher-test").
		WithPartitions(2).
		WithNumMessages(5).
		When().
		StartBroker().
		PublishMessages().
		Then().
		Expect(BrokerIsHealthy()).
		And(MessagesPublished(5))
}

// TestPublisherRetryLogic verifies publisher retry mechanism
func TestPublisherRetryLogic(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("retry-test").
		WithPartitions(1).
		WithNumMessages(5).
		When().
		StartBroker().
		StopBroker().
		StartBroker().
		PublishMessages().
		Then().
		Expect(PublisherRetriedSuccessfully())
}

// TestExactlyOnceSemantics verifies exactly-once delivery with retries
func TestExactlyOnceSemantics(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("exactly-once-test").
		WithPartitions(1).
		WithNumMessages(10).
		When().
		StartBroker().
		PublishMessages().
		RetryPublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

// TestIdempotentProducer verifies idempotent producer behavior
func TestIdempotentProducer(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("idempotent-test").
		WithPartitions(1).
		WithNumMessages(5).
		When().
		StartBroker().
		PublishMessages().
		SimulateNetworkFailure().
		RetryPublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(5))
}
