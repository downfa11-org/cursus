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
		CreateTopic().
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
		CreateTopic().
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
		CreateTopic().
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
		CreateTopic().
		PublishMessages().
		SimulateNetworkFailure().
		RetryPublishMessages().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(5))
}

// TestPublisherAcks verifies different ACK modes
func TestPublisherAcks(t *testing.T) {
	testCases := []struct {
		name string
		acks string
	}{
		{"acks=0", "0"},
		{"acks=1", "1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := Given(t)
			defer ctx.Cleanup()

			ctx.WithTopic("acks-test-" + tc.acks).
				WithPartitions(1).
				WithNumMessages(5).
				WithAcks(tc.acks).
				When().
				StartBroker().
				CreateTopic().
				PublishMessages().
				Then().
				Expect(MessagesPublished(5))
		})
	}
}
