package e2e

import (
	"testing"
	"time"
)

// TestRegexTopicConsumption verifies regex pattern matching for topic consumption
func TestRegexTopicConsumption(t *testing.T) {
	// Create multiple test contexts for different topics
	topics := []string{"test-alpha", "test-beta", "test-gamma", "other-topic"}

	// Create topics and publish messages
	for _, topic := range topics {
		ctx := Given(t).WithTopic(topic).WithPartitions(1).WithNumMessages(5)
		defer ctx.Cleanup()

		ctx.When().
			StartBroker().
			CreateTopic().
			PublishMessages()

		// Wait a bit between topic creations
		time.Sleep(100 * time.Millisecond)
	}

	// Now test regex pattern consumption
	consumerCtx := Given(t).
		WithTopic("test-*"). // Regex pattern to match all test-* topics
		WithPartitions(1).
		WithNumMessages(0). // Not publishing, just consuming
		WithConsumerGroup("regex-test-group")
	defer consumerCtx.Cleanup()

	consumerCtx.When().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(15)) // 5 messages × 3 matching topics
}

// TestExactTopicNameStillWorks verifies backward compatibility with exact topic names
func TestExactTopicNameStillWorks(t *testing.T) {
	ctx := Given(t).
		WithTopic("exact-topic-name").
		WithPartitions(1).
		WithNumMessages(10)
	defer ctx.Cleanup()

	ctx.When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

// TestRegexPatternWithQuestionMark verifies ? wildcard functionality
func TestRegexPatternWithQuestionMark(t *testing.T) {
	topics := []string{"log-1", "log-2", "log-3"}

	// Create topics and publish messages
	for _, topic := range topics {
		ctx := Given(t).WithTopic(topic).WithPartitions(1).WithNumMessages(3)
		defer ctx.Cleanup()

		ctx.When().
			StartBroker().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	// Test ? wildcard pattern
	consumerCtx := Given(t).
		WithTopic("log-?"). // Should match log-1, log-2, log-3
		WithPartitions(1).
		WithNumMessages(0).
		WithConsumerGroup("question-mark-test-group")
	defer consumerCtx.Cleanup()

	consumerCtx.When().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(9)) // 3 messages × 3 matching topics
}
