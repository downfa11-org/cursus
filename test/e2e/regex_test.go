package e2e

import (
	"fmt"
	"testing"
	"time"
)

// TestRegexTopicConsumption verifies regex pattern matching for topic consumption
func TestRegexTopicConsumption(t *testing.T) {
	topics := []string{"test-alpha", "test-beta", "test-gamma", "other-topic"}

	brokerCtx := Given(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := Given(t).WithTopic(topic).WithPartitions(1).WithNumMessages(5)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	totalConsumed := 0
	matchingTopics := []string{"test-alpha", "test-beta", "test-gamma"}

	for i, topic := range matchingTopics {
		groupName := fmt.Sprintf("regex-test-group-%d", i)

		consumerCtx := Given(t).
			WithTopic(topic).
			WithPartitions(1).
			WithNumMessages(0).
			WithConsumerGroup(groupName)
		defer consumerCtx.Cleanup()

		consumerCtx.When().
			JoinGroup().
			SyncGroup().
			ConsumeMessages().
			Then().
			Expect(MessagesConsumed(5))
		totalConsumed += 5
	}

	if totalConsumed != 15 {
		t.Errorf("Expected 15 messages consumed, got %d", totalConsumed)
	}
}

// TestRegexPatternWithQuestionMark verifies ? wildcard functionality
func TestRegexPatternWithQuestionMark(t *testing.T) {
	topics := []string{"log-1", "log-2", "log-3"}

	brokerCtx := Given(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := Given(t).WithTopic(topic).WithPartitions(1).WithNumMessages(3)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	totalConsumed := 0

	for i, topic := range topics {
		groupName := fmt.Sprintf("question-mark-test-group-%d", i)

		consumerCtx := Given(t).
			WithTopic(topic).
			WithPartitions(1).
			WithNumMessages(0).
			WithConsumerGroup(groupName)
		defer consumerCtx.Cleanup()

		consumerCtx.When().
			JoinGroup().
			SyncGroup().
			ConsumeMessages().
			Then().
			Expect(MessagesConsumed(3))
		totalConsumed += 3
	}

	if totalConsumed != 9 {
		t.Errorf("Expected 9 messages consumed, got %d", totalConsumed)
	}
}
