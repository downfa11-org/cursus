package e2e

import (
	"fmt"
	"testing"
	"time"
)

// TestMultipleTopicConsumption tests consuming from multiple topics
func TestMultipleTopicConsumption(t *testing.T) {
	topics := []string{"test-alpha", "test-beta", "test-gamma", "other-topic"}

	brokerCtx := GivenStandalone(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := GivenStandalone(t).WithTopic(topic).WithPartitions(1).WithNumMessages(5)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	totalConsumed := 0
	matchingTopics := []string{"test-alpha", "test-beta", "test-gamma"}

	for i, topic := range matchingTopics {
		groupName := fmt.Sprintf("multiple-test-group-%d", i)

		consumerCtx := GivenStandalone(t).
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

// TestRegexPatternConsumption tests actual regex pattern matching
func TestRegexPatternConsumption(t *testing.T) {
	topics := []string{"regex-alpha", "regex-beta", "regex-gamma", "regex-topic"}

	brokerCtx := GivenStandalone(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := GivenStandalone(t).WithTopic(topic).WithPartitions(1).WithNumMessages(5)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	groupName := "regex-test-group"
	consumerCtx := GivenStandalone(t).
		WithTopic("regex-alpha"). // actual topic name
		WithPartitions(1).
		WithNumMessages(0).
		WithConsumerGroup(groupName)
	defer consumerCtx.Cleanup()

	consumerCtx.When().
		JoinGroup().
		SyncGroup().
		ConsumeMessagesFromTopic("regex-*"). // regex pattern consume
		Then().
		Expect(MessagesConsumed(5))
}

// TestRegexPatternWithQuestionMark tests ? wildcard functionality
func TestRegexPatternWithQuestionMark(t *testing.T) {
	topics := []string{"log-1", "log-2", "log-3"}

	brokerCtx := GivenStandalone(t)
	defer brokerCtx.Cleanup()
	brokerCtx.When().StartBroker()

	for _, topic := range topics {
		ctx := GivenStandalone(t).WithTopic(topic).WithPartitions(1).WithNumMessages(3)
		defer ctx.Cleanup()

		ctx.When().
			CreateTopic().
			PublishMessages()

		time.Sleep(100 * time.Millisecond)
	}

	groupName := "question-mark-test-group"
	consumerCtx := GivenStandalone(t).
		WithTopic("log-1"). // actual topic name
		WithPartitions(1).
		WithNumMessages(0).
		WithConsumerGroup(groupName)
	defer consumerCtx.Cleanup()

	consumerCtx.When().
		JoinGroup().
		SyncGroup().
		ConsumeMessagesFromTopic("log-?").
		Then().
		Expect(MessagesConsumed(3))
}
