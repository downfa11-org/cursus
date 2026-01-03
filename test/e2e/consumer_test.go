package e2e

import (
	"fmt"
	"testing"
	"time"
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
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))
}

// TestWithDefaultConsumerGroup verifies default consumer group behavior
func TestWithDefaultConsumerGroup(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("default-group-test").
		WithDefaultConsumerGroup().
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
		Expect(MessagesConsumed(10))
}

// TestWithCustomConsumerGroup verifies custom consumer group behavior
func TestWithCustomConsumerGroup(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("custom-group-test").
		WithConsumerGroup("my-custom-group").
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
		Expect(MessagesConsumed(10))
}

// TestDefaultGroupOffsetSharing verifies offset sharing between consumers in same group
func TestDefaultGroupOffsetSharing(t *testing.T) {
	ctx1 := Given(t)
	defer ctx1.Cleanup()

	sharedGroup := fmt.Sprintf("shared-group-%d", time.Now().UnixNano())

	ctx1.WithTopic("shared-topic").
		WithPartitions(1).
		WithNumMessages(10).
		WithConsumerGroup(sharedGroup).
		When().
		StartBroker().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(MessagesConsumed(10))

	ctx2 := Given(t)
	defer ctx2.Cleanup()

	ctx2.WithTopic("shared-topic").
		WithPartitions(1).
		WithConsumerGroup(sharedGroup).
		When().
		JoinGroup().
		SyncGroup().
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
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		CommitOffset(0, 10).
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
		CreateTopic().
		PublishMessages().
		JoinGroup().
		SyncGroup().
		ConsumeMessages().
		Then().
		Expect(HeartbeatsSent())
}

// TestConsumerOptionsComprehensive tests all consumer configuration options
func TestConsumerOptionsComprehensive(t *testing.T) {
	testCases := []struct {
		name              string
		group             string
		sessionTimeoutMS  int
		heartbeatInterval int
		autoCommit        bool
		expectedBehavior  string
	}{
		{"default_group", "", 10000, 3000, true, "normal"},
		{"custom_group", "test-group", 5000, 1000, false, "manual_commit"},
		{"short_timeout", "", 1000, 500, true, "rebalance"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := Given(t).
				WithTopic("consumer-options-" + tc.name).
				WithPartitions(2).
				WithNumMessages(30)

			if tc.group != "" {
				ctx = ctx.WithConsumerGroup(tc.group)
			}
			defer ctx.Cleanup()

			ctx.When().
				StartBroker().
				CreateTopic().
				PublishMessages().
				JoinGroup().
				SyncGroup().
				ConsumeMessages().
				CommitAllOffsets(30).
				Then().
				Expect(MessagesConsumed(30)).
				And(OffsetsCommitted())
		})
	}
}
