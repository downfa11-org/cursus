package e2e

import (
	"fmt"
	"time"
)

// Consequences represents test assertions (Then phase)
type Consequences struct {
	ctx *TestContext
}

// Expectation is a function that validates test outcomes
type Expectation func(*TestContext) error

func (c *Consequences) Expect(expectations ...Expectation) *Consequences {
	for _, expectation := range expectations {
		if err := expectation(c.ctx); err != nil {
			c.ctx.t.Error(err)
		}
	}
	return c
}

func (c *Consequences) And(expectations ...Expectation) *Consequences {
	return c.Expect(expectations...)
}

// Common expectations
func BrokerIsHealthy() Expectation {
	return func(ctx *TestContext) error {
		if err := CheckBrokerHealth(); err != nil {
			return fmt.Errorf("broker health check failed: %w", err)
		}
		return nil
	}
}

// MessagesPublished verifies the count of published messages.
func MessagesPublished(expected int) Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount != expected {
			return fmt.Errorf("expected %d messages published, got %d", expected, ctx.publishedCount)
		}
		return nil
	}
}

// MessagesConsumed verifies the count of consumed messages.
func MessagesConsumed(expected int) Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount != expected {
			return fmt.Errorf("expected %d messages consumed, got %d", expected, ctx.consumedCount)
		}
		return nil
	}
}

// PublisherRetriedSuccessfully verifies that at least some messages were published.
func PublisherRetriedSuccessfully() Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount == 0 {
			return fmt.Errorf("no messages were published")
		}
		return nil
	}
}

// MessagesPublishedSince verifies the count and ensures publication happened within a relevant timeframe.
func MessagesPublishedSince(expected int, since time.Time) Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount != expected {
			return fmt.Errorf("expected %d messages published, got %d", expected, ctx.publishedCount)
		}
		if ctx.startTime.Before(since) {
			return fmt.Errorf("publication started before expected time: started at %v, expected since %v", ctx.startTime, since)
		}
		return nil
	}
}

// OffsetsCommitted verifies that the offset for the partition is not 0
func OffsetsCommitted() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount == 0 {
			return fmt.Errorf("no messages consumed, cannot verify offset commit")
		}

		client := NewBrokerClient(ctx.brokerAddr)
		defer client.Close()

		for partition := 0; partition < ctx.partitions; partition++ {
			offset, err := client.FetchCommittedOffset(ctx.topic, partition, ctx.consumerGroup)
			if err != nil {
				return fmt.Errorf("failed to fetch committed offset for partition %d: %w", partition, err)
			}
			if offset == 0 {
				return fmt.Errorf("offset not committed for partition %d", partition)
			}
		}
		return nil
	}
}

// ConsumerGroupIsActive verifies the group status via broker API.
func ConsumerGroupIsActive() Expectation {
	return func(ctx *TestContext) error {
		client := NewBrokerClient(ctx.brokerAddr)
		defer client.Close()

		status, err := client.GetConsumerGroupStatus(ctx.consumerGroup)
		if err != nil {
			return fmt.Errorf("failed to get group status: %w", err)
		}

		if status.GroupName != ctx.consumerGroup {
			return fmt.Errorf("expected group name %s, got %s", ctx.consumerGroup, status.GroupName)
		}

		if status.MemberCount < 1 {
			return fmt.Errorf("expected at least 1 member in group %s, got %d", ctx.consumerGroup, status.MemberCount)
		}

		return nil
	}
}

func HeartbeatsSent() Expectation {
	return func(ctx *TestContext) error {
		client := NewBrokerClient(ctx.brokerAddr)
		defer client.Close()

		status, err := client.GetConsumerGroupStatus(ctx.consumerGroup)
		if err != nil {
			return fmt.Errorf("failed to get group status for %s: %w", ctx.consumerGroup, err)
		}

		if status.GroupName != ctx.consumerGroup {
			return fmt.Errorf("expected group name %s, got %s", ctx.consumerGroup, status.GroupName)
		}

		if status.MemberCount < 1 {
			return fmt.Errorf("expected at least 1 member in group %s, got %d. (Run ConsumeMessages first)", ctx.consumerGroup, status.MemberCount)
		}

		threshold := time.Now().Add(-5 * time.Second)
		for _, member := range status.Members {
			if member.MemberID == ctx.memberID {
				if member.LastHeartbeat.Before(threshold) {
					return fmt.Errorf("member %s last heartbeat was too old: %v", member.MemberID, member.LastHeartbeat)
				}
				ctx.t.Logf("Member %s last heartbeat verified: %v", member.MemberID, member.LastHeartbeat)
				return nil
			}
		}
		return fmt.Errorf("could not find E2E client member ID (%s) in group status", ctx.memberID)
	}
}
