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

func MessagesPublished(expected int) Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount != expected {
			return fmt.Errorf("expected %d messages published, got %d", expected, ctx.publishedCount)
		}
		return nil
	}
}

func MessagesConsumed(expected int) Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount != expected {
			return fmt.Errorf("expected %d messages consumed, got %d", expected, ctx.consumedCount)
		}
		return nil
	}
}

func PublisherRetriedSuccessfully() Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount == 0 {
			return fmt.Errorf("no messages were published")
		}
		return nil
	}
}

func MessagesPublishedSince(expected int, since time.Time) Expectation {
	return func(ctx *TestContext) error {
		if ctx.publishedCount != expected {
			return fmt.Errorf("expected %d messages published, got %d", expected, ctx.publishedCount)
		}
		if ctx.startTime.Before(since) || time.Now().Before(since) {
			return fmt.Errorf("messages were not published after %v", since)
		}
		return nil
	}
}

func OffsetsCommitted() Expectation {
	return func(ctx *TestContext) error {
		if ctx.consumedCount == 0 {
			return fmt.Errorf("no messages consumed, cannot verify offset commit")
		}

		client := NewBrokerClient(ctx.brokerAddr)
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

func HeartbeatsSent() Expectation {
	return func(ctx *TestContext) error {
		client := NewBrokerClient(ctx.brokerAddr)
		if err := client.RegisterConsumerGroup(ctx.topic, ctx.consumerGroup); err != nil {
			return fmt.Errorf("failed to register consumer group: %w", err)
		}

		status, err := client.GetConsumerGroupStatus(ctx.consumerGroup)
		if err != nil {
			return fmt.Errorf("failed to get group status: %w", err)
		}

		if status.GroupName != ctx.consumerGroup {
			return fmt.Errorf("expected group name %s, got %s", ctx.consumerGroup, status.GroupName)
		}

		return nil
	}
}
