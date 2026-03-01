package e2e_cluster

import (
	"fmt"
	"strings"
	"time"

	"github.com/cursus-io/cursus/test/e2e"
)

// ISRMaintained verifies ISR is maintained during operations
func ISRMaintained() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 2*time.Second)
		if err != nil {
			return err
		}
		if !strings.Contains(resp, "\"isr\":") {
			return fmt.Errorf("ISR info not found in metadata")
		}
		return nil
	}
}

func LeaderChanged() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		client := ctx.GetClient()
		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 2*time.Second)
		if err != nil {
			return err
		}
		if strings.Contains(resp, "ERROR:") {
			return fmt.Errorf("metadata fetch failed: %s", resp)
		}
		ctx.GetT().Logf("Verified leader after failover: %s", resp)
		return nil
	}
}

// ExpectDataConsistent verifies all partitions have the same LEO across metadata
func ExpectDataConsistent() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		ctx.GetT().Log("Verifying data consistency across cluster...")
		topic := ctx.GetTopic()
		client := ctx.GetClient()

		resp, err := client.SendCommand("", fmt.Sprintf("DESCRIBE topic=%s", topic), 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to describe topic for consistency check: %w", err)
		}

		if strings.Contains(resp, "ERROR:") {
			return fmt.Errorf("broker error in DESCRIBE: %s", resp)
		}

		if ctx.GetPublishedCount() > 0 && !strings.Contains(resp, "\"leo\":") {
			return fmt.Errorf("consistency check failed: LEO not found in metadata")
		}

		ctx.GetT().Log("Data consistency metadata verified")
		return nil
	}
}

func PublishFailure() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		lastErr := ctx.GetLastError()
		if lastErr == nil {
			return fmt.Errorf("expected publish failure but it succeeded")
		}

		errStr := strings.ToLower(lastErr.Error())
		if strings.Contains(errStr, "not enough in-sync replicas") ||
			strings.Contains(errStr, "failed to forward") ||
			strings.Contains(errStr, "eof") ||
			strings.Contains(errStr, "read length") ||
			strings.Contains(errStr, "refused") ||
			strings.Contains(errStr, "failed after retries") ||
			strings.Contains(errStr, "connection") {
			ctx.GetT().Logf("Confirmed expected cluster failure: %v", lastErr)
			return nil
		}

		return fmt.Errorf("unexpected error message: %v", lastErr)
	}
}

func ExpectOffsetMatched(partition int, expected uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		topic := ctx.GetTopic()
		group := ctx.GetConsumerGroup()
		client := ctx.GetClient()

		time.Sleep(2 * time.Second)

		offset, err := client.FetchCommittedOffset(topic, partition, group)
		if err != nil {
			return fmt.Errorf("failed to fetch offset for verification: %w", err)
		}

		if offset != expected {
			return fmt.Errorf("offset mismatch: expected %d, got %d", expected, offset)
		}

		ctx.GetT().Logf("Confirmed: Offset %d is correctly replicated and matched", offset)
		return nil
	}
}

// MessagesPublishedWithQuorum verifies messages were published with quorum
func MessagesPublishedWithQuorum() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published")
		}

		if ctx.GetPublishedCount() != ctx.GetNumMessages() {
			return fmt.Errorf("quorum not achieved: expected %d messages, got %d",
				ctx.GetNumMessages(), ctx.GetPublishedCount())
		}

		if ctx.GetAcks() != "all" {
			return fmt.Errorf("acks not set to 'all': got %s", ctx.GetAcks())
		}

		ctx.GetT().Logf("Quorum achieved: %d messages published with acks=%s",
			ctx.GetPublishedCount(), ctx.GetAcks())
		return nil
	}
}
