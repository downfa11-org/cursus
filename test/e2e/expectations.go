package e2e

import (
	"fmt"
	"os/exec"
	"strings"
)

// BrokerIsHealthy verifies the broker health check endpoint responds successfully
func BrokerIsHealthy() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("curl", "-f", "http://localhost:9080/health")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("broker health check failed: %w", err)
		}
		return nil
	}
}

// MessagesPublished verifies the expected number of messages were published
func MessagesPublished(count int) Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-publisher")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		lines := strings.Split(string(output), "\n")
		published := 0
		for _, line := range lines {
			if strings.Contains(line, "Message ") && strings.Contains(line, "published successfully") {
				published++
			}
		}
		if published != count {
			return fmt.Errorf("expected %d messages, got %d", count, published)
		}
		return nil
	}
}

// MessagesConsumed verifies that messages were consumed by checking consumer logs
func MessagesConsumed(count int) Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		if !strings.Contains(string(output), "Partition") {
			return fmt.Errorf("no messages consumed")
		}
		return nil
	}
}

// PublisherRetriedSuccessfully verifies publisher retry attempts in logs
func PublisherRetriedSuccessfully() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-publisher")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		if !strings.Contains(string(output), "Attempt") {
			return fmt.Errorf("no retry attempts found in publisher logs")
		}
		return nil
	}
}

// ConsumerJoinedGroup verifies consumer joined the group successfully
func ConsumerJoinedGroup() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		if !strings.Contains(string(output), "Joined group") {
			return fmt.Errorf("consumer did not join group")
		}
		return nil
	}
}

// OffsetsCommitted verifies offsets were committed
func OffsetsCommitted() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		if !strings.Contains(string(output), "commit") {
			return fmt.Errorf("no offset commits found")
		}
		return nil
	}
}

// HeartbeatsSent verifies heartbeats were sent
func HeartbeatsSent() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		if !strings.Contains(string(output), "Heartbeat") {
			return fmt.Errorf("no heartbeats found in consumer logs")
		}
		return nil
	}
}
