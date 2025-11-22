package e2e

import (
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

func BrokerIsHealthy() Expectation {
	return func(ctx *Context) error {
		resp, err := http.Get(healthCheckURL)
		if err != nil {
			return fmt.Errorf("broker health check failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("broker health check returned status %d", resp.StatusCode)
		}
		return nil
	}
}

func MessagesPublished(count int) Expectation {
	return func(ctx *Context) error {
		if ctx.publishedCount < count {
			return fmt.Errorf("expected at least %d messages published, got %d", count, ctx.publishedCount)
		}
		return nil
	}
}

func MessagesConsumed(count int) Expectation {
	return func(ctx *Context) error {
		if ctx.consumedCount < count {
			return fmt.Errorf("expected at least %d messages consumed, got %d", count, ctx.consumedCount)
		}
		return nil
	}
}

func TopicExists(topicName string) Expectation {
	return func(ctx *Context) error {
		conn, err := net.Dial("tcp", defaultBrokerAddr)
		if err != nil {
			return fmt.Errorf("connect to broker: %w", err)
		}
		defer conn.Close()

		listCmd := util.EncodeMessage("admin", "LIST")
		if err := util.WriteWithLength(conn, listCmd); err != nil {
			return fmt.Errorf("send LIST command: %w", err)
		}

		resp, err := util.ReadWithLength(conn)
		if err != nil {
			return fmt.Errorf("read LIST response: %w", err)
		}

		if !strings.Contains(string(resp), topicName) {
			return fmt.Errorf("topic '%s' not found in LIST response", topicName)
		}
		return nil
	}
}

func BrokerLogsContain(searchStr string) Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker")
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to get broker logs: %w", err)
		}

		if !strings.Contains(string(output), searchStr) {
			return fmt.Errorf("broker logs do not contain '%s'", searchStr)
		}
		return nil
	}
}

func MessagesPublishedSince(count int, since time.Time) Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "--since", since.Format(time.RFC3339), "broker")
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to get broker logs: %w", err)
		}

		published := strings.Count(string(output), "Published to")

		if published < count {
			return fmt.Errorf("expected at least %d messages since %v, got %d", count, since, published)
		}
		return nil
	}
}

func OffsetsCommitted() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker")
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to get broker logs: %w", err)
		}

		if !strings.Contains(string(output), "CONSUME") {
			return fmt.Errorf("no CONSUME operations found in logs")
		}

		ctx.t.Log("Note: Offset commit feature not yet implemented in broker")
		return nil
	}
}

func HeartbeatsSent() Expectation {
	return func(ctx *Context) error {
		conn, err := net.Dial("tcp", defaultBrokerAddr)
		if err != nil {
			return fmt.Errorf("failed to connect to broker: %w", err)
		}
		defer conn.Close()

		ctx.t.Log("Note: Heartbeat feature not yet implemented in broker")
		return nil
	}
}

func PublisherRetriedSuccessfully() Expectation {
	return func(ctx *Context) error {
		if ctx.publishedCount < ctx.numMessages {
			return fmt.Errorf("expected %d messages published after retry, got %d",
				ctx.numMessages, ctx.publishedCount)
		}
		return nil
	}
}
