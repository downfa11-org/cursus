package e2e_cluster

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/downfa11-org/go-broker/test/e2e"
)

const (
	composeFile    = "docker-compose.yml"
	baseBrokerPort = 9000
	baseHealthPort = 9080
)

// ClusterTestContext extends e2e.TestContext for cluster testing
type ClusterTestContext struct {
	*e2e.TestContext
	clusterSize       int
	minInSyncReplicas int
	stopPublish       chan struct{}
}

func brokerPort(nodeIndex int) int {
	return baseBrokerPort + nodeIndex // nodeIndex: 1-based
}

func healthPort(nodeIndex int) int {
	return baseHealthPort + nodeIndex // nodeIndex: 1-based
}

func clusterBrokerAddrs(size int) []string {
	addrs := make([]string, 0, size)
	for i := 1; i <= size; i++ {
		addrs = append(addrs,
			fmt.Sprintf("localhost:%d", brokerPort(i)),
		)
	}
	return addrs
}

func clusterHealthCheckAddrs(size int) []string {
	addrs := make([]string, 0, size)
	for i := 1; i <= size; i++ {
		addrs = append(addrs,
			fmt.Sprintf("http://localhost:%d/health", healthPort(i)),
		)
	}
	return addrs
}

// GivenCluster creates a new cluster test context
func GivenCluster(t *testing.T) *ClusterTestContext {
	ctx := e2e.Given(t)

	clusterSize := 3
	ctx.SetBrokerAddrs(clusterBrokerAddrs(clusterSize))

	return &ClusterTestContext{
		TestContext:       ctx,
		clusterSize:       clusterSize,
		minInSyncReplicas: 2,
		stopPublish:       make(chan struct{}),
	}
}

func GivenClusterRestart(t *testing.T) *ClusterTestContext {
	cmd := exec.Command("docker-compose", "-f", composeFile, "up", "-d")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start docker-compose: %v\nOutput: %s", err, string(output))
	}

	t.Cleanup(func() {
		cmd := exec.Command("docker-compose", "-f", composeFile, "down", "-v")

		if err := cmd.Run(); err != nil {
			t.Logf("Cleanup warning: failed to bring down docker-compose: %v", err)
		}
	})

	ctx := GivenCluster(t)
	t.Logf("Waiting for all %d nodes to be healthy...", ctx.clusterSize)

	actions := ctx.WhenCluster()
	if err := actions.checkAllNodesHealth(); err != nil {
		t.Fatalf("Cluster failed to stabilize within timeout: %v", err)
	}

	return ctx
}

func (c *ClusterTestContext) WithTopic(topic string) *ClusterTestContext {
	c.TestContext.WithTopic(topic)
	return c
}

func (c *ClusterTestContext) WithPartitions(partitions int) *ClusterTestContext {
	c.TestContext.WithPartitions(partitions)
	return c
}

func (c *ClusterTestContext) WithNumMessages(num int) *ClusterTestContext {
	c.TestContext.WithNumMessages(num)
	return c
}

func (c *ClusterTestContext) WithAcks(acks string) *ClusterTestContext {
	c.TestContext.WithAcks(acks)
	return c
}

func (c *ClusterTestContext) WithClusterSize(size int) *ClusterTestContext {
	c.clusterSize = size
	c.TestContext.SetBrokerAddrs(clusterBrokerAddrs(size))
	return c
}

func (c *ClusterTestContext) WithMinInSyncReplicas(min int) *ClusterTestContext {
	c.minInSyncReplicas = min
	return c
}
