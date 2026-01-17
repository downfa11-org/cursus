package e2e_cluster

import (
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/downfa11-org/cursus/test/e2e"
)

// ClusterActions represents cluster-specific test actions
type ClusterActions struct {
	ctx     *ClusterTestContext
	actions *e2e.Actions
}

func (c *ClusterTestContext) WhenCluster() *ClusterActions {
	base := c.When()
	return &ClusterActions{
		ctx:     c,
		actions: base,
	}
}

func (a *ClusterActions) StartCluster() *ClusterActions {
	a.ctx.GetT().Log("Cluster sync verified")
	return a
}

// waitForNodeHealth checks if a node is healthy
func (a *ClusterActions) waitForNodeHealth(nodeIndex int, healthUrl string) error {
	a.ctx.GetT().Logf("Waiting for node %d health check", nodeIndex)

	for retry := 0; retry < 30; retry++ {
		resp, err := http.Get(healthUrl)
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			a.ctx.GetT().Logf("Node %d is healthy", nodeIndex)
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("node %d failed to become healthy at %s after 30 retries", nodeIndex, healthUrl)
}

// checkAllNodesHealth verifies all cluster nodes are healthy
func (a *ClusterActions) checkAllNodesHealth() error {
	healthAddrs := clusterHealthCheckAddrs(a.ctx.clusterSize)

	for i, addr := range healthAddrs {
		if err := a.waitForNodeHealth(i+1, addr); err != nil {
			return err
		}
	}
	return nil
}

func (a *ClusterActions) CreateTopic() *ClusterActions {
	a.actions.CreateTopic()
	return a
}

func (a *ClusterActions) PublishMessages() *ClusterActions {
	a.actions.PublishMessages()
	return a
}

func (a *ClusterActions) JoinGroup() *ClusterActions {
	a.actions.JoinGroup()
	return a
}

func (a *ClusterActions) SyncGroup() *ClusterActions {
	a.actions.SyncGroup()
	return a
}

func (a *ClusterActions) ConsumeMessages() *ClusterActions {
	a.actions.ConsumeMessages()
	return a
}

func (a *ClusterActions) Then() *e2e.Consequences {
	return a.actions.Then()
}

func (a *ClusterActions) SimulateFollowerFailure(nodeIndex int) *ClusterActions {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("Invalid nodeIndex %d for failure simulation: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}

	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	a.ctx.GetT().Log("Simulating follower failure")

	cmd := exec.Command("docker", "stop", containerName)
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to stop follower: %v", err)
		return a
	}

	a.ctx.GetT().Logf("Successfully stopped %s", containerName)
	time.Sleep(2 * time.Second)
	return a
}

func (a *ClusterActions) RecoverFollower(nodeIndex int) *ClusterActions {
	if nodeIndex <= 0 || nodeIndex > a.ctx.clusterSize {
		a.ctx.GetT().Fatalf("Invalid nodeIndex %d: cluster size is %d", nodeIndex, a.ctx.clusterSize)
	}

	containerName := fmt.Sprintf("broker-%d", nodeIndex)
	a.ctx.GetT().Log("Recovering follower")

	cmd := exec.Command("docker", "start", containerName)
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Fatalf("Failed to recover follower: %v", err)
	}

	healthAddrs := clusterHealthCheckAddrs(a.ctx.clusterSize)
	if err := a.waitForNodeHealth(nodeIndex, healthAddrs[nodeIndex-1]); err != nil {
		a.ctx.GetT().Fatalf("node health check failed: %v", err)
	}
	return a
}
