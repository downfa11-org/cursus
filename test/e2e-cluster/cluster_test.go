package e2e_cluster

import (
	"testing"
)

// TestISRWithAllAcks tests ISR behavior with acks=all
func TestISRWithAllAcks(t *testing.T) {
	ctx := GivenClusterRestart(t).
		WithTopic("isr-test").
		WithPartitions(1).
		WithNumMessages(50).
		WithAcks("all").
		WithMinInSyncReplicas(2)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		SimulateFollowerFailure().
		Then().
		Expect(MessagesPublishedWithQuorum()).
		And(ISRMaintained())
}
