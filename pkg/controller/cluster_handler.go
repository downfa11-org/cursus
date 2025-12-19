package controller

import (
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

func (ch *CommandHandler) ProcessCommand(cmd string) string {
	ctx := NewClientContext("default-group", 0)
	return ch.HandleCommand(cmd, ctx)
}

func (ch *CommandHandler) isAuthorizedForPartition(topic string, partition int) bool {
	if ch.Cluster == nil {
		return true
	}
	return ch.Cluster.IsAuthorized(topic, partition)
}

// todo. isLeaderAndForward checks if the current node is the cluster leader
func (ch *CommandHandler) isLeaderAndForward(cmd string) (string, bool, error) {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.RaftManager == nil {
		return "", false, nil
	}

	if !ch.Cluster.RaftManager.IsLeader() {
		if ch.Cluster.Router == nil {
			return "ERROR: not the leader, and router is nil", true, nil
		}

		encodedCmd := string(util.EncodeMessage("", cmd))
		const (
			maxRetries = 3
			retryDelay = 200 * time.Millisecond
		)

		for i := 0; i < maxRetries; i++ {
			resp, err := ch.Cluster.Router.ForwardToLeader(encodedCmd)
			if err == nil {
				return resp, true, nil
			}
			util.Debug("Retrying forward to leader... attempt %d", i+1)
			time.Sleep(retryDelay)
		}
		leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
		return fmt.Sprintf("ERROR: failed to forward command to leader (Leader: %s)", leaderAddr), true, nil
	}
	return "", false, nil
}
