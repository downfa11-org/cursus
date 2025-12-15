package controller

import (
	"fmt"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/topic"
)

type ClusterController struct {
	raftManager *replication.RaftReplicationManager
	discovery   ServiceDiscovery

	topicManager *topic.TopicManager
}

func NewClusterController(rm *replication.RaftReplicationManager, sd ServiceDiscovery, tm *topic.TopicManager) *ClusterController {
	return &ClusterController{
		raftManager:  rm,
		discovery:    sd,
		topicManager: tm,
	}
}

func (cc *ClusterController) GetClusterLeader() (string, error) {
	if cc.raftManager != nil {
		raft := cc.raftManager.GetRaft()
		if raft != nil {
			leader := string(raft.Leader())
			if leader != "" {
				return leader, nil
			}
		}
	}
	return "", fmt.Errorf("no cluster leader available")
}
