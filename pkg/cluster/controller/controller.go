package controller

import (
	"fmt"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/config"
)

type ClusterController struct {
	RaftManager *replication.RaftReplicationManager
	Discovery   *ServiceDiscovery
	Election    *ControllerElection
	Router      *ClusterRouter
}

func NewClusterController(cfg *config.Config, rm *replication.RaftReplicationManager, sd *ServiceDiscovery) *ClusterController {
	brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.BrokerPort)

	cc := &ClusterController{
		RaftManager: rm,
		Discovery:   sd,
		Election:    NewControllerElection(rm),
		Router:      NewClusterRouter(brokerID, localAddr, nil, rm, cfg.BrokerPort),
	}
	cc.Start()
	return cc
}

func (cc *ClusterController) Start() {
	cc.Election.Start()
}

func (cc *ClusterController) GetClusterLeader() (string, error) {
	leader := cc.RaftManager.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no cluster leader available")
	}
	return leader, nil
}

func (cc *ClusterController) JoinNewBroker(id, addr string) error {
	_, err := cc.Discovery.AddNode(id, addr)
	return err
}

func (cc *ClusterController) IsLeader() bool {
	if cc.RaftManager != nil {
		return cc.RaftManager.IsLeader()
	}
	return true
}

// todo. Delegate authorization to partition-level leader checks in future releases.
func (cc *ClusterController) IsAuthorized(topic string, partition int) bool {
	return cc.IsLeader()
}
