package controller

import (
	"context"
	"fmt"

	"github.com/downfa11-org/cursus/pkg/cluster/replication"
	"github.com/downfa11-org/cursus/pkg/config"
	"github.com/downfa11-org/cursus/util"
)

type ClusterController struct {
	RaftManager *replication.RaftReplicationManager
	Discovery   *ServiceDiscovery
	Election    *ControllerElection
	Router      *ClusterRouter
}

func NewClusterController(ctx context.Context, cfg *config.Config, rm *replication.RaftReplicationManager, sd *ServiceDiscovery) *ClusterController {
	brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.BrokerPort)

	cc := &ClusterController{
		RaftManager: rm,
		Discovery:   sd,
		Election:    NewControllerElection(rm),
		Router:      NewClusterRouter(brokerID, localAddr, nil, rm, cfg.BrokerPort),
	}

	return cc
}

func (cc *ClusterController) Start(ctx context.Context) {
	cc.Election.Start()
	cc.Discovery.StartReconciler(ctx)
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
	util.Warn("RaftManager is nil, assuming non-leader state")
	return false
}

// todo. (issues #27) Delegate authorization to partition-level leader checks in future releases.
func (cc *ClusterController) IsAuthorized(topic string, partition int) bool {
	return cc.IsLeader()
}
