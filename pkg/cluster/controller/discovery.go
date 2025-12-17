package controller

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/cluster/replication/fsm"
	"github.com/downfa11-org/go-broker/util"
)

type ServiceDiscovery struct {
	rm       *replication.RaftReplicationManager
	fsm      *fsm.BrokerFSM
	brokerID string
	addr     string
}

func NewServiceDiscovery(rm *replication.RaftReplicationManager, brokerID, addr string) ServiceDiscovery {
	return ServiceDiscovery{
		rm:       rm,
		fsm:      rm.GetFSM(),
		brokerID: brokerID,
		addr:     addr,
	}
}

func (sd *ServiceDiscovery) Register() error {
	broker := &fsm.BrokerInfo{
		ID:       sd.brokerID,
		Addr:     sd.addr,
		Status:   "active",
		LastSeen: time.Now(),
	}

	data, _ := json.Marshal(broker)
	if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
		util.Error("Failed to register broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully registered broker %s", sd.brokerID)
	return nil
}

func (sd *ServiceDiscovery) Deregister() error {
	if err := sd.rm.ApplyCommand("DEREGISTER", []byte(sd.brokerID)); err != nil {
		util.Error("Failed to deregister broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully deregistered broker %s via Raft", sd.brokerID)
	return nil
}

func (sd *ServiceDiscovery) DiscoverBrokers() ([]fsm.BrokerInfo, error) {
	brokers := sd.fsm.GetBrokers()
	return brokers, nil
}

func (sd *ServiceDiscovery) AddNode(nodeID string, addr string) (string, error) {
	leaderAddr := sd.rm.GetLeaderAddress()

	if !sd.rm.IsLeader() {
		return leaderAddr, fmt.Errorf("not leader; contact leader at %s", leaderAddr)
	}

	if err := sd.rm.AddVoter(nodeID, addr); err != nil {
		return leaderAddr, err
	}

	broker := &fsm.BrokerInfo{ID: nodeID, Addr: addr, Status: "active", LastSeen: time.Now()}
	data, _ := json.Marshal(broker)
	_ = sd.rm.ApplyCommand("REGISTER", data)

	return leaderAddr, nil
}

func (sd *ServiceDiscovery) RemoveNode(nodeID string) (string, error) {
	leaderAddr := sd.rm.GetLeaderAddress()

	if !sd.rm.IsLeader() {
		return leaderAddr, fmt.Errorf("not leader")
	}

	if err := sd.rm.RemoveServer(nodeID); err != nil {
		return leaderAddr, err
	}

	_ = sd.rm.ApplyCommand("DEREGISTER", []byte(nodeID))
	return leaderAddr, nil
}
