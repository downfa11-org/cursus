package discovery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

const defaultRaftApplyTimeout = 5 * time.Second

type ServiceDiscovery interface {
	Register() error
	DiscoverBrokers() ([]replication.BrokerInfo, error)
	Deregister() error
}

type serviceDiscovery struct {
	fsm      *replication.BrokerFSM
	brokerID string
	addr     string
	raft     *raft.Raft
}

func NewServiceDiscovery(fsm *replication.BrokerFSM, brokerID, addr string, raft *raft.Raft) ServiceDiscovery {
	return &serviceDiscovery{
		fsm:      fsm,
		brokerID: brokerID,
		addr:     addr,
		raft:     raft,
	}
}

func (sd *serviceDiscovery) Register() error {
	broker := &replication.BrokerInfo{
		ID:       sd.brokerID,
		Addr:     sd.addr,
		Status:   "active",
		LastSeen: time.Now(),
	}

	util.Info("Registering broker %s at %s", sd.brokerID, sd.addr)
	data, _ := json.Marshal(broker)
	future := sd.raft.Apply([]byte(fmt.Sprintf("REGISTER:%s", string(data))), defaultRaftApplyTimeout)

	if err := future.Error(); err != nil {
		util.Error("Failed to register broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully registered broker %s", sd.brokerID)
	return nil
}

func (sd *serviceDiscovery) DiscoverBrokers() ([]replication.BrokerInfo, error) {
	brokers := sd.fsm.GetBrokers()
	util.Debug("Discovered %d brokers", len(brokers))
	return brokers, nil
}

func (sd *serviceDiscovery) Deregister() error {
	util.Info("Deregistering broker %s", sd.brokerID)
	future := sd.raft.Apply([]byte(fmt.Sprintf("DEREGISTER:%s", sd.brokerID)), defaultRaftApplyTimeout)

	if err := future.Error(); err != nil {
		util.Error("Failed to deregister broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully deregistered broker %s", sd.brokerID)
	return nil
}
