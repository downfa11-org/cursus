package discovery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/hashicorp/raft"
)

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

	data, _ := json.Marshal(broker)
	future := sd.raft.Apply([]byte(fmt.Sprintf("REGISTER:%s", string(data))), 5*time.Second)
	return future.Error()
}

func (sd *serviceDiscovery) DiscoverBrokers() ([]replication.BrokerInfo, error) {
	return sd.fsm.GetBrokers(), nil
}

func (sd *serviceDiscovery) Deregister() error {
	future := sd.raft.Apply([]byte(fmt.Sprintf("DEREGISTER:%s", sd.brokerID)), 5*time.Second)
	return future.Error()
}
