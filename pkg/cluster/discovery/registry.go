package discovery

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

const defaultRaftApplyTimeout = 5 * time.Second
const joinRequestTimeout = 10 * time.Second

type ServiceDiscovery interface {
	Register() error
	DiscoverBrokers() ([]replication.BrokerInfo, error)
	Deregister() error
	JoinCluster(brokerID string, raftAddr string) (leaderAddr string, err error)
	SetRaftManager(rm *replication.RaftReplicationManager)
	AddNode(nodeID string, addr string) (leader string, err error)
	RemoveNode(nodeID string) (leader string, err error)
}

type serviceDiscovery struct {
	fsm      *replication.BrokerFSM
	brokerID string
	addr     string
	raft     *raft.Raft
	rm       *replication.RaftReplicationManager
	mu       sync.Mutex
}

func NewServiceDiscovery(fsm *replication.BrokerFSM, brokerID, addr string, raft *raft.Raft) ServiceDiscovery {
	return &serviceDiscovery{
		fsm:      fsm,
		brokerID: brokerID,
		addr:     addr,
		raft:     raft,
	}
}

func (sd *serviceDiscovery) SetRaftManager(rm *replication.RaftReplicationManager) {
	sd.rm = rm
}

func (sd *serviceDiscovery) Register() error {
	broker := &replication.BrokerInfo{
		ID:       sd.brokerID,
		Addr:     sd.addr,
		Status:   "active",
		LastSeen: time.Now(),
	}

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
	future := sd.raft.Apply([]byte(fmt.Sprintf("DEREGISTER:%s", sd.brokerID)), defaultRaftApplyTimeout)

	if err := future.Error(); err != nil {
		util.Error("Failed to deregister broker %s: %v", sd.brokerID, err)
		return err
	}

	util.Info("Successfully deregistered broker %s", sd.brokerID)
	return nil
}

// JoinCluster tries to add the given node (brokerID, raftAddr) to the Raft cluster.
func (sd *serviceDiscovery) JoinCluster(brokerID string, raftAddr string) (string, error) {
	if brokerID == "" || raftAddr == "" {
		return "", fmt.Errorf("brokerID and raftAddr are required")
	}

	state := sd.raft.State()
	leader := string(sd.raft.Leader())
	if state != raft.Leader {
		util.Debug("not leader (state=%s). leader=%s", state.String(), leader)
		if leader == "" {
			return "", fmt.Errorf("not leader and leader unknown")
		}
		return leader, fmt.Errorf("not leader; contact leader: %s", leader)
	}

	util.Debug("join cluster: we are leader; attempting AddVoter for id=%s addr=%s", brokerID, raftAddr)
	future := sd.raft.AddVoter(raft.ServerID(brokerID), raft.ServerAddress(raftAddr), 0, joinRequestTimeout)
	if err := future.Error(); err != nil {
		util.Error("AddVoter failed for %s@%s: %v", brokerID, raftAddr, err)
		return leader, fmt.Errorf("add voter failed: %w", err)
	}

	util.Info("AddVoter succeeded for %s@%s", brokerID, raftAddr)
	return leader, nil
}

func (sd *serviceDiscovery) AddNode(nodeID string, addr string) (string, error) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if sd.rm == nil {
		return "", fmt.Errorf("raft manager not set on discovery")
	}

	raftInst := sd.rm.GetRaft()
	if raftInst == nil {
		return "", fmt.Errorf("raft instance not available")
	}

	state := raftInst.State()
	leaderAddr := string(raftInst.Leader())
	if state != raft.Leader {
		util.Debug("not leader (state=%s). leader=%s", state.String(), leaderAddr)
		if leaderAddr == "" {
			return leaderAddr, fmt.Errorf("not leader; leader unknown")
		}
		return leaderAddr, fmt.Errorf("not leader; contact leader %s", leaderAddr)
	}

	util.Debug("leader performing AddVoter for %s@%s", nodeID, addr)
	if err := sd.rm.AddVoter(nodeID, addr); err != nil {
		util.Error("AddVoter failed for %s@%s: %v", nodeID, addr, err)
		return leaderAddr, fmt.Errorf("add voter failed: %w", err)
	}

	broker := replication.BrokerInfo{
		ID:       nodeID,
		Addr:     addr,
		Status:   "active",
		LastSeen: time.Now(),
	}

	if err := sd.registerBrokerInfo(&broker); err != nil {
		util.Warn("AddVoter succeeded but Register failed for %s@%s: %v", nodeID, addr, err)
	}

	util.Info("Successfully added node %s@%s", nodeID, addr)
	return leaderAddr, nil
}

func (sd *serviceDiscovery) registerBrokerInfo(b *replication.BrokerInfo) error {
	if sd.rm == nil || sd.rm.GetRaft() == nil {
		return fmt.Errorf("raft not available")
	}

	bs, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("failed to marshal broker info: %w", err)
	}
	fut := sd.rm.GetRaft().Apply([]byte(fmt.Sprintf("REGISTER:%s", string(bs))), defaultRaftApplyTimeout)
	if err := fut.Error(); err != nil {
		return err
	}
	return nil
}

func (sd *serviceDiscovery) RemoveNode(nodeID string) (string, error) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if sd.rm == nil {
		return "", fmt.Errorf("raft manager not set on discovery")
	}
	raftInst := sd.rm.GetRaft()
	if raftInst == nil {
		return "", fmt.Errorf("raft instance not available")
	}

	state := raftInst.State()
	leaderAddr := string(raftInst.Leader())
	if state != raft.Leader {
		util.Debug("not leader (state=%s). leader=%s", state.String(), leaderAddr)
		if leaderAddr == "" {
			return leaderAddr, fmt.Errorf("not leader; leader unknown")
		}
		return leaderAddr, fmt.Errorf("not leader; contact leader %s", leaderAddr)
	}

	idxF := sd.rm.GetRaft().RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := idxF.Error(); err != nil {
		util.Error("Raft RemoveServer failed for %s: %v", nodeID, err)
		return leaderAddr, fmt.Errorf("remove server failed: %w", err)
	}

	fut := sd.rm.GetRaft().Apply([]byte(fmt.Sprintf("DEREGISTER:%s", nodeID)), 5*time.Second)
	if err := fut.Error(); err != nil {
		util.Warn("Raft deregister apply failed for %s: %v", nodeID, err)
	}

	util.Info("RemoveNode: successfully removed %s", nodeID)
	return leaderAddr, nil
}
