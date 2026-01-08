package controller

import (
	"context"
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

func NewServiceDiscovery(rm *replication.RaftReplicationManager, brokerID, addr string) *ServiceDiscovery {
	return &ServiceDiscovery{
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

	data, err := json.Marshal(broker)
	if err != nil {
		util.Error("Failed to marshal broker info: %v", err)
		return fmt.Errorf("marshal broker info: %w", err)
	}

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
		util.Error("Failed to add Raft voter: %v", err)
		return leaderAddr, err
	}

	broker := &fsm.BrokerInfo{
		ID:       nodeID,
		Addr:     addr,
		Status:   "active",
		LastSeen: time.Now(),
	}

	data, err := json.Marshal(broker)
	if err != nil {
		util.Error("Marshal failed after AddVoter. Node added to Raft but not to FSM: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("marshal failed after AddVoter: %w", err)
	}

	if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
		util.Error("REGISTER command failed after AddVoter. Raft cluster and FSM are now inconsistent: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("REGISTER command failed: %w", err)
	}

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

	if err := sd.rm.ApplyCommand("DEREGISTER", []byte(nodeID)); err != nil {
		util.Error("DEREGISTER failed after RemoveServer. FSM contains stale node info: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("DEREGISTER failed: %w", err)
	}

	return leaderAddr, nil
}

func (sd *ServiceDiscovery) StartReconciler(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		defer ticker.Stop()
		util.Debug("reconciler started for broker %s", sd.brokerID)

		for {
			select {
			case <-ticker.C:
				if !sd.rm.IsLeader() {
					continue
				}
				sd.reconcile()
			case <-ctx.Done():
				util.Info("reconciler stopping for broker %s due to context cancellation", sd.brokerID)
				return
			}
		}
	}()
}

func (sd *ServiceDiscovery) reconcile() {
	future := sd.rm.GetConfiguration()
	if err := future.Error(); err != nil {
		util.Error("Failed to get Raft configuration: %v", err)
		return
	}
	raftServers := future.Configuration().Servers

	raftMap := make(map[string]string)
	for _, s := range raftServers {
		raftMap[string(s.ID)] = string(s.Address)
	}

	fsmBrokers := sd.fsm.GetBrokers()
	fsmMap := make(map[string]bool)

	for _, b := range fsmBrokers {
		fsmMap[b.ID] = true
		if _, exists := raftMap[b.ID]; !exists {
			util.Warn("Node %s found in FSM but missing in Raft. Cleaning up...", b.ID)
			if err := sd.rm.ApplyCommand("DEREGISTER", []byte(b.ID)); err != nil {
				util.Error("Failed to apply DEREGISTER for node %s: %v", b.ID, err)
			}
		}
	}

	for id, addr := range raftMap {
		if !fsmMap[id] {
			util.Warn("Node %s found in Raft but missing in FSM. Repairing...", id)
			broker := &fsm.BrokerInfo{
				ID:       id,
				Addr:     addr,
				Status:   "active",
				LastSeen: time.Now(),
			}
			data, err := json.Marshal(broker)
			if err != nil {
				util.Error("Failed to marshal broker info for node %s: %v", id, err)
				continue
			}
			if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
				util.Error("Failed to apply REGISTER repair for node %s: %v", id, err)
			} else {
				util.Info("Successfully repaired FSM for node %s", id)
			}
		}
	}
}
