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
		util.Error("CRITICAL: Marshal failed after AddVoter. Node added to Raft but not to FSM: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("marshal failed after AddVoter: %w", err)
	}

	if err := sd.rm.ApplyCommand("REGISTER", data); err != nil {
		util.Error("CRITICAL: REGISTER command failed after AddVoter. Raft cluster and FSM are now inconsistent: id=%s err=%v", nodeID, err)
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
		util.Error("CRITICAL: DEREGISTER failed after RemoveServer. FSM contains stale node info: id=%s err=%v", nodeID, err)
		return leaderAddr, fmt.Errorf("DEREGISTER failed: %w", err)
	}

	return leaderAddr, nil
}

/*
todo. 왜 Raft configuration 변경과 FSM 상태 변경이
서로 다른 Apply 경로인가?

AddVoter → Raft internal log

REGISTER → FSM command

이 둘은 같은 합의 단위가 아니다
→ 결국 “언젠가는 어긋난다”

장기적으로는

AddVoter + REGISTER 를 단일 Raft FSM command로 묶거나

실패 시 보정하는 reconciliation 루프가 필요하다
*/
