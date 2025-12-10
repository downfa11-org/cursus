package replication

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/hashicorp/raft"
)

type RaftReplicationManager struct {
	raft       *raft.Raft
	fsm        *BrokerFSM
	isrManager *ISRManager

	brokerID  string
	localAddr string
	peers     map[string]string // brokerID -> addr
	mu        sync.RWMutex

	partitionLeaders map[string]string // topic-partition -> brokerID
}

type ReplicationEntry struct {
	Topic     string
	Partition int
	Message   types.Message
	Term      uint64
}

func (rm *RaftReplicationManager) GetRaft() *raft.Raft {
	return rm.raft
}

func (rm *RaftReplicationManager) GetFSM() *BrokerFSM {
	return rm.fsm
}

func NewRaftReplicationManager(cfg *config.Config, brokerID string, diskManager *disk.DiskManager) (*RaftReplicationManager, error) {
	diskHandler, err := diskManager.GetHandler("replicated", 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk handler: %w", err)
	}

	fsm := NewBrokerFSM(diskHandler)

	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(brokerID)

	dataDir := filepath.Join(cfg.LogDir, "raft")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft data directory: %w", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshots, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	addr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
	transport, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	return &RaftReplicationManager{
		raft:             r,
		brokerID:         brokerID,
		localAddr:        localAddr,
		peers:            make(map[string]string),
		fsm:              fsm,
		partitionLeaders: make(map[string]string),
	}, nil
}

func (rm *RaftReplicationManager) ReplicateMessage(topic string, partition int, msg types.Message) error {
	entry := &ReplicationEntry{
		Topic:     topic,
		Partition: partition,
		Message:   msg,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	future := rm.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to replicate: %w", err)
	}

	return nil
}

func (rm *RaftReplicationManager) IsLeader(topic string, partition int) bool {
	return rm.raft.State() == raft.Leader
}

func (rm *RaftReplicationManager) AddVoter(brokerID, addr string) error {
	configFuture := rm.raft.AddVoter(raft.ServerID(brokerID), raft.ServerAddress(addr), 0, 0)
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	rm.mu.Lock()
	rm.peers[brokerID] = addr
	rm.mu.Unlock()

	return nil
}

func (rm *RaftReplicationManager) Shutdown() error {
	if rm.raft != nil {
		return rm.raft.Shutdown().Error()
	}
	return nil
}

type PartitionMetadata struct {
	Leader      string
	Replicas    []string
	ISR         []string
	LeaderEpoch int64
}

func (rm *RaftReplicationManager) UpdatePartitionLeader(topic string, partition int, leader string) error {
	key := fmt.Sprintf("%s-%d", topic, partition)

	metadata := PartitionMetadata{
		Leader:      leader,
		Replicas:    rm.GetPartitionReplicas(topic, partition),
		ISR:         []string{leader}, // init state, only leader
		LeaderEpoch: time.Now().Unix(),
	}

	data, _ := json.Marshal(metadata)
	future := rm.raft.Apply([]byte(fmt.Sprintf("PARTITION:%s:%s", key, string(data))), 5*time.Second)
	return future.Error()
}

func (rm *RaftReplicationManager) GetPartitionReplicas(topic string, partition int) []string {
	brokers := rm.fsm.GetBrokers()
	if len(brokers) == 0 {
		return nil
	}

	// hash based replicas
	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%s-%d", topic, partition)))

	replicaCount := 3
	if replicaCount > len(brokers) {
		replicaCount = len(brokers)
	}

	var replicas []string
	for i := 0; i < replicaCount; i++ {
		idx := (hash.Sum32() + uint32(i)) % uint32(len(brokers))
		replicas = append(replicas, brokers[idx].Addr)
	}

	return replicas
}

func (rm *RaftReplicationManager) ReplicateToLeader(topic string, partition int, msg types.Message) error {
	if !rm.IsLeader(topic, partition) {
		return fmt.Errorf("not a leader for topic %s partition %d", topic, partition)
	}

	return rm.ReplicateMessage(topic, partition, msg)
}

func (rm *RaftReplicationManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int) error {
	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
			return fmt.Errorf("not enough in-sync replicas for topic %s partition %d but min ISR %d", topic, partition, minISR)
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	future := rm.raft.Apply([]byte(fmt.Sprintf("MESSAGE:%s", string(data))), 5*time.Second)
	if err := future.Error(); err != nil {
		metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
		return fmt.Errorf("failed to replicate with quorum: %w", err)
	}

	metrics.QuorumOperations.WithLabelValues("write", "success").Inc()
	return nil
}

func (rm *RaftReplicationManager) ValidateLeaderEpoch(topic string, partition int, epoch int64) bool {
	key := fmt.Sprintf("%s-%d", topic, partition)

	metadata := rm.getPartitionMetadata(key)
	if metadata == nil {
		return false
	}

	return metadata.LeaderEpoch == epoch
}

func (rm *RaftReplicationManager) getPartitionMetadata(key string) *PartitionMetadata {
	return rm.fsm.GetPartitionMetadata(key)
}
