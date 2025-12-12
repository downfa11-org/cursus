package replication

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	client "github.com/downfa11-org/go-broker/pkg/cluster/client"
	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

type RaftInterface interface {
	BootstrapCluster(raft.Configuration) raft.Future
	Apply([]byte, time.Duration) raft.ApplyFuture
	AddVoter(raft.ServerID, raft.ServerAddress, uint64, time.Duration) raft.IndexFuture
	State() raft.RaftState
	Shutdown() raft.Future
	GetConfiguration() raft.ConfigurationFuture
	RemoveServer(raft.ServerID, uint64, time.Duration) raft.IndexFuture
	Leader() raft.ServerAddress
}

type BrokerFSMInterface interface {
	Apply(*raft.Log) interface{}
	Restore(io.ReadCloser) error
	Snapshot() (raft.FSMSnapshot, error)
	GetBrokers() []BrokerInfo
	GetPartitionMetadata(string) *PartitionMetadata
}

type ISRManagerInterface interface {
	HasQuorum(string, int, int) bool
}

type RaftReplicationManager struct {
	raft       RaftInterface
	fsm        BrokerFSMInterface
	isrManager ISRManagerInterface

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
	if r, ok := rm.raft.(*raft.Raft); ok {
		return r
	}
	return nil
}

func (rm *RaftReplicationManager) GetFSM() *BrokerFSM {
	if f, ok := rm.fsm.(*BrokerFSM); ok {
		return f
	}
	return nil
}

func NewRaftReplicationManager(cfg *config.Config, brokerID string, diskManager *disk.DiskManager, clusterClient client.ClusterClient) (*RaftReplicationManager, error) {
	diskHandler, err := diskManager.GetHandler("replicated", 0)
	if err != nil {
		util.Error("Failed to get disk handler for replication: %v", err)
		return nil, fmt.Errorf("failed to get disk handler: %w", err)
	}

	fsm := NewBrokerFSM(diskHandler)

	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(brokerID)

	config.ProtocolVersion = raft.ProtocolVersionMax
	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 1500 * time.Millisecond
	config.CommitTimeout = 100 * time.Millisecond
	config.LogLevel = "Debug"

	if len(cfg.StaticClusterMembers) >= 3 {
		config.PreVoteDisabled = true
	}

	dataDir := filepath.Join(cfg.LogDir, "raft")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		util.Error("Failed to create raft data directory %s: %v", dataDir, err)
		return nil, fmt.Errorf("failed to create raft data directory: %w", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		util.Error("Failed to create snapshot store: %v", err)
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	advertiseTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		util.Error("Failed to resolve advertised address %s: %v", localAddr, err)
		return nil, fmt.Errorf("failed to resolve advertised address: %w", err)
	}

	bindAddr := fmt.Sprintf("0.0.0.0:%d", cfg.RaftPort)
	transport, err := raft.NewTCPTransport(bindAddr, advertiseTCPAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		util.Error("Failed to create raft transport: %v", err)
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		util.Error("Failed to create raft instance: %v", err)
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	if cfg.BootstrapCluster {
		if confFuture := r.GetConfiguration(); confFuture.Error() == nil {
			conf := confFuture.Configuration()
			if len(conf.Servers) == 0 {
				util.Info("🚀 Starting static cluster bootstrap")

				var servers []raft.Server
				if len(cfg.StaticClusterMembers) == 0 {
					if staticMembers := os.Getenv("STATIC_CLUSTER_MEMBERS"); staticMembers != "" {
						cfg.StaticClusterMembers = strings.Split(staticMembers, ",")
					}
				}

				if len(cfg.StaticClusterMembers) == 0 {
					return nil, fmt.Errorf("STATIC_CLUSTER_MEMBERS is required for static cluster bootstrap")
				}

				for _, member := range cfg.StaticClusterMembers {
					member = strings.TrimSpace(member)
					if member == "" {
						continue
					}

					var memberID, memberAddr string
					if strings.Contains(member, "@") {
						parts := strings.SplitN(member, "@", 2)
						if len(parts) == 2 {
							memberID = parts[0]
							memberAddr = parts[1]
						} else {
							continue
						}
					} else {
						memberAddr = member
						memberID = strings.Split(memberAddr, ":")[0]
					}

					servers = append(servers, raft.Server{
						ID:       raft.ServerID(memberID),
						Address:  raft.ServerAddress(memberAddr),
						Suffrage: raft.Voter,
					})
					util.Debug("Added static cluster member: id=%s addr=%s", memberID, memberAddr)
				}

				if len(servers) == 0 {
					return nil, fmt.Errorf("no valid servers found in StaticClusterMembers")
				}

				bootstrapConfig := raft.Configuration{Servers: servers}
				util.Debug("Static bootstrap configuration: %+v", bootstrapConfig)

				future := r.BootstrapCluster(bootstrapConfig)
				if err := future.Error(); err != nil {
					util.Error("Failed to bootstrap static cluster: %v", err)
					return nil, fmt.Errorf("failed to bootstrap static cluster: %w", err)
				}
				util.Info("✅ Static cluster bootstrap completed")
			}
		}
	} else if len(cfg.RaftPeers) > 0 {
		go func() {
			if err := clusterClient.JoinCluster(cfg.RaftPeers, brokerID, localAddr, cfg.DiscoveryPort); err != nil {
				util.Error("Failed to join cluster: %v", err)
			}
		}()
	}

	manager := &RaftReplicationManager{
		raft:             r,
		brokerID:         brokerID,
		localAddr:        localAddr,
		peers:            make(map[string]string),
		fsm:              fsm,
		partitionLeaders: make(map[string]string),
	}

	if cfg.LogLevel == util.LogLevelDebug {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				state := manager.raft.State()
				leaderAddr := manager.raft.Leader()

				if configFuture := manager.raft.GetConfiguration(); configFuture.Error() == nil {
					config := configFuture.Configuration()
					util.Debug("DEBUG_RAFT_STATE: State=%s, Leader=%s, IsLeader=%v, KnownServers=%d",
						state.String(), leaderAddr, state == raft.Leader, len(config.Servers))
				} else {
					util.Debug("DEBUG_RAFT_STATE: State=%s, Leader=%s, IsLeader=%v",
						state.String(), leaderAddr, state == raft.Leader)
				}
			}
		}()
	}

	return manager, nil
}

func (rm *RaftReplicationManager) ReplicateMessage(topic string, partition int, msg types.Message) error {
	util.Debug("Replicating message to topic %s partition %d", topic, partition)

	entry := &ReplicationEntry{
		Topic:     topic,
		Partition: partition,
		Message:   msg,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		util.Error("Failed to marshal replication entry: %v", err)
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	future := rm.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		util.Error("Failed to replicate message: %v", err)
		return fmt.Errorf("failed to replicate: %w", err)
	}

	util.Debug("Successfully replicated message to topic %s partition %d", topic, partition)
	return nil
}

func (rm *RaftReplicationManager) IsLeader(topic string, partition int) bool {
	isLeader := rm.raft.State() == raft.Leader
	util.Debug("Leadership check for topic %s partition %d: %v", topic, partition, isLeader)
	return isLeader
}

func (rm *RaftReplicationManager) AddVoter(brokerID, addr string) error {
	util.Info("Adding voter %s at %s", brokerID, addr)

	configFuture := rm.raft.AddVoter(raft.ServerID(brokerID), raft.ServerAddress(addr), 0, 0)
	if err := configFuture.Error(); err != nil {
		util.Error("Failed to add voter %s: %v", brokerID, err)
		return fmt.Errorf("failed to add voter: %w", err)
	}

	rm.mu.Lock()
	rm.peers[brokerID] = addr
	rm.mu.Unlock()

	util.Info("Successfully added voter %s", brokerID)
	return nil
}

func (rm *RaftReplicationManager) Shutdown() error {
	if rm.raft != nil {
		if err := rm.raft.Shutdown().Error(); err != nil {
			util.Error("Failed to shutdown raft: %v", err)
			return err
		}
	}
	util.Info("Successfully shutdown RaftReplicationManager")
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
	util.Info("Updating partition leader for %s to %s", key, leader)

	metadata := PartitionMetadata{
		Leader:      leader,
		Replicas:    rm.GetPartitionReplicas(topic, partition),
		ISR:         []string{leader}, // init state, only leader
		LeaderEpoch: time.Now().Unix(),
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal partition metadata: %w", err)
	}

	future := rm.raft.Apply([]byte(fmt.Sprintf("PARTITION:%s:%s", key, string(data))), 5*time.Second)
	if err := future.Error(); err != nil {
		util.Error("Failed to update partition leader for %s: %v", key, err)
		return err
	}

	util.Info("Successfully updated partition leader for %s", key)
	return nil
}

func (rm *RaftReplicationManager) GetPartitionReplicas(topic string, partition int) []string {
	brokers := rm.fsm.GetBrokers()
	if len(brokers) == 0 {
		util.Debug("No brokers available for replica selection")
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

	util.Debug("Selected %d replicas for %s-%d: %v", len(replicas), topic, partition, replicas)
	return replicas
}

func (rm *RaftReplicationManager) ReplicateToLeader(topic string, partition int, msg types.Message) error {
	if !rm.IsLeader(topic, partition) {
		util.Warn("Attempted to replicate to non-leader for topic %s partition %d", topic, partition)
		return fmt.Errorf("not a leader for topic %s partition %d", topic, partition)
	}

	return rm.ReplicateMessage(topic, partition, msg)
}

func (rm *RaftReplicationManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int) error {
	util.Debug("Replicating with quorum for topic %s partition %d (min ISR: %d)", topic, partition, minISR)

	if rm.isrManager != nil {
		if !rm.isrManager.HasQuorum(topic, partition, minISR) {
			metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
			util.Error("Insufficient in-sync replicas for topic %s partition %d (min ISR: %d)", topic, partition, minISR)
			return fmt.Errorf("not enough in-sync replicas for topic %s partition %d but min ISR %d", topic, partition, minISR)
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		util.Error("Failed to marshal message for quorum replication: %v", err)
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	future := rm.raft.Apply([]byte(fmt.Sprintf("MESSAGE:%s", string(data))), 5*time.Second)
	if err := future.Error(); err != nil {
		metrics.QuorumOperations.WithLabelValues("write", "failure").Inc()
		util.Error("Failed to replicate with quorum for topic %s partition %d: %v", topic, partition, err)
		return fmt.Errorf("failed to replicate with quorum: %w", err)
	}

	metrics.QuorumOperations.WithLabelValues("write", "success").Inc()
	util.Debug("Successfully replicated with quorum for topic %s partition %d", topic, partition)
	return nil
}

func (rm *RaftReplicationManager) ValidateLeaderEpoch(topic string, partition int, epoch int64) bool {
	key := fmt.Sprintf("%s-%d", topic, partition)

	metadata := rm.getPartitionMetadata(key)
	if metadata == nil {
		util.Debug("No metadata found for %s during epoch validation", key)
		return false
	}

	valid := metadata.LeaderEpoch == epoch
	util.Debug("Epoch validation for %s: expected=%d, actual=%d, valid=%v", key, epoch, metadata.LeaderEpoch, valid)
	return valid
}

func (rm *RaftReplicationManager) getPartitionMetadata(key string) *PartitionMetadata {
	return rm.fsm.GetPartitionMetadata(key)
}
