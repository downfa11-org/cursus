package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/discovery"
	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/util"
)

type ClusterController struct {
	raftManager        *replication.RaftReplicationManager
	isrManager         *replication.ISRManager
	preferredLeaderMgr *replication.PreferredLeaderManager
	topicManager       *topic.TopicManager

	discovery discovery.ServiceDiscovery
	mu        sync.RWMutex

	partitionLeaders  map[string]string                         // topic-partition -> broker
	partitionMetadata map[string]*replication.PartitionMetadata // topic-partition -> metadata
}

func NewClusterController(rm *replication.RaftReplicationManager, sd discovery.ServiceDiscovery, tm *topic.TopicManager) *ClusterController {
	return &ClusterController{
		raftManager:        rm,
		discovery:          sd,
		topicManager:       tm,
		partitionLeaders:   make(map[string]string),
		partitionMetadata:  make(map[string]*replication.PartitionMetadata),
		preferredLeaderMgr: replication.NewPreferredLeaderManager(),
	}
}

func (cc *ClusterController) SetISRManager(isrManager *replication.ISRManager) {
	cc.isrManager = isrManager
}

func (cc *ClusterController) GetPartitionLeader(topic string, partition int) (string, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	cc.mu.RLock()
	if metadata, exists := cc.partitionMetadata[key]; exists {
		cc.mu.RUnlock()
		util.Debug("Found cached leader for %s: %s", key, metadata.Leader)
		return metadata.Leader, nil
	}
	cc.mu.RUnlock()

	util.Info("No cached leader for %s, triggering election", key)
	if err := cc.ElectPartitionLeader(topic, partition); err != nil {
		util.Error("Failed to elect leader for %s: %v", key, err)
		return "", err
	}

	cc.mu.RLock()
	defer cc.mu.RUnlock()

	metadata, exists := cc.partitionMetadata[key]
	if !exists || metadata == nil {
		return "", fmt.Errorf("metadata not found after election for %s", key)
	}
	leader := metadata.Leader
	util.Info("Elected new leader for %s: %s", key, leader)
	return leader, nil
}

func (cc *ClusterController) ElectPartitionLeader(topic string, partition int) error {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		metrics.LeaderElectionFailures.WithLabelValues(topic, fmt.Sprintf("%d", partition), err.Error()).Inc()
		util.Error("Failed to discover brokers during election: %v", err)
		return err
	}

	key := fmt.Sprintf("%s-%d", topic, partition)
	util.Debug("Found %d brokers for election of %s", len(brokers), key)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	var epoch int64 = time.Now().Unix()
	if existing, exists := cc.partitionMetadata[key]; exists {
		epoch = existing.LeaderEpoch + 1
		util.Debug("Incrementing epoch for %s to %d", key, epoch)
	}

	if preferredLeader, exists := cc.preferredLeaderMgr.GetPreferredLeader(topic, partition); exists {
		util.Debug("Checking preferred leader %s for %s", preferredLeader, key)
		for _, broker := range brokers {
			if broker.Addr == preferredLeader && cc.isBrokerHealthy(broker.Addr) {
				metrics.LeaderElectionTotal.WithLabelValues(topic, fmt.Sprintf("%d", partition)).Inc()
				util.Info("Assigning preferred leader %s for %s", preferredLeader, key)
				return cc.assignLeader(topic, partition, broker.Addr, epoch)
			}
		}
		util.Warn("Preferred leader %s is not healthy for %s", preferredLeader, key)
	}

	for _, broker := range brokers {
		if cc.isBrokerHealthy(broker.Addr) {
			metrics.LeaderElectionTotal.WithLabelValues(topic, fmt.Sprintf("%d", partition)).Inc()
			util.Info("Assigning healthy broker %s as leader for %s", broker.Addr, key)
			return cc.assignLeader(topic, partition, broker.Addr, epoch)
		}
	}

	metrics.LeaderElectionFailures.WithLabelValues(topic, fmt.Sprintf("%d", partition), "FAILED").Inc()
	util.Error("No healthy broker available for leadership of %s", key)
	return fmt.Errorf("no healthy broker available for leadership")
}

func (cc *ClusterController) isBrokerHealthy(addr string) bool {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		util.Warn("Failed to discover brokers while checking health of %s: %v", addr, err)
		return false
	}

	for _, broker := range brokers {
		if broker.Addr == addr && broker.Status == "active" {
			healthy := time.Since(broker.LastSeen) < 5*time.Minute
			if !healthy {
				util.Warn("Broker %s is stale (last seen: %v)", addr, broker.LastSeen)
			}
			return healthy
		}
	}
	util.Debug("Broker %s not found or not active", addr)
	return false
}

func (cc *ClusterController) getAllTopics() []string {
	if cc.topicManager != nil {
		topics := cc.topicManager.ListTopics()
		util.Debug("Retrieved %d topics from topic manager", len(topics))
		return topics
	}
	util.Warn("TopicManager is nil, returning empty topic list")
	return []string{}
}

func (cc *ClusterController) getPartitionCount(topic string) int {
	if cc.topicManager != nil {
		t := cc.topicManager.GetTopic(topic)
		if t != nil {
			count := len(t.Partitions)
			util.Debug("Topic %s has %d partitions", topic, count)
			return count
		}
		util.Warn("Topic %s not found", topic)
	}
	return 0
}

func (cc *ClusterController) selectLeaderWithLeastLoad(brokers []replication.BrokerInfo, leaderCount map[string]int) *replication.BrokerInfo {
	var selected *replication.BrokerInfo
	minCount := int(^uint(0) >> 1)

	for _, broker := range brokers {
		count := leaderCount[broker.Addr]
		if count < minCount {
			minCount = count
			selected = &broker
		}
	}

	if selected != nil {
		util.Debug("Selected broker %s with least load (%d partitions)", selected.Addr, minCount)
	}
	return selected
}

func (cc *ClusterController) UpdateISRStates() {
	util.Debug("Updating ISR states for %d partitions", len(cc.partitionLeaders))
	updated := 0

	cc.mu.RLock()
	leaders := make(map[string]string, len(cc.partitionLeaders))
	for k, v := range cc.partitionLeaders {
		leaders[k] = v
	}
	cc.mu.RUnlock()

	for key, leader := range leaders {
		parts := strings.Split(key, "-")
		if len(parts) == 2 {
			topic := parts[0]
			partition, err := strconv.Atoi(parts[1])
			if err != nil {
				util.Warn("Invalid partition key %s: %v", key, err)
				continue
			}

			replicas := cc.raftManager.GetPartitionReplicas(topic, partition)
			cc.isrManager.UpdateISR(topic, partition, leader, replicas)
			updated++
		}
	}

	util.Debug("Updated ISR states for %d partitions", updated)
}

func (cc *ClusterController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				util.Info("ClusterController stopping: context cancelled")
				return
			case <-ticker.C:
				cc.UpdateISRStates()
			}
		}
	}()
}

func (cc *ClusterController) assignLeader(topic string, partition int, leaderAddr string, epoch int64) error {
	key := fmt.Sprintf("%s-%d", topic, partition)

	if oldLeader, exists := cc.partitionLeaders[key]; exists {
		cc.preferredLeaderMgr.UpdateReplicaLoad(oldLeader, -1)
		util.Debug("Reduced load for previous leader %s of %s", oldLeader, key)
	}

	cc.preferredLeaderMgr.UpdateReplicaLoad(leaderAddr, 1)

	metadata := &replication.PartitionMetadata{
		Leader:      leaderAddr,
		Replicas:    cc.raftManager.GetPartitionReplicas(topic, partition),
		ISR:         []string{leaderAddr},
		LeaderEpoch: epoch,
	}

	cc.partitionLeaders[key] = leaderAddr
	cc.partitionMetadata[key] = metadata

	util.Info("Assigned leader %s (epoch %d) for %s with %d replicas", leaderAddr, epoch, key, len(metadata.Replicas))

	if err := cc.raftManager.UpdatePartitionLeader(topic, partition, leaderAddr); err != nil {
		util.Error("Failed to update partition leader in raft: %v", err)
		return err
	}

	return nil
}

func (cc *ClusterController) RebalanceToPreferredLeaders() error {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		util.Error("Failed to discover brokers for rebalance: %v", err)
		return err
	}

	rebalanced := 0
	for _, broker := range brokers {
		load := cc.preferredLeaderMgr.GetReplicaLoad(broker.Addr)
		if load < 3 {
			util.Debug("Setting preferred leader for underloaded broker %s (load: %d)", broker.Addr, load)
			cc.setPreferredLeaderForPartitions(broker.Addr)
			rebalanced++
		}
	}

	util.Info("Rebalanced %d brokers to preferred leaders", rebalanced)
	return nil
}

func (cc *ClusterController) setPreferredLeaderForPartitions(brokerAddr string) {
	topics := cc.getAllTopics()
	partitionsSet := 0

	for _, topic := range topics {
		partitionCount := cc.getPartitionCount(topic)
		for partition := 0; partition < partitionCount; partition++ {
			cc.preferredLeaderMgr.SetPreferredLeader(topic, partition, brokerAddr)
			partitionsSet++
		}
	}

	util.Debug("Set %s as preferred leader for %d partitions", brokerAddr, partitionsSet)
}
