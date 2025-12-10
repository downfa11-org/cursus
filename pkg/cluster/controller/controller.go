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
		return metadata.Leader, nil
	}
	cc.mu.RUnlock()

	if err := cc.ElectPartitionLeader(topic, partition); err != nil {
		return "", err
	}

	cc.mu.RLock()
	defer cc.mu.RUnlock()
	return cc.partitionMetadata[key].Leader, nil
}

func (cc *ClusterController) ElectPartitionLeader(topic string, partition int) error {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		metrics.LeaderElectionFailures.WithLabelValues(topic, fmt.Sprintf("%d", partition), err.Error()).Inc()
		return err
	}

	key := fmt.Sprintf("%s-%d", topic, partition)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	var epoch int64 = time.Now().Unix()
	if existing, exists := cc.partitionMetadata[key]; exists {
		epoch = existing.LeaderEpoch + 1
	}

	if preferredLeader, exists := cc.preferredLeaderMgr.GetPreferredLeader(topic, partition); exists {
		for _, broker := range brokers {
			if broker.Addr == preferredLeader && cc.isBrokerHealthy(broker.Addr) {
				metrics.LeaderElectionTotal.WithLabelValues(topic, fmt.Sprintf("%d", partition)).Inc()
				return cc.assignLeader(topic, partition, broker.Addr, epoch)
			}
		}
	}

	for _, broker := range brokers {
		if cc.isBrokerHealthy(broker.Addr) {
			metrics.LeaderElectionTotal.WithLabelValues(topic, fmt.Sprintf("%d", partition)).Inc()
			return cc.assignLeader(topic, partition, broker.Addr, epoch)
		}
	}

	metrics.LeaderElectionFailures.WithLabelValues(topic, fmt.Sprintf("%d", partition), "FAILED").Inc()
	return fmt.Errorf("no healthy broker available for leadership")
}

func (cc *ClusterController) isBrokerHealthy(addr string) bool {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		return false
	}

	for _, broker := range brokers {
		if broker.Addr == addr && broker.Status == "active" {
			return time.Since(broker.LastSeen) < 5*time.Minute
		}
	}
	return false
}

func (cc *ClusterController) getAllTopics() []string {
	if cc.topicManager != nil {
		return cc.topicManager.ListTopics()
	}
	return []string{}
}

func (cc *ClusterController) getPartitionCount(topic string) int {
	if cc.topicManager != nil {
		t := cc.topicManager.GetTopic(topic)
		if t != nil {
			return len(t.Partitions)
		}
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

	return selected
}

func (cc *ClusterController) UpdateISRStates() {
	for key, leader := range cc.partitionLeaders {
		parts := strings.Split(key, "-")
		if len(parts) == 2 {
			topic := parts[0]
			partition, _ := strconv.Atoi(parts[1])

			replicas := cc.raftManager.GetPartitionReplicas(topic, partition)
			cc.isrManager.UpdateISR(topic, partition, leader, replicas)
		}
	}
}

func (cc *ClusterController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
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
	return cc.raftManager.UpdatePartitionLeader(topic, partition, leaderAddr)
}

func (cc *ClusterController) RebalanceToPreferredLeaders() error {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		return err
	}

	for _, broker := range brokers {
		load := cc.preferredLeaderMgr.GetReplicaLoad(broker.Addr)
		if load < 3 {
			cc.setPreferredLeaderForPartitions(broker.Addr)
		}
	}

	return nil
}

func (cc *ClusterController) setPreferredLeaderForPartitions(brokerAddr string) {
	topics := cc.getAllTopics()
	for _, topic := range topics {
		partitionCount := cc.getPartitionCount(topic)
		for partition := 0; partition < partitionCount; partition++ {
			cc.preferredLeaderMgr.SetPreferredLeader(topic, partition, brokerAddr)
		}
	}
}
