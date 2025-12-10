package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ClusterBrokersTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cluster_cluster_brokers_total",
			Help: "Total number of brokers in the cluster",
		},
		[]string{"cluster_id"},
	)

	PartitionLeadersTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cluster_partition_leaders_total",
			Help: "Total number of partition leaders",
		},
		[]string{"broker_id"},
	)

	ClusterReplicationLag = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cluster_replication_lag_seconds",
			Help: "Replication lag across cluster",
		},
		[]string{"topic", "partition", "broker"},
	)

	LeaderElectionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cluster_leader_elections_total",
			Help: "Total number of leader elections",
		},
		[]string{"topic", "partition"},
	)

	ISRSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cluster_isr_size",
			Help: "Size of ISR for each partition",
		},
		[]string{"topic", "partition"},
	)

	ReplicationLagBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cluster_replication_lag_bytes",
			Help: "Current replication lag in bytes per partition",
		},
		[]string{"topic", "partition", "follower_broker"},
	)

	ISRChangesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cluster_isr_changes_total",
			Help: "Total number of ISR changes",
		},
		[]string{"topic", "partition", "change_type"}, // add, remove
	)

	LeaderElectionFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cluster_leader_election_failures_total",
			Help: "Total number of failed leader elections",
		},
		[]string{"topic", "partition", "reason"},
	)

	BrokerHealthStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cluster_broker_health_status",
			Help: "Broker health status (1=healthy, 0=unhealthy)",
		},
		[]string{"broker_id", "broker_addr"},
	)

	QuorumOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cluster_quorum_operations_total",
			Help: "Total quorum operations",
		},
		[]string{"operation", "result"}, // write, read; success, failure
	)

	PartitionReassignments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cluster_partition_reassignments_total",
			Help: "Total number of partition reassignments",
		},
		[]string{"topic", "partition", "from_broker", "to_broker"},
	)
)
