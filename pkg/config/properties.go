package config

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/util"
	"gopkg.in/yaml.v3"
)

type ConsumerGroupConfig struct {
	Name            string         `yaml:"name" json:"name"`
	ConsumerCount   int            `yaml:"consumer_count" json:"consumer_count"`
	Topics          []string       `yaml:"topics" json:"topics"`
	TopicPartitions map[string]int `yaml:"topic_partitions" json:"topic_partitions"`
}

type Config struct {
	BrokerPort      int           `yaml:"broker_port" json:"broker.port"`
	HealthCheckPort int           `yaml:"health_check_port" json:"health.check.port"`
	EnableExporter  bool          `yaml:"enable_exporter" json:"enable.exporter"`
	ExporterPort    int           `yaml:"exporter_port" json:"exporter.port"`
	LogLevel        util.LogLevel `yaml:"log_level" json:"log_level"`

	LogDir             string `yaml:"log_dir" json:"log.dir"`
	DiskFlushBatchSize int    `yaml:"disk_flush_batch_size" json:"disk.flush.batch.size"`
	LingerMS           int    `yaml:"linger_ms" json:"linger.ms"`
	ChannelBufferSize  int    `yaml:"channel_buffer_size" json:"channel.buffer.size"`
	DiskWriteTimeoutMS int    `yaml:"disk_write_timeout_ms" json:"disk.write.timeout.ms"`
	SegmentSize        int    `yaml:"segment_size" json:"segment.size"`
	SegmentRollTimeMS  int    `yaml:"segment_roll_time_ms" json:"segment.roll.time.ms"`

	PartitionChannelBufSize    int `yaml:"partition_channel_buffer_size" json:"partition.channel.buffer.size"`
	ConsumerChannelBufSize     int `yaml:"consumer_channel_buffer_size" json:"consumer.channel.buffer.size"`
	BroadcastChannelBufferSize int `yaml:"broadcast_channel_buffer_size" json:"broadcast.channel.buffer.size"`

	ConsumerSessionTimeoutMS int `yaml:"consumer_session_timeout_ms" json:"consumer.session.timeout.ms"`
	ConsumerHeartbeatCheckMS int `yaml:"consumer_heartbeat_check_ms" json:"consumer.heartbeat.check.ms"`

	CleanupInterval      int                   `yaml:"cleanup_interval" json:"cleanup.interval"`
	StaticConsumerGroups []ConsumerGroupConfig `yaml:"static_consumer_groups" json:"static_consumer_groups"`

	MaxStreamConnections    int           `yaml:"max_stream_connections" json:"max.stream.connections"`
	StreamTimeout           time.Duration `yaml:"stream_timeout" json:"stream.timeout"`
	StreamHeartbeatInterval time.Duration `yaml:"stream_heartbeat_interval" json:"stream.heartbeat.interval"`
	StreamCommitInterval    time.Duration `yaml:"stream_commit_interval" json:"stream.commit.interval"`

	UseTLS      bool `yaml:"use_tls" json:"tls.enable"`
	TLSCert     tls.Certificate
	TLSCertPath string `yaml:"tls_cert_path" json:"tls.cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls.key_path"`

	CompressionType string `yaml:"compression_type" json:"compression.type"` // "none", "gzip", "snappy", "lz4"

	// cluster Options
	EnabledDistribution  bool     `yaml:"enabled_distribution" json:"distribution.enabled"`
	RaftPort             int      `yaml:"raft_port" json:"distribution.raft.port"`
	DiscoveryPort        int      `yaml:"discovery_port" json:"distribution.discovery.port"`
	RaftPeers            []string `yaml:"raft_peers" json:"distribution.raft.peers"`
	StaticClusterMembers []string `yaml:"static_cluster_members" json:"static_cluster_members"`
	BootstrapCluster     bool     `yaml:"bootstrap_cluster" json:"distribution.bootstrap"`
	AdvertisedHost       string   `yaml:"advertised_host" json:"distribution.advertised_host"`
	MinInSyncReplicas    int      `yaml:"min_insync_replicas" json:"min.insync.replicas"`
}

func defaultConfig() *Config {
	return &Config{
		BrokerPort:                 9000,
		HealthCheckPort:            9080,
		EnableExporter:             true,
		ExporterPort:               9100,
		LogLevel:                   util.LogLevelInfo,
		LogDir:                     "broker-logs",
		DiskFlushBatchSize:         50,
		LingerMS:                   50,
		ChannelBufferSize:          1024,
		DiskWriteTimeoutMS:         10,
		SegmentSize:                1 << 20,
		SegmentRollTimeMS:          0,
		PartitionChannelBufSize:    10000,
		ConsumerChannelBufSize:     1000,
		BroadcastChannelBufferSize: 10000,
		ConsumerSessionTimeoutMS:   10000,
		ConsumerHeartbeatCheckMS:   5000,
		CleanupInterval:            300,
		MaxStreamConnections:       1000,
		StreamTimeout:              30 * time.Minute,
		StreamHeartbeatInterval:    3 * time.Second,
		StreamCommitInterval:       5 * time.Second,
		CompressionType:            "none",
		EnabledDistribution:        false,
		RaftPort:                   9001,
		DiscoveryPort:              8000,
		RaftPeers:                  []string{},
		StaticClusterMembers:       []string{},
		BootstrapCluster:           false,
		AdvertisedHost:             "localhost",
		MinInSyncReplicas:          2,
	}
}

func LoadConfig() (*Config, error) {
	cfg := defaultConfig()
	configPath := flag.String("config", "", "Path to YAML/JSON config file")

	flag.IntVar(&cfg.BrokerPort, "port", cfg.BrokerPort, "Broker port")
	flag.IntVar(&cfg.HealthCheckPort, "health-port", cfg.HealthCheckPort, "Health port")
	flag.StringVar(&cfg.LogDir, "log-dir", cfg.LogDir, "Log directory")
	flag.BoolVar(&cfg.EnableExporter, "exporter", cfg.EnableExporter, "Enable exporter")
	flag.IntVar(&cfg.ExporterPort, "exporter-port", cfg.ExporterPort, "Exporter port")

	logLevelStr := flag.String("log-level", "info", "Log level")

	flag.IntVar(&cfg.CleanupInterval, "cleanup-interval", cfg.CleanupInterval, "Cleanup seconds")

	flag.IntVar(&cfg.DiskFlushBatchSize, "disk-flush-batch", cfg.DiskFlushBatchSize, "Disk flush batch")
	flag.IntVar(&cfg.LingerMS, "linger-ms", cfg.LingerMS, "Linger ms")
	flag.IntVar(&cfg.ChannelBufferSize, "channel-buffer", cfg.ChannelBufferSize, "Channel buffer")
	flag.IntVar(&cfg.DiskWriteTimeoutMS, "disk-write-timeout", cfg.DiskWriteTimeoutMS, "Disk write timeout")
	flag.IntVar(&cfg.SegmentSize, "segment-size", cfg.SegmentSize, "Segment size")
	flag.IntVar(&cfg.SegmentRollTimeMS, "segment-roll-time-ms", cfg.SegmentRollTimeMS, "Segment roll time")

	flag.IntVar(&cfg.PartitionChannelBufSize, "partition-ch-buffer", cfg.PartitionChannelBufSize, "Partition buffer")
	flag.IntVar(&cfg.ConsumerChannelBufSize, "consumer-ch-buffer", cfg.ConsumerChannelBufSize, "Consumer buffer")
	flag.IntVar(&cfg.BroadcastChannelBufferSize, "broadcast-ch-buffer", cfg.BroadcastChannelBufferSize, "Broadcast channel buffer size")

	flag.IntVar(&cfg.ConsumerSessionTimeoutMS, "consumer-session-timeout", cfg.ConsumerSessionTimeoutMS, "Session timeout")
	flag.IntVar(&cfg.ConsumerHeartbeatCheckMS, "consumer-heartbeat-check", cfg.ConsumerHeartbeatCheckMS, "Heartbeat check")

	flag.BoolVar(&cfg.UseTLS, "tls", cfg.UseTLS, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", cfg.TLSCertPath, "TLS cert")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", cfg.TLSKeyPath, "TLS key")

	flag.StringVar(&cfg.CompressionType, "compression-type", "none", "Compression type (none, gzip, snappy, lz4)")

	flag.IntVar(&cfg.MaxStreamConnections, "max-stream-connections", cfg.MaxStreamConnections, "Max stream connections")
	flag.DurationVar(&cfg.StreamTimeout, "stream-timeout", cfg.StreamTimeout, "Stream timeout")
	flag.DurationVar(&cfg.StreamHeartbeatInterval, "stream-heartbeat-interval", cfg.StreamHeartbeatInterval, "Stream heartbeat")
	flag.DurationVar(&cfg.StreamCommitInterval, "stream-commit-interval", cfg.StreamCommitInterval, "Stream commit interval")

	flag.BoolVar(&cfg.EnabledDistribution, "enable-distribution", cfg.EnabledDistribution, "Enable distributed clustering")
	flag.StringVar(&cfg.AdvertisedHost, "advertised-host", cfg.AdvertisedHost, "Advertised host for discovery")
	flag.IntVar(&cfg.RaftPort, "raft-port", cfg.RaftPort, "Raft port for replication")
	flag.IntVar(&cfg.DiscoveryPort, "discovery-port", cfg.DiscoveryPort, "Discovery service port")
	raftPeersFlag := flag.String("raft-peers", "", "Raft peer addresses (comma-separated)")
	flag.BoolVar(&cfg.BootstrapCluster, "bootstrap-cluster", cfg.BootstrapCluster, "Bootstrap Raft cluster")
	flag.IntVar(&cfg.MinInSyncReplicas, "min-insync-replicas", cfg.MinInSyncReplicas, "Minimum in-sync replicas for writes")

	flag.Parse()

	switch strings.ToLower(*logLevelStr) {
	case "debug":
		cfg.LogLevel = util.LogLevelDebug
	case "warn", "warning":
		cfg.LogLevel = util.LogLevelWarn
	case "error":
		cfg.LogLevel = util.LogLevelError
	default:
		cfg.LogLevel = util.LogLevelInfo
	}

	if env := os.Getenv("CONFIG_PATH"); env != "" && *configPath == "" {
		*configPath = env
	}

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			if os.IsNotExist(err) {
				util.Warn("Config file %s not found, using flag defaults", *configPath)
				return cfg, nil
			}
			return nil, fmt.Errorf("failed to read config file %s: %w", *configPath, err)
		}
		if strings.HasSuffix(*configPath, ".json") {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		}
	}

	if *raftPeersFlag != "" {
		parts := strings.Split(*raftPeersFlag, ",")
		cfg.RaftPeers = make([]string, 0, len(parts))
		for _, s := range parts {
			s = strings.TrimSpace(s)
			if s != "" {
				cfg.RaftPeers = append(cfg.RaftPeers, s)
			}
		}
	}

	overrideEnvInt(&cfg.BrokerPort, "BROKER_PORT")
	overrideEnvInt(&cfg.HealthCheckPort, "HEALTH_CHECK_PORT")

	overrideEnvBool(&cfg.EnableExporter, "ENABLE_EXPORTER")
	overrideEnvInt(&cfg.ExporterPort, "EXPORTER_PORT")

	overrideEnvInt(&cfg.DiskFlushBatchSize, "DISK_FLUSH_BATCH")
	overrideEnvInt(&cfg.LingerMS, "LINGER_MS")

	overrideEnvInt(&cfg.PartitionChannelBufSize, "PARTITION_CH_BUFFER")
	overrideEnvInt(&cfg.ConsumerChannelBufSize, "CONSUMER_CH_BUFFER")
	overrideEnvInt(&cfg.BroadcastChannelBufferSize, "BROADCAST_CH_BUFFER")

	overrideEnvInt(&cfg.ConsumerSessionTimeoutMS, "CONSUMER_SESSION_TIMEOUT")
	overrideEnvInt(&cfg.ConsumerHeartbeatCheckMS, "CONSUMER_HEARTBEAT_CHECK")

	overrideEnvString(&cfg.CompressionType, "COMPRESSION_TYPE")

	overrideEnvBool(&cfg.EnabledDistribution, "ENABLE_DISTRIBUTION")
	overrideEnvString(&cfg.AdvertisedHost, "ADVERTISED_HOST")
	overrideEnvInt(&cfg.RaftPort, "RAFT_PORT")
	overrideEnvInt(&cfg.DiscoveryPort, "DISCOVERY_PORT")
	overrideEnvStringSlice(&cfg.RaftPeers, "RAFT_PEERS")
	overrideEnvBool(&cfg.BootstrapCluster, "BOOTSTRAP_CLUSTER")
	overrideEnvInt(&cfg.MinInSyncReplicas, "MIN_INSYNC_REPLICAS")

	cfg.Normalize()
	util.SetLevel(cfg.LogLevel)

	if cfg.UseTLS {
		if cfg.TLSCertPath == "" || cfg.TLSKeyPath == "" {
			return nil, fmt.Errorf("TLS enabled but missing cert/key path")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS cert: %w", err)
		}
		cfg.TLSCert = cert
	}

	return cfg, nil
}

func overrideEnvInt(target *int, key string) {
	if v := os.Getenv(key); v != "" {
		*target = util.ParseInt(v, *target)
	}
}

func overrideEnvBool(target *bool, key string) {
	if v := os.Getenv(key); v != "" {
		*target = util.ParseBool(v, *target)
	}
}

func overrideEnvString(target *string, key string) {
	if v := os.Getenv(key); v != "" {
		*target = v
	}
}

func overrideEnvStringSlice(target *[]string, key string) {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		result := make([]string, 0, len(parts))
		for _, s := range parts {
			s = strings.TrimSpace(s)
			if s != "" {
				result = append(result, s)
			}
		}
		*target = result
	}
}

func (cfg *Config) Normalize() {
	if cfg.BrokerPort <= 0 {
		cfg.BrokerPort = 9000
	}
	if cfg.HealthCheckPort <= 0 {
		cfg.HealthCheckPort = 9080
	}
	if strings.TrimSpace(cfg.LogDir) == "" {
		cfg.LogDir = "broker-logs"
	}
	if cfg.ExporterPort <= 0 {
		cfg.ExporterPort = 9100
	}
	if cfg.CompressionType == "" {
		cfg.CompressionType = "none"
	}
	switch cfg.CompressionType {
	case "none", "gzip", "snappy", "lz4":
	default:
		util.Warn("Invalid compression_type '%s', defaulting to 'none'", cfg.CompressionType)
		cfg.CompressionType = "none"
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 300
	}
	if cfg.DiskFlushBatchSize <= 0 {
		cfg.DiskFlushBatchSize = 50
	}
	if cfg.LingerMS < 0 {
		cfg.LingerMS = 0
	}
	if cfg.ChannelBufferSize <= 0 {
		cfg.ChannelBufferSize = 1024
	}
	if cfg.DiskWriteTimeoutMS <= 0 {
		cfg.DiskWriteTimeoutMS = 10
	}
	if cfg.SegmentSize < 1024 {
		cfg.SegmentSize = 1 << 20
	}
	if cfg.SegmentRollTimeMS < 0 {
		cfg.SegmentRollTimeMS = 0
	}
	if cfg.PartitionChannelBufSize <= 0 {
		cfg.PartitionChannelBufSize = 10000
	}
	if cfg.ConsumerChannelBufSize <= 0 {
		cfg.ConsumerChannelBufSize = 1000
	}
	if cfg.BroadcastChannelBufferSize <= 0 {
		cfg.BroadcastChannelBufferSize = 10000
	}

	for i := range cfg.StaticConsumerGroups {
		g := &cfg.StaticConsumerGroups[i]
		if strings.TrimSpace(g.Name) == "" {
			g.Name = "default-group"
		}
		if g.ConsumerCount <= 0 {
			g.ConsumerCount = 1
		}
		if len(g.Topics) == 0 {
			g.Topics = []string{"default-topic"}
		}
		if g.TopicPartitions == nil {
			g.TopicPartitions = map[string]int{}
		}
		for _, t := range g.Topics {
			if g.TopicPartitions[t] <= 0 {
				g.TopicPartitions[t] = 1
			}
		}
	}

	if cfg.ConsumerSessionTimeoutMS <= 0 {
		cfg.ConsumerSessionTimeoutMS = 10000
	}
	if cfg.ConsumerHeartbeatCheckMS <= 0 {
		cfg.ConsumerHeartbeatCheckMS = 5000
	}
	if cfg.ConsumerHeartbeatCheckMS >= cfg.ConsumerSessionTimeoutMS {
		cfg.ConsumerHeartbeatCheckMS = cfg.ConsumerSessionTimeoutMS / 2
	}

	if cfg.MaxStreamConnections <= 0 {
		cfg.MaxStreamConnections = 1000
	}
	if cfg.StreamTimeout <= 0 {
		cfg.StreamTimeout = 30 * time.Minute
	}
	if cfg.StreamHeartbeatInterval <= 0 {
		cfg.StreamHeartbeatInterval = 3 * time.Second
	}
	if cfg.StreamCommitInterval <= 0 {
		cfg.StreamCommitInterval = 5 * time.Second
	}
	if cfg.RaftPort <= 0 {
		cfg.RaftPort = 9001
	}
	if cfg.DiscoveryPort <= 0 {
		cfg.DiscoveryPort = 8000
	}
	if strings.TrimSpace(cfg.AdvertisedHost) == "" {
		cfg.AdvertisedHost = "localhost"
	}
	if cfg.MinInSyncReplicas <= 0 {
		cfg.MinInSyncReplicas = 2
	}
}
