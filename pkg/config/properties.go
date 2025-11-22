package config

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type ConsumerGroupConfig struct {
	Name            string         `yaml:"name" json:"name"`
	ConsumerCount   int            `yaml:"consumer_count" json:"consumer_count"`
	Topics          []string       `yaml:"topics" json:"topics"`
	TopicPartitions map[string]int `yaml:"topic_partitions" json:"topic_partitions"` // optional
}

// Config represents the broker configuration including tunable performance options
type Config struct {
	// Common
	BrokerPort      int    `yaml:"broker_port" json:"broker.port"`
	HealthCheckPort int    `yaml:"health_check_port" json:"health.check.port"`
	LogDir          string `yaml:"log_dir" json:"log.dir"`
	EnableExporter  bool   `yaml:"enable_exporter" json:"enable.exporter"`
	ExporterPort    int    `yaml:"exporter_port" json:"exporter.port"`
	EnableBenchmark bool   `yaml:"enable_benchmark" json:"enable.benchmark"`

	CleanupInterval int `yaml:"cleanup_interval" json:"cleanup.interval"`

	// message
	UseTLS      bool   `yaml:"use_tls" json:"tls.enable"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls.cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls.key_path"`
	EnableGzip  bool   `yaml:"enable_gzip" json:"gzip.enable"`
	TLSCert     tls.Certificate

	// Broker-specific
	BootstrapServers           []string `yaml:"bootstrap_servers" json:"bootstrap.servers"`
	Acks                       string   `yaml:"acks" json:"acks"` // "0", "1", "all"
	AckTimeoutMS               int      `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`
	MinInsyncReplicas          int      `yaml:"min_insync_replicas" json:"min.insync.replicas"`
	BufferSize                 int      `yaml:"buffer_size" json:"buffer.size"`
	BatchSize                  int      `yaml:"batch_size" json:"batch.size"`
	MaxInflightRequestsPerConn int      `yaml:"max_inflight_requests_per_conn" json:"max.inflight.requests.per.connection"`

	// Consumer-specific
	AutoOffsetReset      string                `yaml:"auto_offset_reset" json:"auto.offset.reset"` // "earliest" or "latest"
	StaticConsumerGroups []ConsumerGroupConfig `yaml:"static_consumer_groups" json:"static_consumer_groups"`
	HeartbeatIntervalMS  int                   `yaml:"heartbeat_interval_ms"`
	SessionTimeoutMS     int                   `yaml:"session_timeout_ms"`
	RebalanceTimeoutMS   int                   `yaml:"rebalance_timeout_ms"`
	MaxPollRecords       int                   `yaml:"max_poll_records" json:"max.poll.records"`

	// DiskHandler tuning
	DiskFlushBatchSize int `yaml:"disk_flush_batch_size" json:"disk.flush.batch.size"`
	LingerMS           int `yaml:"linger_ms" json:"linger.ms"`
	ChannelBufferSize  int `yaml:"channel_buffer_size" json:"channel.buffer.size"`
	DiskWriteTimeoutMS int `yaml:"disk_write_timeout_ms" json:"disk.write.timeout.ms"`
	SegmentSize        int `yaml:"segment_size" json:"segment.size"`
	SegmentRollTimeMS  int `yaml:"segment_roll_time_ms" json:"segment.roll.time.ms"`

	// Partition / Topic tuning
	PartitionChannelBufSize int  `yaml:"partition_channel_buffer_size" json:"partition.channel.buffer.size"`
	ConsumerChannelBufSize  int  `yaml:"consumer_channel_buffer_size" json:"consumer.channel.buffer.size"`
	AutoCreateTopics        bool `yaml:"auto_create_topics" json:"auto.create.topics"`
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}

	// default flags
	flag.IntVar(&cfg.BrokerPort, "port", 9000, "Broker port")
	flag.IntVar(&cfg.HealthCheckPort, "health-port", 9080, "Health check server port")
	flag.StringVar(&cfg.LogDir, "log-dir", "broker-logs", "Path for logs")
	flag.BoolVar(&cfg.EnableExporter, "exporter", true, "Enable Prometheus exporter")
	flag.IntVar(&cfg.ExporterPort, "exporter-port", 9100, "Exporter port")
	flag.BoolVar(&cfg.EnableBenchmark, "benchmark", false, "Enable benchmark mode")

	flag.IntVar(&cfg.CleanupInterval, "cleanup-interval", 300, "Cleanup interval in seconds")

	// message
	flag.BoolVar(&cfg.UseTLS, "tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS certificate path")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS key path")
	flag.BoolVar(&cfg.EnableGzip, "gzip", false, "Enable gzip compression")

	// broker-specific
	flag.StringVar(&cfg.Acks, "acks", "0", "ACK level: 0 (no ack), 1 (leader ack), all (all replicas)")
	flag.IntVar(&cfg.AckTimeoutMS, "ack-timeout-ms", 5000, "ACK timeout in milliseconds")

	// consumer-specific
	flag.StringVar(&cfg.AutoOffsetReset, "auto-offset-reset", "latest", "What to do when there is no initial offset (earliest|latest)")
	flag.IntVar(&cfg.HeartbeatIntervalMS, "heartbeat-interval", 3000, "Heartbeat interval in milliseconds")
	flag.IntVar(&cfg.SessionTimeoutMS, "session-timeout", 30000, "Session timeout in milliseconds")
	flag.IntVar(&cfg.RebalanceTimeoutMS, "rebalance-timeout", 60000, "Rebalance timeout in milliseconds")
	flag.IntVar(&cfg.MaxPollRecords, "max-poll-records", 8192, "Maximum messages per CONSUME request")

	// DiskHandler tuning
	flag.IntVar(&cfg.DiskFlushBatchSize, "disk-flush-batch", 50, "Number of messages per disk flush")
	flag.IntVar(&cfg.LingerMS, "linger-ms", 50, "Maximum time to wait before flush (ms)")
	flag.IntVar(&cfg.ChannelBufferSize, "channel-buffer", 1024, "DiskHandler write channel buffer size")
	flag.IntVar(&cfg.DiskWriteTimeoutMS, "disk-write-timeout", 5, "Synchronous write timeout if channel is full (ms)")
	flag.IntVar(&cfg.SegmentSize, "segment-size", 1048576, "Segment file size in bytes (default: 1MB)")
	flag.IntVar(&cfg.SegmentRollTimeMS, "segment-roll-time-ms", 0, "Time-based segment rotation in milliseconds (0=disabled)")

	// Partition / Topic tuning
	flag.IntVar(&cfg.PartitionChannelBufSize, "partition-ch-buffer", 10000, "Partition input channel buffer size")
	flag.IntVar(&cfg.ConsumerChannelBufSize, "consumer-ch-buffer", 1000, "Consumer channel buffer size")
	flag.BoolVar(&cfg.AutoCreateTopics, "auto-create-topics", true, "Auto-create topics on publish")

	configPath := flag.String("config", "", "Path to YAML/JSON config file")

	if envPath := os.Getenv("CONFIG_PATH"); envPath != "" && *configPath == "" {
		*configPath = envPath
	}
	flag.Parse()

	// file load (YAML or JSON)
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return nil, err
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

	cfg.Normalize()
	return cfg, nil
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

	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 300
	}

	// message / ack
	if cfg.Acks != "0" && cfg.Acks != "1" && cfg.Acks != "all" {
		cfg.Acks = "0"
	}
	if cfg.AckTimeoutMS <= 0 {
		cfg.AckTimeoutMS = 5000
	}

	// consumer
	switch cfg.AutoOffsetReset {
	case "earliest", "latest":
	default:
		cfg.AutoOffsetReset = "latest"
	}

	if cfg.HeartbeatIntervalMS <= 0 {
		cfg.HeartbeatIntervalMS = 3000
	}
	if cfg.HeartbeatIntervalMS > int(time.Minute.Milliseconds()) {
		cfg.HeartbeatIntervalMS = int(time.Minute.Milliseconds())
	}
	if cfg.SessionTimeoutMS <= 0 {
		cfg.SessionTimeoutMS = 30000
	}
	// Ensure session timeout is greater than heartbeat interval
	if cfg.SessionTimeoutMS <= cfg.HeartbeatIntervalMS {
		cfg.SessionTimeoutMS = cfg.HeartbeatIntervalMS * 10
	}
	if cfg.RebalanceTimeoutMS <= 0 {
		cfg.RebalanceTimeoutMS = 60000
	}
	if cfg.RebalanceTimeoutMS < cfg.SessionTimeoutMS {
		cfg.RebalanceTimeoutMS = cfg.SessionTimeoutMS
	}

	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 8192
	}

	// DiskHandler
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
		cfg.DiskWriteTimeoutMS = 5
	}
	if cfg.SegmentSize < 1024 {
		cfg.SegmentSize = 1 << 20
	}
	if cfg.SegmentRollTimeMS < 0 {
		cfg.SegmentRollTimeMS = 0
	}

	// partition/topic
	if cfg.PartitionChannelBufSize <= 0 {
		cfg.PartitionChannelBufSize = 10000
	}
	if cfg.ConsumerChannelBufSize <= 0 {
		cfg.ConsumerChannelBufSize = 1000
	}

	// bootstrap
	if len(cfg.BootstrapServers) == 1 &&
		strings.Contains(cfg.BootstrapServers[0], ",") {
		servers := strings.Split(cfg.BootstrapServers[0], ",")
		for i := range servers {
			servers[i] = strings.TrimSpace(servers[i])
		}
		cfg.BootstrapServers = servers
	}
	if len(cfg.BootstrapServers) == 0 {
		cfg.BootstrapServers = []string{"localhost:9000"}
	}

	// static consumer groups
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
		for _, topic := range g.Topics {
			if g.TopicPartitions[topic] <= 0 {
				g.TopicPartitions[topic] = 1
			}
		}
	}

	if cfg.UseTLS {
		if cfg.TLSCertPath == "" || cfg.TLSKeyPath == "" {
			cfg.UseTLS = false
		} else {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
			if err != nil {
				cfg.UseTLS = false
			} else {
				cfg.TLSCert = cert
			}
		}
	}
}
