package config

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/downfa11-org/go-broker/util"
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
	// Server settings
	BrokerPort      int           `yaml:"broker_port" json:"broker.port"`
	HealthCheckPort int           `yaml:"health_check_port" json:"health.check.port"`
	EnableExporter  bool          `yaml:"enable_exporter" json:"enable.exporter"`
	ExporterPort    int           `yaml:"exporter_port" json:"exporter.port"`
	EnableBenchmark bool          `yaml:"enable_benchmark" json:"enable.benchmark"`
	LogLevel        util.LogLevel `yaml:"log_level" json:"log_level"`

	// Disk persistence
	LogDir             string `yaml:"log_dir" json:"log.dir"`
	DiskFlushBatchSize int    `yaml:"disk_flush_batch_size" json:"disk.flush.batch.size"`
	LingerMS           int    `yaml:"linger_ms" json:"linger.ms"`
	ChannelBufferSize  int    `yaml:"channel_buffer_size" json:"channel.buffer.size"`
	DiskWriteTimeoutMS int    `yaml:"disk_write_timeout_ms" json:"disk.write.timeout.ms"`
	SegmentSize        int    `yaml:"segment_size" json:"segment.size"`
	SegmentRollTimeMS  int    `yaml:"segment_roll_time_ms" json:"segment.roll.time.ms"`

	// Internal channel buffers
	PartitionChannelBufSize int `yaml:"partition_channel_buffer_size" json:"partition.channel.buffer.size"`
	ConsumerChannelBufSize  int `yaml:"consumer_channel_buffer_size" json:"consumer.channel.buffer.size"`

	// Group Coordinator
	ConsumerSessionTimeoutMS int `yaml:"consumer_session_timeout_ms" json:"consumer.session.timeout.ms"`
	ConsumerHeartbeatCheckMS int `yaml:"consumer_heartbeat_check_ms" json:"consumer.heartbeat.check.ms"`

	// System maintenance
	CleanupInterval      int                   `yaml:"cleanup_interval" json:"cleanup.interval"`
	AutoCreateTopics     bool                  `yaml:"auto_create_topics" json:"auto.create.topics"`
	StaticConsumerGroups []ConsumerGroupConfig `yaml:"static_consumer_groups" json:"static_consumer_groups"`

	// Security & compression (server-side)
	UseTLS      bool   `yaml:"use_tls" json:"tls.enable"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls.cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls.key_path"`
	EnableGzip  bool   `yaml:"enable_gzip" json:"gzip.enable"`
	TLSCert     tls.Certificate
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}
	configPath := flag.String("config", "", "Path to YAML/JSON config file")
	logLevelStr := flag.String("log-level", "info", "Log Level (debug, info, warn, error)")

	// Server settings
	flag.IntVar(&cfg.BrokerPort, "port", 9000, "Broker port")
	flag.IntVar(&cfg.HealthCheckPort, "health-port", 9080, "Health check server port")
	flag.BoolVar(&cfg.EnableExporter, "exporter", true, "Enable Prometheus exporter")
	flag.IntVar(&cfg.ExporterPort, "exporter-port", 9100, "Exporter port")
	flag.BoolVar(&cfg.EnableBenchmark, "benchmark", false, "Enable benchmark mode")

	// Disk persistence
	flag.StringVar(&cfg.LogDir, "log-dir", "broker-logs", "Path for logs")
	flag.IntVar(&cfg.DiskFlushBatchSize, "disk-flush-batch", 50, "Number of messages per disk flush")
	flag.IntVar(&cfg.LingerMS, "linger-ms", 50, "Maximum time to wait before flush (ms)")
	flag.IntVar(&cfg.ChannelBufferSize, "channel-buffer", 1024, "DiskHandler write channel buffer size")
	flag.IntVar(&cfg.DiskWriteTimeoutMS, "disk-write-timeout", 5, "Synchronous write timeout if channel is full (ms)")
	flag.IntVar(&cfg.SegmentSize, "segment-size", 1048576, "Segment file size in bytes (default: 1MB)")
	flag.IntVar(&cfg.SegmentRollTimeMS, "segment-roll-time-ms", 0, "Time-based segment rotation in milliseconds (0=disabled)")

	// Internal buffers
	flag.IntVar(&cfg.PartitionChannelBufSize, "partition-ch-buffer", 10000, "Partition input channel buffer size")
	flag.IntVar(&cfg.ConsumerChannelBufSize, "consumer-ch-buffer", 1000, "Consumer channel buffer size")

	// Group coordinator
	flag.IntVar(&cfg.ConsumerSessionTimeoutMS, "consumer-session-timeout", 30000, "Consumer session timeout in milliseconds")
	flag.IntVar(&cfg.ConsumerHeartbeatCheckMS, "consumer-heartbeat-check", 5000, "Heartbeat check interval in milliseconds")

	// System maintenance
	flag.IntVar(&cfg.CleanupInterval, "cleanup-interval", 300, "Cleanup interval in seconds")
	flag.BoolVar(&cfg.AutoCreateTopics, "auto-create-topics", true, "Auto-create topics on publish")

	// Security & compression
	flag.BoolVar(&cfg.UseTLS, "tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS certificate path")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS key path")
	flag.BoolVar(&cfg.EnableGzip, "gzip", false, "Enable gzip compression")

	if envPath := os.Getenv("CONFIG_PATH"); envPath != "" && *configPath == "" {
		*configPath = envPath
	}
	flag.Parse()

	// File load (YAML or JSON)
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

	if logLevelStr != nil {
		switch strings.ToLower(*logLevelStr) {
		case "debug":
			cfg.LogLevel = util.LogLevelDebug
		case "info":
			cfg.LogLevel = util.LogLevelInfo
		case "warn", "warning":
			cfg.LogLevel = util.LogLevelWarn
		case "error":
			cfg.LogLevel = util.LogLevelError
		default:
			cfg.LogLevel = util.LogLevelInfo
		}
	}

	cfg.Normalize()
	util.SetLevel(cfg.LogLevel)

	if cfg.UseTLS {
		if cfg.TLSCertPath == "" || cfg.TLSKeyPath == "" {
			cfg.UseTLS = false
			return nil, fmt.Errorf("TLS enabled but certificate or key path is empty")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			cfg.UseTLS = false
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}
		cfg.TLSCert = cert
	}

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

	if cfg.ConsumerSessionTimeoutMS <= 0 {
		cfg.ConsumerSessionTimeoutMS = 30000
	}
	if cfg.ConsumerHeartbeatCheckMS <= 0 {
		cfg.ConsumerHeartbeatCheckMS = 5000
	}
	if cfg.ConsumerHeartbeatCheckMS >= cfg.ConsumerSessionTimeoutMS {
		fmt.Fprintf(os.Stderr,
			"warning: ConsumerHeartbeatCheckMS (%d ms) >= ConsumerSessionTimeoutMS (%d ms), adjusting heartbeat to half of session timeout\n",
			cfg.ConsumerHeartbeatCheckMS, cfg.ConsumerSessionTimeoutMS,
		)
		cfg.ConsumerHeartbeatCheckMS = cfg.ConsumerSessionTimeoutMS / 2
	}
}
