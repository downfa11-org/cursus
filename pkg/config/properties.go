package config

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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

	// streaming setting
	MaxStreamConnections    int           `yaml:"max_stream_connections" json:"max.stream.connections"`
	StreamTimeout           time.Duration `yaml:"stream_timeout" json:"stream.timeout"`
	StreamHeartbeatInterval time.Duration `yaml:"stream_heartbeat_interval" json:"stream.heartbeat.interval"`

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
	portStr := flag.String("port", "9000", "Broker port")
	healthPortStr := flag.String("health-port", "9080", "Health check server port")
	logDirStr := flag.String("log-dir", "broker-logs", "Path for logs")
	exporterStr := flag.String("exporter", "true", "Enable Prometheus exporter")
	exporterPortStr := flag.String("exporter-port", "9100", "Exporter port")
	logLevelStr := flag.String("log-level", "info", "Log Level (debug, info, warn, error)")

	cleanupIntervalStr := flag.String("cleanup-interval", "300", "Cleanup interval in seconds")

	tlsStr := flag.String("tls", "false", "Enable TLS")
	tlsCertStr := flag.String("tls-cert", "", "TLS certificate path")
	tlsKeyStr := flag.String("tls-key", "", "TLS key path")
	gzipStr := flag.String("gzip", "false", "Enable gzip compression")

	diskFlushBatchStr := flag.String("disk-flush-batch", "50", "Number of messages per disk flush")
	lingerStr := flag.String("linger-ms", "50", "Maximum time to wait before flush (ms)")
	channelBufferStr := flag.String("channel-buffer", "1024", "DiskHandler write channel buffer size")
	diskWriteTimeoutStr := flag.String("disk-write-timeout", "5", "Synchronous write timeout if channel is full (ms)")
	segmentSizeStr := flag.String("segment-size", "1048576", "Segment file size in bytes (default: 1MB)")
	segmentRollTimeStr := flag.String("segment-roll-time-ms", "0", "Time-based segment rotation in milliseconds (0=disabled)")

	partitionChBufferStr := flag.String("partition-ch-buffer", "10000", "Partition input channel buffer size")
	consumerChBufferStr := flag.String("consumer-ch-buffer", "1000", "Consumer channel buffer size")

	consumerSessionTimeoutStr := flag.String("consumer-session-timeout", "30000", "Consumer session timeout in milliseconds")
	consumerHeartbeatCheckStr := flag.String("consumer-heartbeat-check", "5000", "Heartbeat check interval in milliseconds")

	autoCreateTopicsStr := flag.String("auto-create-topics", "true", "Auto-create topics on publish")

	maxStreamConn := flag.Int("max-stream-connections", 1000, "Maximum stream connections")
	streamTimeout := flag.Duration("stream-timeout", 30*time.Minute, "Stream connection timeout")
	streamHeartbeat := flag.Duration("stream-heartbeat-interval", 30*time.Second, "Stream heartbeat interval")

	if envPath := os.Getenv("CONFIG_PATH"); envPath != "" && *configPath == "" {
		*configPath = envPath
	}

	flag.Parse()

	applyDefaults(cfg, portStr, healthPortStr, logDirStr, exporterStr, exporterPortStr,
		logLevelStr, cleanupIntervalStr, tlsStr, tlsCertStr, tlsKeyStr,
		gzipStr, diskFlushBatchStr, lingerStr, channelBufferStr, diskWriteTimeoutStr,
		segmentSizeStr, segmentRollTimeStr, partitionChBufferStr, consumerChBufferStr,
		consumerSessionTimeoutStr, consumerHeartbeatCheckStr, autoCreateTopicsStr)

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

	applyExplicitFlags(cfg, portStr, healthPortStr, logDirStr, exporterStr, exporterPortStr,
		logLevelStr, cleanupIntervalStr, tlsStr, tlsCertStr, tlsKeyStr,
		gzipStr, diskFlushBatchStr, lingerStr, channelBufferStr, diskWriteTimeoutStr,
		segmentSizeStr, segmentRollTimeStr, partitionChBufferStr, consumerChBufferStr,
		consumerSessionTimeoutStr, consumerHeartbeatCheckStr, autoCreateTopicsStr, maxStreamConn, streamTimeout, streamHeartbeat)

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

func applyDefaults(cfg *Config, portStr, healthPortStr, logDirStr, exporterStr, exporterPortStr,
	logLevelStr, cleanupIntervalStr, tlsStr, tlsCertStr, tlsKeyStr,
	gzipStr, diskFlushBatchStr, lingerStr, channelBufferStr, diskWriteTimeoutStr,
	segmentSizeStr, segmentRollTimeStr, partitionChBufferStr, consumerChBufferStr,
	consumerSessionTimeoutStr, consumerHeartbeatCheckStr, autoCreateTopicsStr *string) {

	cfg.BrokerPort = util.ParseInt(*portStr, 9000)

	if healthPort, err := strconv.Atoi(*healthPortStr); err == nil {
		cfg.HealthCheckPort = healthPort
	}
	cfg.LogDir = *logDirStr
	if exporter, err := strconv.ParseBool(*exporterStr); err == nil {
		cfg.EnableExporter = exporter
	}
	if exporterPort, err := strconv.Atoi(*exporterPortStr); err == nil {
		cfg.ExporterPort = exporterPort
	}

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

	if cleanupInterval, err := strconv.Atoi(*cleanupIntervalStr); err == nil {
		cfg.CleanupInterval = cleanupInterval
	}

	if tls, err := strconv.ParseBool(*tlsStr); err == nil {
		cfg.UseTLS = tls
	}
	cfg.TLSCertPath = *tlsCertStr
	cfg.TLSKeyPath = *tlsKeyStr
	if gzip, err := strconv.ParseBool(*gzipStr); err == nil {
		cfg.EnableGzip = gzip
	}

	if diskFlushBatch, err := strconv.Atoi(*diskFlushBatchStr); err == nil {
		cfg.DiskFlushBatchSize = diskFlushBatch
	}
	if linger, err := strconv.Atoi(*lingerStr); err == nil {
		cfg.LingerMS = linger
	}
	if channelBuffer, err := strconv.Atoi(*channelBufferStr); err == nil {
		cfg.ChannelBufferSize = channelBuffer
	}
	if diskWriteTimeout, err := strconv.Atoi(*diskWriteTimeoutStr); err == nil {
		cfg.DiskWriteTimeoutMS = diskWriteTimeout
	}
	if segmentSize, err := strconv.Atoi(*segmentSizeStr); err == nil {
		cfg.SegmentSize = segmentSize
	}
	if segmentRollTime, err := strconv.Atoi(*segmentRollTimeStr); err == nil {
		cfg.SegmentRollTimeMS = segmentRollTime
	}

	if partitionChBuffer, err := strconv.Atoi(*partitionChBufferStr); err == nil {
		cfg.PartitionChannelBufSize = partitionChBuffer
	}
	if consumerChBuffer, err := strconv.Atoi(*consumerChBufferStr); err == nil {
		cfg.ConsumerChannelBufSize = consumerChBuffer
	}

	if consumerSessionTimeout, err := strconv.Atoi(*consumerSessionTimeoutStr); err == nil {
		cfg.ConsumerSessionTimeoutMS = consumerSessionTimeout
	}
	if consumerHeartbeatCheck, err := strconv.Atoi(*consumerHeartbeatCheckStr); err == nil {
		cfg.ConsumerHeartbeatCheckMS = consumerHeartbeatCheck
	}

	if autoCreateTopics, err := strconv.ParseBool(*autoCreateTopicsStr); err == nil {
		cfg.AutoCreateTopics = autoCreateTopics
	}
}

func applyExplicitFlags(cfg *Config,
	portStr, healthPortStr, logDirStr, exporterStr, exporterPortStr,
	logLevelStr, cleanupIntervalStr, tlsStr, tlsCertStr, tlsKeyStr,
	gzipStr, diskFlushBatchStr, lingerStr, channelBufferStr, diskWriteTimeoutStr,
	segmentSizeStr, segmentRollTimeStr, partitionChBufferStr, consumerChBufferStr,
	consumerSessionTimeoutStr, consumerHeartbeatCheckStr, autoCreateTopicsStr *string,
	maxStreamConn *int, streamTimeout, streamHeartbeat *time.Duration) {

	if *portStr != "9000" {
		if port, err := strconv.Atoi(*portStr); err == nil {
			cfg.BrokerPort = port
		}
	}
	if *healthPortStr != "9080" {
		if healthPort, err := strconv.Atoi(*healthPortStr); err == nil {
			cfg.HealthCheckPort = healthPort
		}
	}
	if *logDirStr != "broker-logs" {
		cfg.LogDir = *logDirStr
	}
	if *exporterStr != "true" {
		if exporter, err := strconv.ParseBool(*exporterStr); err == nil {
			cfg.EnableExporter = exporter
		}
	}
	if *exporterPortStr != "9100" {
		if exporterPort, err := strconv.Atoi(*exporterPortStr); err == nil {
			cfg.ExporterPort = exporterPort
		}
	}
	if *logLevelStr != "info" {
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
	if *cleanupIntervalStr != "300" {
		if cleanupInterval, err := strconv.Atoi(*cleanupIntervalStr); err == nil {
			cfg.CleanupInterval = cleanupInterval
		}
	}
	if *tlsStr != "false" {
		if tls, err := strconv.ParseBool(*tlsStr); err == nil {
			cfg.UseTLS = tls
		}
	}
	if *tlsCertStr != "" {
		cfg.TLSCertPath = *tlsCertStr
	}
	if *tlsKeyStr != "" {
		cfg.TLSKeyPath = *tlsKeyStr
	}
	if *gzipStr != "false" {
		if gzip, err := strconv.ParseBool(*gzipStr); err == nil {
			cfg.EnableGzip = gzip
		}
	}
	if *diskFlushBatchStr != "50" {
		if diskFlushBatch, err := strconv.Atoi(*diskFlushBatchStr); err == nil {
			cfg.DiskFlushBatchSize = diskFlushBatch
		}
	}
	if *lingerStr != "50" {
		if linger, err := strconv.Atoi(*lingerStr); err == nil {
			cfg.LingerMS = linger
		}
	}
	if *channelBufferStr != "1024" {
		if channelBuffer, err := strconv.Atoi(*channelBufferStr); err == nil {
			cfg.ChannelBufferSize = channelBuffer
		}
	}
	if *diskWriteTimeoutStr != "5" {
		if diskWriteTimeout, err := strconv.Atoi(*diskWriteTimeoutStr); err == nil {
			cfg.DiskWriteTimeoutMS = diskWriteTimeout
		}
	}
	if *segmentSizeStr != "1048576" {
		if segmentSize, err := strconv.Atoi(*segmentSizeStr); err == nil {
			cfg.SegmentSize = segmentSize
		}
	}
	if *segmentRollTimeStr != "0" {
		if segmentRollTime, err := strconv.Atoi(*segmentRollTimeStr); err == nil {
			cfg.SegmentRollTimeMS = segmentRollTime
		}
	}
	if *partitionChBufferStr != "10000" {
		if partitionChBuffer, err := strconv.Atoi(*partitionChBufferStr); err == nil {
			cfg.PartitionChannelBufSize = partitionChBuffer
		}
	}
	if *consumerChBufferStr != "1000" {
		if consumerChBuffer, err := strconv.Atoi(*consumerChBufferStr); err == nil {
			cfg.ConsumerChannelBufSize = consumerChBuffer
		}
	}
	if *consumerSessionTimeoutStr != "30000" {
		if consumerSessionTimeout, err := strconv.Atoi(*consumerSessionTimeoutStr); err == nil {
			cfg.ConsumerSessionTimeoutMS = consumerSessionTimeout
		}
	}
	if *consumerHeartbeatCheckStr != "5000" {
		if consumerHeartbeatCheck, err := strconv.Atoi(*consumerHeartbeatCheckStr); err == nil {
			cfg.ConsumerHeartbeatCheckMS = consumerHeartbeatCheck
		}
	}
	if *autoCreateTopicsStr != "true" {
		if autoCreateTopics, err := strconv.ParseBool(*autoCreateTopicsStr); err == nil {
			cfg.AutoCreateTopics = autoCreateTopics
		}
	}

	if *maxStreamConn != 1000 {
		cfg.MaxStreamConnections = *maxStreamConn
	}
	if *streamTimeout != 30*time.Minute {
		cfg.StreamTimeout = *streamTimeout
	}
	if *streamHeartbeat != 30*time.Second {
		cfg.StreamHeartbeatInterval = *streamHeartbeat
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
