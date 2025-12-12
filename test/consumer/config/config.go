package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/util"
	"gopkg.in/yaml.v3"
)

type ConsumerMode string

const (
	ModePolling   ConsumerMode = "polling"
	ModeStreaming ConsumerMode = "streaming"
)

type ConsumerConfig struct {
	BrokerAddrs        []string     `yaml:"broker_addrs" json:"broker_addrs"`
	CurrentBrokerIndex int          `yaml:"-" json:"-"`
	mu                 sync.RWMutex `yaml:"-" json:"-"`

	Topic      string `yaml:"topic" json:"topic"`
	GroupID    string `yaml:"group_id" json:"group_id"`
	ConsumerID string `yaml:"consumer_id" json:"consumer_id"`

	EnableBenchmark bool         `yaml:"enable_benchmark" json:"enable_benchmark"`
	NumMessages     int          `yaml:"num_messages" json:"num_messages"`
	Mode            ConsumerMode `yaml:"mode" json:"mode"`

	PollInterval  time.Duration `yaml:"poll_interval" json:"poll_interval"`
	PollTimeoutMS int           `yaml:"poll_timeout_ms" json:"poll_timeout_ms"`
	BatchSize     int           `yaml:"batch_size" json:"batch_size"`

	SessionTimeoutMS         int `yaml:"session_timeout_ms" json:"session_timeout_ms"`
	MaxPollRecords           int `yaml:"max_poll_records" json:"max_poll_records"`
	MaxConnectRetries        int `yaml:"max_connect_retries" json:"max_connect_retries"`
	ConnectRetryBackoffMS    int `yaml:"connect_retry_backoff_ms" json:"connect_retry_backoff_ms"`
	HeartbeatIntervalMS      int `yaml:"heartbeat_interval_ms" json:"heartbeat_interval_ms"`
	StreamingReadDeadlineMS  int `yaml:"streaming_read_deadline_ms" json:"streaming_read_deadline_ms"`
	StreamingRetryIntervalMS int `yaml:"streaming_retry_interval_ms" json:"streaming_retry_interval_ms"`

	EnableAutoCommit   bool          `yaml:"enable_auto_commit" json:"enable_auto_commit"`
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval" json:"auto_commit_interval"`

	EnableGzip bool `yaml:"enable_gzip" json:"enable_gzip"`

	LogLevel util.LogLevel `yaml:"log_level" json:"log_level"`
}

func LoadConfig() (*ConsumerConfig, error) {
	cfg := &ConsumerConfig{}

	flag.Func("broker-addr", "Comma-separated broker addresses", func(val string) error {
		cfg.BrokerAddrs = strings.Split(val, ",")
		for i, addr := range cfg.BrokerAddrs {
			cfg.BrokerAddrs[i] = strings.TrimSpace(addr)
		}
		return nil
	})

	flag.StringVar(&cfg.ConsumerID, "consumer-id", "consumer-1", "Consumer ID")
	flag.StringVar(&cfg.GroupID, "group-id", "default-group", "Consumer group ID")
	flag.StringVar(&cfg.Topic, "topic", "", "Topic to consume")

	flag.DurationVar(&cfg.PollInterval, "poll-interval", 500*time.Millisecond, "Poll interval")
	flag.IntVar(&cfg.PollTimeoutMS, "poll-timeout-ms", 30000, "Maximum time in milliseconds to wait for new messages in a poll (Long Polling)")

	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Batch size for consuming")
	flag.DurationVar(&cfg.AutoCommitInterval, "auto-commit-interval", 5*time.Second, "Auto commit interval")

	flag.IntVar(&cfg.MaxPollRecords, "max-poll-records", 500, "Max records per poll")
	flag.BoolVar(&cfg.EnableAutoCommit, "enable-auto-commit", true, "Enable auto commit")
	flag.IntVar(&cfg.SessionTimeoutMS, "session-timeout-ms", 30000, "Session timeout in milliseconds")
	flag.BoolVar(&cfg.EnableGzip, "enable-gzip", false, "Enable gzip compression")

	benchmarkFlag := flag.Bool("benchmark", false, "Enable benchmark mode with detailed metrics")
	flag.IntVar(&cfg.NumMessages, "num-messages", 10, "Number of messages to publish")

	configPath := flag.String("config", "/config.yaml", "Path to YAML/JSON config file")
	logLevelStr := flag.String("log-level", "info", "Log level")

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

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			if os.IsNotExist(err) {
				util.Error("Config file %s not found, using flag defaults", *configPath)
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

	if len(cfg.BrokerAddrs) == 0 {
		cfg.BrokerAddrs = []string{"localhost:9000"}
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.PollTimeoutMS == 0 {
		cfg.PollTimeoutMS = 30000
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.AutoCommitInterval == 0 {
		cfg.AutoCommitInterval = 5 * time.Second
	}
	if cfg.MaxPollRecords == 0 {
		cfg.MaxPollRecords = 500
	}
	if cfg.Mode == "" {
		cfg.Mode = ModePolling
	}
	if cfg.MaxConnectRetries == 0 {
		cfg.MaxConnectRetries = 5
	}
	if cfg.ConnectRetryBackoffMS == 0 {
		cfg.ConnectRetryBackoffMS = 1000
	}
	if cfg.HeartbeatIntervalMS == 0 {
		cfg.HeartbeatIntervalMS = cfg.SessionTimeoutMS / 2
	}
	if cfg.StreamingReadDeadlineMS == 0 {
		cfg.StreamingReadDeadlineMS = 5 * 60 * 1000 // 5min
	}
	if cfg.StreamingRetryIntervalMS == 0 {
		cfg.StreamingRetryIntervalMS = 50
	}

	if *benchmarkFlag {
		cfg.EnableBenchmark = true
	}

	if cfg.EnableBenchmark {
		if cfg.NumMessages <= 0 {
			cfg.NumMessages = 10
		}
	}

	util.SetLevel(cfg.LogLevel)
	return cfg, nil
}
