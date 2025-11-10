package config

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

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
	Acks                       string   `yaml:"acks" json:"acks"`
	MinInsyncReplicas          int      `yaml:"min_insync_replicas" json:"min.insync.replicas"`
	BufferSize                 int      `yaml:"buffer_size" json:"buffer.size"`
	BatchSize                  int      `yaml:"batch_size" json:"batch.size"`
	MaxInflightRequestsPerConn int      `yaml:"max_inflight_requests_per_conn" json:"max.inflight.requests.per.connection"`
}

// LoadConfig merges CLI, YAML, and JSON configs.
func LoadConfig() (*Config, error) {
	cfg := &Config{}

	// default
	flag.IntVar(&cfg.BrokerPort, "port", 9000, "Broker port")
	flag.IntVar(&cfg.HealthCheckPort, "health-port", 9080, "Health check server port")
	flag.StringVar(&cfg.LogDir, "log-dir", "broker_logs", "Path for logs")
	flag.BoolVar(&cfg.EnableExporter, "exporter", true, "Enable Prometheus exporter")
	flag.IntVar(&cfg.ExporterPort, "exporter-port", 9100, "Exporter port")
	flag.BoolVar(&cfg.EnableBenchmark, "benchmark", false, "Enable benchmark mode")

	flag.IntVar(&cfg.CleanupInterval, "cleanup-interval", 300, "Cleanup interval in seconds")

	flag.BoolVar(&cfg.UseTLS, "tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS certificate path")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS key path")
	flag.BoolVar(&cfg.EnableGzip, "gzip", true, "Enable gzip compression")

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

	// bootstrap string split
	if len(cfg.BootstrapServers) == 1 && strings.Contains(cfg.BootstrapServers[0], ",") {
		cfg.BootstrapServers = strings.Split(cfg.BootstrapServers[0], ",")
	}

	if cfg.UseTLS && cfg.TLSCertPath != "" && cfg.TLSKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			return nil, err
		}
		cfg.TLSCert = cert
	}

	return cfg, nil
}
