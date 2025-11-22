package config_test

import (
	"testing"

	"github.com/downfa11-org/go-broker/pkg/config"
)

func TestNormalizeDefaults(t *testing.T) {
	cfg := &config.Config{}
	cfg.Normalize()

	if cfg.BrokerPort != 9000 {
		t.Errorf("BrokerPort default incorrect: %d", cfg.BrokerPort)
	}
	if cfg.MaxPollRecords != 8192 {
		t.Errorf("MaxPollRecords default incorrect: %d", cfg.MaxPollRecords)
	}
	if cfg.SegmentSize != 1<<20 {
		t.Errorf("SegmentSize default incorrect: %d", cfg.SegmentSize)
	}
	if len(cfg.BootstrapServers) == 0 {
		t.Errorf("BootstrapServers should fallback to default")
	}
}

func TestStaticConsumerGroupsNormalize(t *testing.T) {
	cfg := &config.Config{
		StaticConsumerGroups: []config.ConsumerGroupConfig{
			{
				Name:          "",
				ConsumerCount: 0,
				Topics:        []string{"a"},
				TopicPartitions: map[string]int{
					"a": 0,
				},
			},
		},
	}

	cfg.Normalize()
	g := cfg.StaticConsumerGroups[0]

	if g.Name != "default-group" {
		t.Errorf("Name normalization failed")
	}
	if g.ConsumerCount != 1 {
		t.Errorf("ConsumerCount normalization failed")
	}
	if g.TopicPartitions["a"] != 1 {
		t.Errorf("TopicPartitions normalization failed")
	}
}

func TestAckNormalization(t *testing.T) {
	cfg := &config.Config{Acks: "garbage"}
	cfg.Normalize()

	if cfg.Acks != "0" {
		t.Errorf("Acks normalization failed: %s", cfg.Acks)
	}
}
