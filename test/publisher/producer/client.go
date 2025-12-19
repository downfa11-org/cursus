package producer

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/publisher/config"
	"github.com/google/uuid"
)

const defaultLeaderStalenessThreshold = 30 * time.Second

type leaderInfo struct {
	addr    string
	updated time.Time
}

type ProducerState struct {
	ProducerID  string         `json:"producer_id"`
	LastSeqNums map[int]uint64 `json:"last_seq_nums"`
	Epoch       int64          `json:"epoch"`
}

type ProducerClient struct {
	ID      string
	seqNums []atomic.Uint64
	Epoch   int64
	mu      sync.RWMutex
	conns   atomic.Pointer[[]net.Conn]
	config  *config.PublisherConfig

	leader atomic.Value
}

func (pc *ProducerClient) CommitSeqRange(partition int, endSeq uint64) {
	if partition < 0 || partition >= len(pc.seqNums) {
		panic(fmt.Sprintf("invalid partition index in CommitSeqRange: %d", partition))
	}

	for {
		current := pc.seqNums[partition].Load()
		if endSeq <= current {
			return
		}
		if pc.seqNums[partition].CompareAndSwap(current, endSeq) {
			return
		}
	}
}

func NewProducerClient(partitions int, config *config.PublisherConfig) *ProducerClient {
	pc := &ProducerClient{
		ID:      uuid.New().String(),
		Epoch:   time.Now().UnixNano(),
		seqNums: make([]atomic.Uint64, partitions),
		config:  config,
	}

	pc.leader.Store(&leaderInfo{})
	if err := pc.loadState(); err != nil {
		fmt.Printf("Warning: failed to load producer state: %v\n", err)
	}
	return pc
}

func (pc *ProducerClient) loadState() error {
	data, err := os.ReadFile("producer_state.json")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var state ProducerState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	pc.ID = state.ProducerID
	pc.Epoch = state.Epoch

	for partition, seq := range state.LastSeqNums {
		if partition < len(pc.seqNums) {
			pc.seqNums[partition].Store(seq)
		}
	}
	return nil
}

func (pc *ProducerClient) NextSeqNum(partition int) uint64 {
	if partition < 0 || partition >= len(pc.seqNums) {
		panic(fmt.Sprintf("invalid partition index: %d", partition))
	}
	return pc.seqNums[partition].Add(1)
}

func (pc *ProducerClient) connectPartitionLocked(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	if idx < 0 {
		return fmt.Errorf("invalid partition index: %d", idx)
	}

	var conn net.Conn
	var err error

	if useTLS {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}
		conn, err = tls.Dial("tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err != nil {
			return fmt.Errorf("TLS dial to %s failed: %w", addr, err)
		}
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("TCP dial to %s failed: %w", addr, err)
		}
	}

	var currentConns []net.Conn
	if ptr := pc.conns.Load(); ptr != nil {
		currentConns = *ptr
	}

	newSize := idx + 1
	if len(currentConns) > newSize {
		newSize = len(currentConns)
	}

	tmp := make([]net.Conn, newSize)
	copy(tmp, currentConns)
	tmp[idx] = conn

	pc.conns.Store(&tmp)
	return nil
}

func (pc *ProducerClient) GetConn(part int) net.Conn {
	ptr := pc.conns.Load()
	if ptr == nil {
		return nil
	}
	conns := *ptr
	if part >= 0 && part < len(conns) {
		return conns[part]
	}
	return nil
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	ptr := pc.conns.Swap(nil)
	if ptr == nil {
		return nil
	}

	conns := *ptr
	for i, c := range conns {
		if c != nil {
			_ = c.Close()
			conns[i] = nil
		}
	}

	return nil
}

func (pc *ProducerClient) GetLeaderAddr() string {
	info, ok := pc.leader.Load().(*leaderInfo)
	if !ok || info.addr == "" {
		return ""
	}
	return info.addr
}

func (pc *ProducerClient) UpdateLeader(leaderAddr string) {
	old := pc.leader.Load().(*leaderInfo)
	if old.addr != leaderAddr {
		pc.leader.Store(&leaderInfo{
			addr:    leaderAddr,
			updated: time.Now(),
		})
	}
}

func (pc *ProducerClient) selectBroker() string {
	if pc.config == nil || len(pc.config.BrokerAddrs) == 0 {
		return ""
	}

	threshold := defaultLeaderStalenessThreshold
	info, ok := pc.leader.Load().(*leaderInfo)
	if ok && info.addr != "" && time.Since(info.updated) < threshold {
		return info.addr
	}

	return pc.config.BrokerAddrs[0]
}

func (pc *ProducerClient) ConnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	if addr == "" {
		addr = pc.selectBroker()
	}
	if addr == "" {
		return fmt.Errorf("no broker address available for partition %d", idx)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}

func (pc *ProducerClient) ReconnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	oldPtr := pc.conns.Load()
	if oldPtr != nil {
		conns := *oldPtr
		if idx < len(conns) && conns[idx] != nil {
			_ = conns[idx].Close()
		}
	}

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}
