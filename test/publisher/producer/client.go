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

	leaderAddr       string
	lastLeaderUpdate time.Time
}

func (pc *ProducerClient) CommitSeqRange(partition int, endSeq uint64) {
	if partition < 0 || partition >= len(pc.seqNums) {
		panic(fmt.Sprintf("invalid partition index in CommitSeqRange: %d", partition))
	}

	current := pc.seqNums[partition].Load()
	if endSeq > current {
		pc.seqNums[partition].Store(endSeq)
	}
}

func NewProducerClient(partitions int, config *config.PublisherConfig) *ProducerClient {
	pc := &ProducerClient{
		ID:      uuid.New().String(),
		Epoch:   time.Now().UnixNano(),
		seqNums: make([]atomic.Uint64, partitions),
		config:  config,
	}
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
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.leaderAddr
}

func (pc *ProducerClient) UpdateLeader(leaderAddr string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.leaderAddr != leaderAddr {
		pc.leaderAddr = leaderAddr
		pc.lastLeaderUpdate = time.Now()
	}
}

func (pc *ProducerClient) selectBroker() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.config == nil || len(pc.config.BrokerAddrs) == 0 {
		return ""
	}

	if pc.leaderAddr != "" && time.Since(pc.lastLeaderUpdate) < 30*time.Second {
		return pc.leaderAddr
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
	oldPtr := pc.conns.Load()
	if oldPtr != nil {
		conns := *oldPtr
		if idx < len(conns) && conns[idx] != nil {
			_ = conns[idx].Close()
		}
	}

	return pc.ConnectPartition(idx, addr, useTLS, certPath, keyPath)
}
