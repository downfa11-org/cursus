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

	"github.com/google/uuid"
)

type ProducerState struct {
	ProducerID   string         `json:"producer_id"`
	LastSeqNums  map[int]uint64 `json:"last_seq_nums"`
	Epoch        int64          `json:"epoch"`
	GlobalSeqNum uint64         `json:"global_seq_num"`
}

type ProducerClient struct {
	ID           string
	seqNums      []atomic.Uint64
	globalSeqNum atomic.Uint64
	Epoch        int64
	mu           sync.Mutex
	conns        []net.Conn
}

func (pc *ProducerClient) ReserveSeqRange(partition int, count int) (uint64, uint64) {
	start := pc.globalSeqNum.Add(uint64(count)) - uint64(count-1)
	end := start + uint64(count-1)
	return start, end
}

func (pc *ProducerClient) CommitSeqRange(partition int, endSeq uint64) {
	pc.seqNums[partition].Store(endSeq)
}

func NewProducerClient(partitions int) *ProducerClient {
	pc := &ProducerClient{
		ID:      uuid.New().String(),
		Epoch:   time.Now().UnixNano(),
		seqNums: make([]atomic.Uint64, partitions),
	}
	pc.loadState()
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
	pc.globalSeqNum.Store(state.GlobalSeqNum)

	for partition, seq := range state.LastSeqNums {
		if partition < len(pc.seqNums) {
			pc.seqNums[partition].Store(seq)
		}
	}
	return nil
}

func (pc *ProducerClient) SaveState() error {
	lastSeqNums := make(map[int]uint64)
	for i := range pc.seqNums {
		lastSeqNums[i] = pc.seqNums[i].Load()
	}

	state := ProducerState{
		ProducerID:   pc.ID,
		LastSeqNums:  lastSeqNums,
		Epoch:        pc.Epoch,
		GlobalSeqNum: pc.globalSeqNum.Load(),
	}

	data, _ := json.MarshalIndent(state, "", "  ")
	tmpFile := "producer_state.json.tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return err
	}

	f.Close()
	return os.Rename(tmpFile, "producer_state.json")
}

func (pc *ProducerClient) NextSeqNum(partition int) uint64 {
	return pc.globalSeqNum.Add(1)
}

func (pc *ProducerClient) ConnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}

func (pc *ProducerClient) connectPartitionLocked(idx int, addr string, useTLS bool, certPath, keyPath string) error {
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

	if len(pc.conns) <= idx {
		tmp := make([]net.Conn, idx+1)
		copy(tmp, pc.conns)
		pc.conns = tmp
	}
	pc.conns[idx] = conn
	return nil
}

func (pc *ProducerClient) ReconnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if idx < len(pc.conns) && pc.conns[idx] != nil {
		_ = pc.conns[idx].Close()
		pc.conns[idx] = nil
	}

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}

func (pc *ProducerClient) GetConn(part int) net.Conn {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if part < len(pc.conns) {
		return pc.conns[part]
	}
	return nil
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for i, c := range pc.conns {
		if c != nil {
			_ = c.Close()
			pc.conns[i] = nil
		}
	}
	return nil
}
