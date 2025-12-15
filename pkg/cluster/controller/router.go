package controller

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

type LocalProcessor interface {
	ProcessCommand(cmd string) string
}

type ClusterRouter struct {
	brokerID       string
	localAddr      string
	leaderAddr     string
	leaderAddrMu   sync.RWMutex
	timeout        time.Duration
	localProcessor LocalProcessor
	raft           *raft.Raft
	clientPort     int
}

func NewClusterRouter(brokerID, localAddr string, processor LocalProcessor, raft *raft.Raft, clientPort int) *ClusterRouter {
	router := &ClusterRouter{
		brokerID:       brokerID,
		localAddr:      localAddr,
		timeout:        5 * time.Second,
		localProcessor: processor,
		raft:           raft,
		clientPort:     clientPort,
	}

	go func() {
		time.Sleep(2 * time.Second)
		if _, err := router.getLeader(); err != nil {
			util.Debug("Initial leader discovery failed: %v", err)
		}
	}()

	return router
}

func (r *ClusterRouter) SetLocalProcessor(processor LocalProcessor) {
	r.localProcessor = processor
}

func (r *ClusterRouter) StartLeaderRefresh() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if _, err := r.getLeader(); err != nil {
				util.Debug("Periodic leader refresh failed: %v", err)
			}
		}
	}()
}

func (r *ClusterRouter) getLeader() (string, error) {
	if r.raft != nil {
		currentLeaderAddr := string(r.raft.Leader())
		if currentLeaderAddr != "" {
			r.leaderAddrMu.Lock()

			if r.leaderAddr != currentLeaderAddr {
				util.Info("✅ Raft Leader changed: %s -> %s", r.leaderAddr, currentLeaderAddr)
				r.leaderAddr = currentLeaderAddr
			}

			r.leaderAddrMu.Unlock()
			return currentLeaderAddr, nil
		}
	}
	return "", fmt.Errorf("no leader available from Raft")
}

func (r *ClusterRouter) GetCachedLeader() (string, error) {
	r.leaderAddrMu.RLock()
	defer r.leaderAddrMu.RUnlock()
	return r.leaderAddr, nil
}

func (r *ClusterRouter) GetLocalAddr() string {
	return r.localAddr
}

func (r *ClusterRouter) processLocally(req string) string {
	if r.localProcessor != nil {
		return r.localProcessor.ProcessCommand(req)
	}
	return "ERROR: no local processor configured"
}

func (r *ClusterRouter) ForwardToLeader(req string) (string, error) {
	leader, err := r.getLeader()
	if err != nil {
		return "", err
	}

	if leader == r.localAddr {
		util.Info("you're leader node.")
		return r.processLocally(req), nil
	}

	host, _, splitErr := net.SplitHostPort(leader)
	if splitErr != nil {
		return "", fmt.Errorf("invalid leader address format: %w", splitErr)
	}

	clientLeaderAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	resp, err := r.sendRequest(clientLeaderAddr, req)
	util.Info("Forwarding [%s] to leader: %s -> %s", req, r.GetLocalAddr(), clientLeaderAddr)
	return resp, err
}

// ForwardDataToLeader forwards a binary data packet (like a batch message) to the cluster leader.
func (r *ClusterRouter) ForwardDataToLeader(data []byte) (string, error) {
	leader, err := r.getLeader()
	if err != nil {
		return "", err
	}

	if leader == r.localAddr {
		util.Warn("Attempted to forward data to self as leader.")
		return "", fmt.Errorf("internal routing error: cannot forward batch data to self")
	}

	host, _, splitErr := net.SplitHostPort(leader)
	if splitErr != nil {
		return "", fmt.Errorf("invalid leader address format: %w", splitErr)
	}

	clientLeaderAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	resp, err := r.sendDataRequest(clientLeaderAddr, data)
	util.Info("Forwarding data to leader: %s -> %s (Data size: %d)", r.GetLocalAddr(), clientLeaderAddr, len(data))
	return resp, err
}

func (r *ClusterRouter) sendRequest(addr, command string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return "", err
	}

	data := []byte(command)
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(len(data) >> 24)
	lenBuf[1] = byte(len(data) >> 16)
	lenBuf[2] = byte(len(data) >> 8)
	lenBuf[3] = byte(len(data))

	if _, err := conn.Write(lenBuf); err != nil {
		return "", err
	}
	if _, err := conn.Write(data); err != nil {
		return "", err
	}

	respLenBuf := make([]byte, 4)
	if _, err := conn.Read(respLenBuf); err != nil {
		return "", err
	}

	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 |
		uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])

	respBuf := make([]byte, respLen)
	if _, err := conn.Read(respBuf); err != nil {
		return "", err
	}

	return string(respBuf), nil
}

// sendDataRequest sends raw data (byte slice) to the specified address and expects a response string.
func (r *ClusterRouter) sendDataRequest(addr string, data []byte) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return "", err
	}

	lenBuf := make([]byte, 4)
	dataLen := len(data)
	lenBuf[0] = byte(dataLen >> 24)
	lenBuf[1] = byte(dataLen >> 16)
	lenBuf[2] = byte(dataLen >> 8)
	lenBuf[3] = byte(dataLen)

	if _, err := conn.Write(lenBuf); err != nil {
		return "", err
	}

	if _, err := conn.Write(data); err != nil {
		return "", err
	}

	respLenBuf := make([]byte, 4)
	if _, err := conn.Read(respLenBuf); err != nil {
		return "", err
	}

	respLen := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])

	respBuf := make([]byte, respLen)
	if _, err := conn.Read(respBuf); err != nil {
		return "", err
	}

	return string(respBuf), nil
}
