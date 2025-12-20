package controller

import (
	"fmt"
	"net"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
)

type LocalProcessor interface {
	ProcessCommand(cmd string) string
}

type ClusterRouter struct {
	LocalAddr      string
	brokerID       string
	rm             *replication.RaftReplicationManager
	clientPort     int
	timeout        time.Duration
	localProcessor LocalProcessor
}

func NewClusterRouter(brokerID, localAddr string, processor LocalProcessor, rm *replication.RaftReplicationManager, clientPort int) *ClusterRouter {
	return &ClusterRouter{
		brokerID:       brokerID,
		LocalAddr:      localAddr,
		rm:             rm,
		clientPort:     clientPort,
		timeout:        5 * time.Second,
		localProcessor: processor,
	}
}

func (r *ClusterRouter) getLeader() (string, error) {
	leader := r.rm.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no leader available from Raft")
	}
	return leader, nil
}

func (r *ClusterRouter) ForwardToLeader(req string) (string, error) {
	leader, err := r.getLeader()
	if err != nil {
		return "", err
	}

	if r.rm.IsLeader() || leader == r.LocalAddr {
		return r.processLocally(req), nil
	}

	host, _, splitErr := net.SplitHostPort(leader)
	if splitErr != nil {
		return "", fmt.Errorf("invalid leader address format: %w", splitErr)
	}

	clientLeaderAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	return r.sendRequest(clientLeaderAddr, req)
}

func (r *ClusterRouter) ForwardDataToLeader(data []byte) (string, error) {
	leader, err := r.getLeader()
	if err != nil {
		return "", err
	}

	if r.rm.IsLeader() || leader == r.LocalAddr {
		return "", fmt.Errorf("internal routing error: cannot forward batch data to self")
	}

	host, _, splitErr := net.SplitHostPort(leader)
	if splitErr != nil {
		return "", fmt.Errorf("invalid leader address format: %w", splitErr)
	}

	clientLeaderAddr := fmt.Sprintf("%s:%d", host, r.clientPort)
	return r.sendDataRequest(clientLeaderAddr, data)
}

func (r *ClusterRouter) processLocally(req string) string {
	if r.localProcessor != nil {
		return r.localProcessor.ProcessCommand(req)
	}
	return "ERROR: no local processor configured"
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
