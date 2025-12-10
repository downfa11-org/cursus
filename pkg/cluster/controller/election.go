package controller

import (
	"context"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/hashicorp/raft"
)

type ControllerElection struct {
	rm       *replication.RaftReplicationManager
	isLeader bool
	leaderCh chan bool
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewControllerElection(rm *replication.RaftReplicationManager) *ControllerElection {
	ctx, cancel := context.WithCancel(context.Background())
	return &ControllerElection{
		rm:       rm,
		leaderCh: make(chan bool, 1),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (ce *ControllerElection) Start() {
	go ce.monitorLeadership()
}

func (ce *ControllerElection) monitorLeadership() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ce.ctx.Done():
			return
		case <-ticker.C:
			isLeader := ce.rm.GetRaft().State() == raft.Leader
			if isLeader != ce.isLeader {
				ce.isLeader = isLeader
				ce.leaderCh <- isLeader
			}
		}
	}
}

func (ce *ControllerElection) IsLeader() bool {
	return ce.isLeader
}
