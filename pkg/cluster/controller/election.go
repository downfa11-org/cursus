package controller

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

type ControllerElection struct {
	rm       *replication.RaftReplicationManager
	isLeader atomic.Bool
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
	util.Info("Starting controller leadership monitoring")
	go ce.monitorLeadership()
}

func (ce *ControllerElection) Stop() {
	ce.cancel()
}

func (ce *ControllerElection) monitorLeadership() {
	util.Debug("Starting leadership monitor loop")
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ce.ctx.Done():
			util.Info("Leadership monitor stopping: context cancelled")
			return
		case <-ticker.C:
			isLeader := ce.rm.GetRaft().State() == raft.Leader
			if isLeader != ce.isLeader.Load() {
				ce.isLeader.Store(isLeader)
				if isLeader {
					util.Info("Became cluster leader")
				} else {
					util.Info("Lost cluster leadership")
				}
				ce.leaderCh <- isLeader
			}
		}
	}
}

func (ce *ControllerElection) IsLeader() bool {
	return ce.isLeader.Load()
}
