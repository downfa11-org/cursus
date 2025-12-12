package server

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster"
	clusterClient "github.com/downfa11-org/go-broker/pkg/cluster/client"
	clusterController "github.com/downfa11-org/go-broker/pkg/cluster/controller"
	"github.com/downfa11-org/go-broker/pkg/cluster/delivery"
	"github.com/downfa11-org/go-broker/pkg/cluster/discovery"
	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/cluster/routing"
	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/controller"
	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/stream"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const (
	maxWorkers             = 1000
	DefaultHealthCheckPort = 9080
)

var brokerReady = &atomic.Bool{}

// RunServer starts the broker with optional TLS and gzip
func RunServer(
	cfg *config.Config,
	tm *topic.TopicManager,
	dm *disk.DiskManager,
	cd *coordinator.Coordinator,
	sm *stream.StreamManager,
) error {

	if cfg.EnableExporter {
		metrics.StartMetricsServer(cfg.ExporterPort)
		util.Info("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		util.Info("📉 Exporter disabled")
	}

	addr := fmt.Sprintf(":%d", cfg.BrokerPort)
	var ln net.Listener
	var err error
	if cfg.UseTLS {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cfg.TLSCert},
			MinVersion:   tls.VersionTLS12,
		}
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}

	util.Info("🧩 Broker listening on %s (TLS=%v, Compression=%v)", addr, cfg.UseTLS, cfg.CompressionType)
	brokerReady.Store(true)

	healthPort := cfg.HealthCheckPort
	if healthPort == 0 {
		healthPort = DefaultHealthCheckPort
	}
	startHealthCheckServer(healthPort, brokerReady)

	if cd != nil {
		cd.Start()
		util.Info("🔄 Coordinator started with heartbeat monitoring")
	}

	var sd discovery.ServiceDiscovery
	var rm *replication.RaftReplicationManager
	var md *delivery.MessageDelivery

	if cfg.EnabledDistribution {
		brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
		localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
		raftServerID := cfg.AdvertisedHost

		var err error
		clusterClient := clusterClient.TCPClusterClient{}
		rm, err = replication.NewRaftReplicationManager(cfg, raftServerID, dm, clusterClient)
		if err != nil {
			return fmt.Errorf("failed to create raft replication manager: %w", err)
		}

		fsm := rm.GetFSM()
		sd = discovery.NewServiceDiscovery(fsm, brokerID, localAddr, rm.GetRaft())

		discoveryAddr := fmt.Sprintf(":%d", cfg.DiscoveryPort)
		cs := cluster.NewClusterServer(sd)
		go func() {
			if _, err := cs.Start(discoveryAddr); err != nil {
				util.Error("discovery-server start error: %v", err)
			}
		}()

		sd.SetRaftManager(rm)

		cc := clusterController.NewClusterController(rm, sd, tm)
		isrManager := replication.NewISRManager()
		cc.SetISRManager(isrManager)

		controllerElection := clusterController.NewControllerElection(rm)
		controllerElection.Start()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			util.Info("🔄 Starting cluster leader election monitor...")
			for isLeader := range controllerElection.LeaderCh() {
				if isLeader {
					util.Info("🎉 Became cluster leader! Registering self and starting controller.")
					if regErr := sd.Register(); regErr != nil {
						util.Error("❌ Failed to register as leader, attempting to step down: %v", regErr)
						continue
					}
					cc.Start(ctx)
				} else {
					util.Info("💀 Lost cluster leadership. Stopping controller functions.")
				}
			}
		}()

		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if controllerElection.IsLeader() {
						if err := cc.RebalanceToPreferredLeaders(); err != nil {
							util.Error("Preferred leader rebalance failed: %v", err)
						}
					}
				}
			}
		}()
		md = delivery.NewMessageDelivery(cc, brokerID, localAddr, 5*time.Second)
		util.Info("🌐 Distributed clustering enabled (brokerID=%s, localAddr=%s)", brokerID, localAddr)
	}

	workerCh := make(chan net.Conn, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for conn := range workerCh {
				HandleConnection(conn, tm, dm, cfg, cd, sm, sd, rm, md)
			}
		}()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			util.Error("⚠️ Accept error: %v", err)
			continue
		}
		workerCh <- conn
	}
}

// HandleConnection processes a single client connection
func HandleConnection(
	conn net.Conn,
	tm *topic.TopicManager,
	dm *disk.DiskManager,
	cfg *config.Config,
	cd *coordinator.Coordinator,
	sm *stream.StreamManager,
	sd discovery.ServiceDiscovery,
	rm *replication.RaftReplicationManager,
	md *delivery.MessageDelivery,
) {
	defer conn.Close()

	var router *routing.ClientRouter
	if cfg.EnabledDistribution && sd != nil {
		brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
		localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.BrokerPort)
		router = routing.NewClientRouter(sd, brokerID, localAddr)
	}

	cmdHandler := controller.NewCommandHandler(tm, dm, cfg, cd, sm, router)
	ctx := controller.NewClientContext("default-group", 0)

	writeTimeout := 10 * time.Second

	for {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Error("⚠️ SetReadDeadline error: %v", err)
			return
		}

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				util.Error("⚠️ Read length error: %v", err)
			}
			return
		}

		msgLen := binary.BigEndian.Uint32(lenBuf)
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			if err != io.EOF {
				util.Error("⚠️ Read message error: %v (len=%d)", err, len(msgBuf))
			}
			return
		}

		data, err := util.DecompressMessage(msgBuf, cfg.CompressionType)
		if err != nil {
			util.Error("⚠️ Decompress error: %v", err)
			return
		}

		topicName, payload := util.DecodeMessage(data)

		if strings.HasPrefix(strings.ToUpper(payload), "HEARTBEAT") {
			writeResponseWithTimeout(conn, "OK", writeTimeout)
			continue
		}

		if strings.HasPrefix(strings.ToUpper(payload), "JOIN_GROUP") ||
			strings.HasPrefix(strings.ToUpper(payload), "SYNC_GROUP") {
			resp := cmdHandler.HandleCommand(payload, ctx)
			writeResponse(conn, resp)
			continue
		}

		var resp string
		if isCommand(payload) {
			resp = cmdHandler.HandleCommand(payload, ctx)
			if resp == controller.STREAM_DATA_SIGNAL {
				if strings.HasPrefix(strings.ToUpper(payload), "STREAM ") {
					if err := cmdHandler.HandleStreamCommand(conn, payload, ctx); err != nil {
						writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					}
					return
				} else {
					if _, err := cmdHandler.HandleConsumeCommand(conn, payload, ctx); err != nil {
						writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					}
					continue
				}
			}
			if resp != "" {
				writeResponse(conn, resp)
				continue
			}
		}

		if isBatchMessage(data) {
			batch, err := util.DecodeBatchMessages(data)
			if err != nil {
				util.Error("Batch message decoding failed: %v", err)
				writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
				continue
			}

			if len(batch.Messages) == 0 {
				writeResponse(conn, "ERROR: empty batch")
				continue
			}

			if err := tm.PublishBatchSync(batch.Topic, batch.Messages); err != nil {
				writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
				continue

			}

			var lastOffset uint64
			var seqStart, seqEnd uint64
			var producerID string
			var producerEpoch int64

			lastOffset = batch.Messages[len(batch.Messages)-1].Offset
			seqStart = batch.Messages[0].SeqNum
			seqEnd = batch.Messages[len(batch.Messages)-1].SeqNum
			producerID = batch.Messages[0].ProducerID
			producerEpoch = batch.Messages[0].Epoch

			ackResp := types.AckResponse{
				Status:        "OK",
				LastOffset:    lastOffset,
				ProducerID:    producerID,
				ProducerEpoch: producerEpoch,
				SeqStart:      seqStart,
				SeqEnd:        seqEnd,
			}

			ackBytes, err := json.Marshal(ackResp)
			if err != nil {
				util.Error("Failed to marshal AckResponse: %v", err)
				writeResponse(conn, "ERROR: internal marshal error")
				continue
			}
			writeResponse(conn, string(ackBytes))
			continue
		}

		if topicName == "" || payload == "" {
			rawInput := strings.TrimSpace(string(data))
			util.Debug("[%s] Received unrecognized input: %s", conn.RemoteAddr().String(), rawInput)
			writeResponse(conn, "ERROR: malformed input - missing topic or payload")
			return
		}

		acks, message := extractAcksAndMessage(payload)
		msg := &types.Message{Payload: message}

		switch acks {
		case "0":
			if err := tm.Publish(topicName, msg); err != nil {
				writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
				continue
			}
			writeResponse(conn, "OK")
		case "1":
			if cfg.EnabledDistribution && rm != nil {
				if err := rm.ReplicateToLeader(topicName, 0, *msg); err != nil {
					writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					continue
				}
			} else {
				if err := tm.PublishWithAck(topicName, msg); err != nil {
					writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					continue
				}
			}
			writeResponse(conn, "OK")
		case "-1", "all":
			if cfg.EnabledDistribution && rm != nil {
				if err := rm.ReplicateWithQuorum(topicName, 0, *msg, cfg.MinInSyncReplicas); err != nil {
					writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					continue
				}
				writeResponse(conn, "OK")
			} else {
				writeResponse(conn, "ERROR: acks=all requires distributed clustering")
				continue
			}
		default:
			writeResponse(conn, fmt.Sprintf("ERROR: invalid acks: %s", acks))
		}
	}
}

// writeResponseWithTimeout adds write timeout
func writeResponseWithTimeout(conn net.Conn, msg string, timeout time.Duration) {
	resp := []byte(msg)
	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		util.Error("⚠️ SetWriteDeadline error: %v", err)
		return
	}

	if _, err := conn.Write(respLen); err != nil {
		util.Error("⚠️ Write length error: %v", err)
		return
	}
	if _, err := conn.Write(resp); err != nil {
		util.Error("⚠️ Write response error: %v", err)
		return
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		util.Error("Failed to reset write deadline: %v", err)
	}
}

// isBatchMessage checks if the data is in binary batch format
func isBatchMessage(data []byte) bool {
	if len(data) < 6 {
		return false
	}
	if data[0] != 0xBA || data[1] != 0x7C {
		return false
	}

	topicLen := binary.BigEndian.Uint16(data[2:4])
	if topicLen == 0 || int(topicLen)+2 > len(data) {
		return false
	}
	return true
}

// extractAcksAndMessage parses acks level and message from payload
func extractAcksAndMessage(payload string) (acks, message string) {
	parts := strings.SplitN(payload, ";", 2)
	if len(parts) == 2 && strings.HasPrefix(parts[0], "acks=") {
		acks = strings.TrimPrefix(parts[0], "acks=")
		message = strings.TrimPrefix(parts[1], "message=")
		return
	}
	// Default for backward compatibility
	acks = "0"
	message = payload
	return
}

func isCommand(s string) bool {
	keywords := []string{"CREATE", "DELETE", "LIST", "PUBLISH", "CONSUME", "STREAM", "HELP",
		"HEARTBEAT", "JOIN_GROUP", "LEAVE_GROUP", "COMMIT_OFFSET", "REGISTER_GROUP",
		"GROUP_STATUS", "FETCH_OFFSET", "LIST_GROUPS", "SYNC_GROUP"}
	for _, k := range keywords {
		if strings.HasPrefix(strings.ToUpper(s), k) {
			return true
		}
	}
	return false
}

func writeResponse(conn net.Conn, msg string) {
	resp := []byte(msg)
	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	if _, err := conn.Write(respLen); err != nil {
		util.Error("⚠️ Write length error: %v", err)
		return
	}
	if _, err := conn.Write(resp); err != nil {
		util.Error("⚠️ Write response error: %v", err)
		return
	}
}

// startHealthCheckServer starts a simple HTTP server for health checks
func startHealthCheckServer(port int, brokerReady *atomic.Bool) {
	mux := http.NewServeMux()

	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		if !brokerReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte("Broker not ready: Main listener not active")); err != nil {
				util.Error("⚠️ Health check response write error: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			util.Error("⚠️ Health check response write error: %v", err)
		}
	}

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", healthHandler)

	addr := fmt.Sprintf(":%d", port)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			util.Error("❌ Health check server failed: %v", err)
		}
	}()
	util.Info("🩺 Health check endpoint started on port %d", port)
}
