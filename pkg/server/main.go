package server

import (
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
func RunServer(cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator, sm *stream.StreamManager) error {
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

	util.Info("🧩 Broker listening on %s (TLS=%v, Gzip=%v)", addr, cfg.UseTLS, cfg.EnableGzip)
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

	workerCh := make(chan net.Conn, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for conn := range workerCh {
				HandleConnection(conn, tm, dm, cfg, cd, sm)
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
func HandleConnection(conn net.Conn, tm *topic.TopicManager, dm *disk.DiskManager, cfg *config.Config, cd *coordinator.Coordinator, sm *stream.StreamManager) {
	defer conn.Close()

	cmdHandler := controller.NewCommandHandler(tm, dm, cfg, cd, sm)
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

		data, err := DecompressMessage(msgBuf, cfg.EnableGzip)
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
			if err := tm.PublishWithAck(topicName, msg); err != nil {
				writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
				continue
			}
			writeResponse(conn, "OK")
		case "-1":
			writeResponse(conn, "ERROR: acks=-1(all) not implemented")
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
