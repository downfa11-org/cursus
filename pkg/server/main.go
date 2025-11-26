package server

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/controller"
	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/offset"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const (
	maxWorkers             = 1000
	readDeadline           = 5 * time.Minute // Read deadline defined as a constant
	DefaultHealthCheckPort = 9080
)

var brokerReady = &atomic.Bool{}

// RunServer starts the broker with optional TLS and gzip
func RunServer(cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, om *offset.OffsetManager, cd *coordinator.Coordinator) error {
	if cfg.EnableExporter {
		metrics.StartMetricsServer(cfg.ExporterPort)
		util.Info("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		log.Println("📉 Exporter disabled")
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
				HandleConnection(conn, tm, dm, cfg, om, cd)
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
func HandleConnection(conn net.Conn, tm *topic.TopicManager, dm *disk.DiskManager, cfg *config.Config, om *offset.OffsetManager, cd *coordinator.Coordinator) {
	defer conn.Close()

	cmdHandler := controller.NewCommandHandler(tm, dm, cfg, om, cd)
	ctx := controller.NewClientContext("default-group", 0)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
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
			util.Error("⚠️ Read message error: %v", err)
			return
		}

		data, err := DecompressMessage(msgBuf, cfg.EnableGzip)
		if err != nil {
			util.Error("⚠️ Decompress error: %v", err)
			return
		}

		topicName, payload := util.DecodeMessage(data)
		clientAddr := conn.RemoteAddr().String()

		if topicName == "" || payload == "" {
			rawInput := strings.TrimSpace(string(data))
			util.Debug("[INPUT_WARN] [%s] Received unrecognized input (not cmd/msg format): %s", clientAddr, rawInput)
			return
		}

		util.Debug("[%s] Received request. Topic: '%s', Payload: '%s'", clientAddr, topicName, payload)

		var resp string
		cmdStr := payload

		if !isCommand(payload) {
			var msg types.Message
			var acks string = "0"

			util.Debug("Processing non-command message. Topic: %s, Payload prefix: %s",
				topicName, payload[:min(50, len(payload))])

			if strings.HasPrefix(payload, "PUBLISH:") {
				parts := strings.SplitN(payload[8:], ":", 3)
				if len(parts) < 3 {
					util.Error("Malformed PUBLISH message: expected 3 parts, got %d", len(parts))
					writeResponse(conn, "ERROR: malformed PUBLISH format")
					continue
				}

				acks = parts[0]
				topicName = parts[1]
				msg.Payload = parts[2]
				msg.Key = parts[2]

			} else if strings.HasPrefix(payload, "IDEMPOTENT:") {
				parts := strings.SplitN(payload[11:], ":", 5)
				if len(parts) < 5 {
					util.Error("Malformed IDEMPOTENT message: expected 5 parts, got %d", len(parts))
					writeResponse(conn, "ERROR: malformed IDEMPOTENT format")
					continue
				}
				acks = parts[0]
				msg.ProducerID = parts[1]
				seqNum, err := strconv.ParseUint(parts[2], 10, 64)
				if err != nil {
					util.Error("Invalid seqNum in IDEMPOTENT message: %v", err)
					writeResponse(conn, "ERROR: invalid seqNum format")
					continue
				}
				msg.SeqNum = seqNum
				epoch, err := strconv.ParseInt(parts[3], 10, 64)
				if err != nil {
					util.Error("Invalid epoch in IDEMPOTENT message: %v", err)
					writeResponse(conn, "ERROR: invalid epoch format")
					continue
				}
				msg.Epoch = epoch
				msg.Payload = parts[4]
				msg.Key = parts[4]

			} else {
				msg.Payload = payload
				msg.Key = payload
			}

			switch acks {
			case "0":
				//acks=0
				util.Debug("Calling tm.Publish for topic '%s' with acks=0", topicName)
				err := tm.Publish(topicName, msg)
				if err != nil {
					util.Error("tm.Publish failed: %v", err)
					writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					continue
				}
				util.Debug("tm.Publish completed successfully")
				writeResponse(conn, "OK")
			case "1":
				// acks=1
				util.Debug("Calling tm.PublishWithAck for topic '%s' with acks=1", topicName)
				err := tm.PublishWithAck(topicName, msg)
				if err != nil {
					writeResponse(conn, fmt.Sprintf("ERROR: %v", err))
					continue
				}
				util.Debug("tm.Publish completed successfully")
				writeResponse(conn, "OK")
			case "all":
				// TODO: acks=all case with MinInsyncReplicas Option
				util.Error("acks=all not yet implemented")
				writeResponse(conn, "ERROR: acks=all not yet implemented")
			default:
				util.Error("invalid acks: %s", acks)
				writeResponse(conn, fmt.Sprintf("ERROR: invalid acks: %s", acks))
			}

			resp = ""
		} else {
			resp = cmdHandler.HandleCommand(payload, ctx)
		}

		if resp == controller.STREAM_DATA_SIGNAL {
			streamed, err := cmdHandler.HandleConsumeCommand(conn, cmdStr, ctx)
			if err != nil {
				errMsg := fmt.Sprintf("ERROR: %v", err)
				util.Error("[CONSUME_ERR] Error streaming data for command [%s]: %v", cmdStr, err)
				writeResponse(conn, errMsg)
				return
			}
			util.Debug("Completed streaming %d messages for command [%s]", streamed, cmdStr)
			return
		}

		if resp != "" {
			writeResponse(conn, resp)
		}
	}
}

func isCommand(s string) bool {
	if strings.HasPrefix(s, "PUBLISH:") || strings.HasPrefix(s, "IDEMPOTENT:") {
		return false
	}

	keywords := []string{"CREATE", "DELETE", "LIST", "SUBSCRIBE", "PUBLISH", "CONSUME", "HELP",
		"SETGROUP", "HEARTBEAT", "JOIN_GROUP", "LEAVE_GROUP", "COMMIT_OFFSET", "REGISTER_GROUP",
		"GROUP_STATUS", "FETCH_OFFSET", "LIST_GROUPS"}
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
