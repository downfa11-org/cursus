package server

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
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
		log.Printf("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		log.Println("📉 Exporter disabled")
	}

	addr := fmt.Sprintf(":%d", cfg.BrokerPort)
	var ln net.Listener
	var err error
	if cfg.UseTLS {
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cfg.TLSCert}}
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}

	log.Printf("🧩 Broker listening on %s (TLS=%v, Gzip=%v)", addr, cfg.UseTLS, cfg.EnableGzip)
	brokerReady.Store(true)

	healthPort := cfg.HealthCheckPort
	if healthPort == 0 {
		healthPort = DefaultHealthCheckPort
	}
	startHealthCheckServer(healthPort, brokerReady)

	if cd != nil {
		cd.Start()
		log.Printf("🔄 Coordinator started with heartbeat monitoring")
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
			log.Printf("⚠️ Accept error: %v", err)
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
			log.Printf("⚠️ SetReadDeadline error: %v", err)
			return
		}

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("⚠️ Read length error: %v", err)
			}
			return
		}

		msgLen := binary.BigEndian.Uint32(lenBuf)
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			log.Printf("⚠️ Read message error: %v", err)
			return
		}

		data, err := DecompressMessage(msgBuf, cfg.EnableGzip)
		if err != nil {
			log.Printf("⚠️ Decompress error: %v", err)
			return
		}

		consumerGroupOrTopic, payload := util.DecodeMessage(data)
		clientAddr := conn.RemoteAddr().String()

		if consumerGroupOrTopic == "" || payload == "" {
			rawInput := strings.TrimSpace(string(data))
			log.Printf("[INPUT_WARN] [%s] Received unrecognized input (not cmd/msg format): %s", clientAddr, rawInput)
			return
		}

		if isCommand(payload) {
			ctx.SetConsumerGroup(consumerGroupOrTopic)
			log.Printf("[%s] Received command. Group: '%s', Payload: '%s'", clientAddr, consumerGroupOrTopic, payload)
		} else {
			log.Printf("[%s] Received request. Topic: '%s', Payload: '%s'", clientAddr, consumerGroupOrTopic, payload)
		}

		var resp string
		cmdStr := payload

		if isCommand(payload) {
			resp = cmdHandler.HandleCommand(payload, ctx)
		} else {
			topicName := consumerGroupOrTopic

			switch cfg.Acks {
			case "0":
				err := tm.Publish(topicName, types.Message{
					Payload: payload,
					Key:     payload,
				})

				if err != nil {
					resp = fmt.Sprintf("ERROR: %v", err)
					writeResponse(conn, resp)
					continue
				}

				writeResponse(conn, "OK")

			case "1":
				// acks=1
				err := tm.PublishWithAck(topicName, types.Message{
					Payload: payload,
					Key:     payload,
				})

				if err != nil {
					errMsg := fmt.Sprintf("ERROR: %v", err)
					log.Printf("[PUBLISH_ERR] [%s] %s", clientAddr, errMsg)
					writeResponse(conn, errMsg)
					continue
				}
				writeResponse(conn, "OK")

			case "all":
				// TODO: acks=all case with MinInsyncReplicas Option
				writeResponse(conn, "ERROR: acks=all not yet implemented")
			default:
				errMsg := fmt.Sprintf("ERROR: invalid acks configuration: %s", cfg.Acks)
				log.Printf("[CONFIG_ERR] %s", errMsg)
				writeResponse(conn, errMsg)
				return
			}
			resp = ""
		}

		if resp == controller.STREAM_DATA_SIGNAL {
			streamed, err := cmdHandler.HandleConsumeCommand(conn, cmdStr, ctx)
			if err != nil {
				errMsg := fmt.Sprintf("ERROR: %v", err)
				log.Printf("[CONSUME_ERR] Error streaming data for command [%s]: %v", cmdStr, err)
				writeResponse(conn, errMsg)
				return
			}
			log.Printf("[STREAM] Completed streaming %d messages for command [%s]", streamed, cmdStr)
			return
		}

		if resp != "" {
			writeResponse(conn, resp)
		}
	}
}

func isCommand(s string) bool {
	keywords := []string{"CREATE", "DELETE", "LIST", "SUBSCRIBE", "PUBLISH", "CONSUME", "HELP",
		"SETGROUP", "HEARTBEAT", "JOIN_GROUP", "LEAVE_GROUP", "COMMIT_OFFSET", "REGISTER_GROUP"}
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
		log.Printf("⚠️ Write length error: %v", err)
		return
	}
	if _, err := conn.Write(resp); err != nil {
		log.Printf("⚠️ Write response error: %v", err)
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
				log.Printf("⚠️ Health check response write error: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			log.Printf("⚠️ Health check response write error: %v", err)
		}
	}

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", healthHandler)

	addr := fmt.Sprintf(":%d", port)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("❌ Health check server failed: %v", err)
		}
	}()
	log.Printf("🩺 Health check endpoint started on port %d", port)
}
