package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func MockServer(t *testing.T, addr string, response string, closeAfter bool) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			t.Errorf("Mock server: Failed to read command length: %v", err)
			return
		}

		msgLen := binary.BigEndian.Uint32(lenBuf)
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			t.Errorf("Mock server: Failed to read command body: %v", err)
			return
		}

		receivedCmd := string(msgBuf)
		t.Logf("Mock server received: %s", receivedCmd)

		respLen := uint32(len(response))
		respLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(respLenBuf, respLen)

		if _, err := conn.Write(respLenBuf); err != nil {
			t.Errorf("Mock server: Failed to send response length: %v", err)
			return
		}
		if _, err := conn.Write([]byte(response)); err != nil {
			t.Errorf("Mock server: Failed to send response body: %v", err)
			return
		}

		if closeAfter {
			conn.Close()
		}
	}()
}

func TestNewTransport(t *testing.T) {
	timeout := 10 * time.Second
	tr := NewTransport(timeout)

	if tr == nil {
		t.Fatal("NewTransport should not return nil")
	}
	if tr.timeout != timeout {
		t.Errorf("Expected timeout %v, got %v", timeout, tr.timeout)
	}
}

func TestSendRequest_DialTimeout(t *testing.T) {
	testAddr := "127.0.0.1:65535"

	tr := NewTransport(1 * time.Millisecond)

	_, err := tr.SendRequest(testAddr, "PING")

	if err == nil {
		t.Fatalf("SendRequest expected to fail due to timeout, but succeeded")
	}
	if !errors.Is(err, io.EOF) &&
		!bytes.Contains([]byte(err.Error()), []byte("refused")) &&
		!bytes.Contains([]byte(err.Error()), []byte("timeout")) {
		t.Errorf("Unexpected error type: %v", err)
	}
}
