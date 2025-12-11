package transport

import (
	"net"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

type Transport struct {
	timeout time.Duration
}

func NewTransport(timeout time.Duration) *Transport {
	return &Transport{timeout: timeout}
}

func (t *Transport) SendRequest(addr, command string) (string, error) {
	util.Debug("Sending request to %s (timeout: %v)", addr, t.timeout)

	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		util.Error("Failed to connect to %s: %v", addr, err)
		return "", err
	}
	defer conn.Close()

	if err := t.sendCommand(conn, command); err != nil {
		util.Error("Failed to send command to %s: %v", addr, err)
		return "", err
	}

	resp, err := t.receiveResponse(conn)
	if err != nil {
		util.Error("Failed to receive response from %s: %v", addr, err)
		return "", err
	}

	util.Debug("Successfully received response from %s", addr)
	return resp, nil
}

func (t *Transport) sendCommand(conn net.Conn, command string) error {
	data := []byte(command)
	lenBuf := make([]byte, 4)

	lenBuf[0] = byte(len(data) >> 24)
	lenBuf[1] = byte(len(data) >> 16)
	lenBuf[2] = byte(len(data) >> 8)
	lenBuf[3] = byte(len(data))

	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	_, err := conn.Write(data)
	util.Debug("Sent %d bytes to %s", len(data), conn.RemoteAddr())
	return err
}

func (t *Transport) receiveResponse(conn net.Conn) (string, error) {
	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return "", err
	}

	msgLen := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	msgBuf := make([]byte, msgLen)

	if _, err := conn.Read(msgBuf); err != nil {
		return "", err
	}

	util.Debug("Received %d bytes from %s", msgLen, conn.RemoteAddr())
	return string(msgBuf), nil
}
