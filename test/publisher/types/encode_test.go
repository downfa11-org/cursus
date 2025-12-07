package types_test

import (
	"bytes"
	"testing"

	"github.com/downfa11-org/go-broker/publisher/types"
)

func TestEncodeBatchMessages(t *testing.T) {
	msgs := []types.Message{
		{SeqNum: 1, Payload: "msg1"},
		{SeqNum: 2, Payload: "msg2"},
	}

	data, err := types.EncodeBatchMessages("test-topic", 1, msgs)
	if err != nil {
		t.Fatalf("EncodeBatchMessages failed: %v", err)
	}

	if len(data) < 2 || data[0] != 0xBA || data[1] != 0x7C {
		t.Errorf("Invalid batch header, got %x", data[:2])
	}
	if !bytes.Contains(data, []byte("msg2")) {
		t.Errorf("Encoded batch missing last message payload")
	}
}

func TestCompressDecompress(t *testing.T) {
	raw := []byte("hello world")

	// gzip
	cmp, err := types.CompressMessage(raw, true)
	if err != nil {
		t.Fatalf("CompressMessage failed: %v", err)
	}

	dec, err := types.DecompressMessage(cmp)
	if err != nil {
		t.Fatalf("DecompressMessage failed: %v", err)
	}

	if string(dec) != string(raw) {
		t.Errorf("Decompressed data mismatch, expected %s, got %s", raw, dec)
	}

	// not gzip
	cmp2, err := types.CompressMessage(raw, false)
	if err != nil {
		t.Fatalf("CompressMessage failed: %v", err)
	}

	if !bytes.Equal(cmp2, raw) {
		t.Errorf("Expected uncompressed data to match original")
	}
}

func TestDecompressInvalidData(t *testing.T) {
	_, err := types.DecompressMessage([]byte("notgzip"))
	if err == nil {
		t.Errorf("Expected error when decompressing invalid gzip data")
	}
}

func TestEncodeEmptyBatch(t *testing.T) {
	data, err := types.EncodeBatchMessages("topic", 0, nil)
	if err != nil {
		t.Fatalf("EncodeBatchMessages failed for empty batch: %v", err)
	}

	if len(data) < 2 || data[0] != 0xBA || data[1] != 0x7C {
		t.Errorf("Invalid batch header for empty batch")
	}
}
