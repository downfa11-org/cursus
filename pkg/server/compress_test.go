package server_test

import (
	"bytes"
	"testing"

	"github.com/downfa11-org/go-broker/pkg/server"
)

func TestCompressDecompress(t *testing.T) {
	input := []byte("hello world")
	cmp, err := server.CompressMessage(input, true)
	if err != nil {
		t.Fatal(err)
	}

	out, err := server.DecompressMessage(cmp, true)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(input, out) {
		t.Errorf("Expected %s, got %s", input, out)
	}
}
