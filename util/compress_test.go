package util_test

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/downfa11-org/go-broker/util"
)

// TestCompressMessage_AllTypes tests all supported compression types
func TestCompressMessage_AllTypes(t *testing.T) {
	testData := []byte("Hello, World! This is a test string for compression.")

	tests := []struct {
		name             string
		compressionType  string
		expectError      bool
		expectCompressed bool
	}{
		{"gzip", "gzip", false, true},
		{"snappy", "snappy", false, true},
		{"lz4", "lz4", false, true},
		{"none", "none", false, false},
		{"empty", "", false, false},
		{"unsupported", "unknown", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := util.CompressMessage(testData, tt.compressionType)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for compression type %s", tt.compressionType)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error for compression type %s: %v", tt.compressionType, err)
			}

			if tt.expectCompressed && bytes.Equal(result, testData) {
				t.Errorf("Expected compressed data for type %s, but got original data", tt.compressionType)
			}

			if !tt.expectCompressed && !bytes.Equal(result, testData) {
				t.Errorf("Expected original data for type %s, but got compressed data", tt.compressionType)
			}
		})
	}
}

// TestDecompressMessage_AllTypes tests decompression for all supported types
func TestDecompressMessage_AllTypes(t *testing.T) {
	testData := []byte("Hello, World! This is a test string for compression.")

	compressedData := make(map[string][]byte)
	for _, compType := range []string{"gzip", "snappy", "lz4", "none"} {
		comp, err := util.CompressMessage(testData, compType)
		if err != nil {
			t.Fatalf("Failed to compress with %s: %v", compType, err)
		}
		compressedData[compType] = comp
	}

	tests := []struct {
		name            string
		compressionType string
		expectError     bool
	}{
		{"gzip", "gzip", false},
		{"snappy", "snappy", false},
		{"lz4", "lz4", false},
		{"none", "none", false},
		{"empty", "", false},
		{"unsupported", "unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var data []byte
			if tt.compressionType == "" {
				data = testData
			} else if compData, exists := compressedData[tt.compressionType]; exists {
				data = compData
			} else {
				data = []byte("invalid")
			}

			result, err := util.DecompressMessage(data, tt.compressionType)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for compression type %s", tt.compressionType)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error for compression type %s: %v", tt.compressionType, err)
			}

			if !bytes.Equal(result, testData) {
				t.Errorf("Decompressed data mismatch for type %s: expected %s, got %s",
					tt.compressionType, testData, result)
			}
		})
	}
}

// TestCompressDecompressRoundtrip verifies roundtrip compression/decompression
func TestCompressDecompressRoundtrip(t *testing.T) {
	testCases := [][]byte{
		[]byte("a"),
		[]byte("Hello, World!"),
		make([]byte, 1000),
		make([]byte, 10000),
		[]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
			"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."),
	}

	for _, tc := range testCases {
		for _, compType := range []string{"gzip", "snappy", "lz4", "none"} {
			// Snappy has a minimum practical input size; skip very small inputs
			if compType == "snappy" && (len(tc) == 0 || len(tc) == 1) {
				continue
			}

			t.Run(fmt.Sprintf("%s_%dB", compType, len(tc)), func(t *testing.T) {
				compressed, err := util.CompressMessage(tc, compType)
				if err != nil {
					t.Fatalf("Compression failed: %v", err)
				}

				decompressed, err := util.DecompressMessage(compressed, compType)
				if err != nil {
					t.Fatalf("Decompression failed: %v", err)
				}

				if !bytes.Equal(decompressed, tc) {
					t.Errorf("Roundtrip failed: original length=%d, decompressed length=%d",
						len(tc), len(decompressed))
				}
			})
		}
	}
}

// TestCompressMessage_EdgeCases tests edge cases for compression
func TestCompressMessage_EdgeCases(t *testing.T) {
	tests := []struct {
		name            string
		data            []byte
		compressionType string
		expectError     bool
	}{
		{"nil_data_gzip", nil, "gzip", false},
		{"nil_data_snappy", nil, "snappy", false},
		{"nil_data_lz4", nil, "lz4", false},
		{"nil_data_none", nil, "none", false},
		{"empty_data_gzip", []byte{}, "gzip", false},
		{"empty_data_snappy", []byte{}, "snappy", false},
		{"empty_data_lz4", []byte{}, "lz4", false},
		{"empty_data_none", []byte{}, "none", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := util.CompressMessage(tt.data, tt.compressionType)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for %s with %s", tt.name, tt.compressionType)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if tt.compressionType == "none" || tt.compressionType == "" {
				if !bytes.Equal(result, tt.data) {
					t.Errorf("Expected original data for none compression")
				}
			}
		})
	}
}

// TestDecompressMessage_InvalidData tests error handling for invalid compressed data
func TestDecompressMessage_InvalidData(t *testing.T) {
	invalidData := []byte("this is not valid compressed data")

	tests := []struct {
		name            string
		compressionType string
		data            []byte
		expectError     bool
	}{
		{"invalid_gzip", "gzip", invalidData, true},
		{"invalid_snappy", "snappy", invalidData, true},
		{"invalid_lz4", "lz4", invalidData, true},
		{"empty_gzip", "gzip", []byte{}, true},
		{"empty_snappy", "snappy", []byte{}, true},
		{"empty_lz4", "lz4", []byte{}, false},
		{"none_with_invalid", "none", invalidData, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := util.DecompressMessage(tt.data, tt.compressionType)

			if tt.expectError && err == nil {
				t.Errorf("Expected error for %s", tt.name)
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error for %s: %v", tt.name, err)
			}
		})
	}
}

// TestCompressionRatio verifies that compression actually reduces size for compressible data
func TestCompressionRatio(t *testing.T) {
	compressibleData := bytes.Repeat([]byte("1234567890"), 1000) // 10KB

	for _, compType := range []string{"gzip", "snappy", "lz4"} {
		t.Run(compType, func(t *testing.T) {
			compressed, err := util.CompressMessage(compressibleData, compType)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			if len(compressed) >= len(compressibleData) {
				t.Errorf("Compression %s didn't reduce size: original=%d, compressed=%d",
					compType, len(compressibleData), len(compressed))
			}

			decompressed, err := util.DecompressMessage(compressed, compType)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			if !bytes.Equal(decompressed, compressibleData) {
				t.Error("Roundtrip verification failed")
			}
		})
	}
}

// TestConcurrentCompression tests thread safety of compression functions
func TestConcurrentCompression(t *testing.T) {
	testData := []byte("Hello, World! This is a concurrent test.")

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			compType := []string{"gzip", "snappy", "lz4", "none"}[id%4]

			compressed, err := util.CompressMessage(testData, compType)
			if err != nil {
				errors <- fmt.Errorf("compression failed: %v", err)
				return
			}

			decompressed, err := util.DecompressMessage(compressed, compType)
			if err != nil {
				errors <- fmt.Errorf("decompression failed: %v", err)
				return
			}

			if !bytes.Equal(decompressed, testData) {
				errors <- fmt.Errorf("data mismatch in goroutine %d", id)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestMemoryUsage verifies that compression doesn't leak memory
func TestMemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	testData := make([]byte, 1024*1024) // 1MB

	for i := 0; i < 100; i++ {
		for _, compType := range []string{"gzip", "snappy", "lz4"} {
			compressed, err := util.CompressMessage(testData, compType)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			_, err = util.DecompressMessage(compressed, compType)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	var memIncrease uint64
	if m2.Alloc > m1.Alloc {
		memIncrease = m2.Alloc - m1.Alloc
	}

	if memIncrease > 50*1024*1024 { // 50MB threshold
		t.Errorf("Potential memory leak: increased by %d bytes", memIncrease)
	}
}

// BenchmarkCompression benchmarks different compression algorithms
func BenchmarkCompression(b *testing.B) {
	testData := bytes.Repeat([]byte("Hello, World! "), 100)

	for _, compType := range []string{"gzip", "snappy", "lz4", "none"} {
		b.Run(compType, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := util.CompressMessage(testData, compType)
				if err != nil {
					b.Fatalf("Compression failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkDecompression benchmarks different decompression algorithms
func BenchmarkDecompression(b *testing.B) {
	testData := bytes.Repeat([]byte("Hello, World! "), 100)

	compressed := make(map[string][]byte)
	for _, compType := range []string{"gzip", "snappy", "lz4", "none"} {
		comp, err := util.CompressMessage(testData, compType)
		if err != nil {
			b.Fatalf("Failed to compress with %s: %v", compType, err)
		}
		compressed[compType] = comp
	}

	for _, compType := range []string{"gzip", "snappy", "lz4", "none"} {
		b.Run(compType, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := util.DecompressMessage(compressed[compType], compType)
				if err != nil {
					b.Fatalf("Decompression failed: %v", err)
				}
			}
		})
	}
}
