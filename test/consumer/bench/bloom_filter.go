package bench

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync/atomic"
)

func encodeOffset(partition int, offset int64) []byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(partition))
	binary.BigEndian.PutUint64(buf[8:16], uint64(offset))
	return buf[:]
}

func encodeMessageID(producerID string, seqNum uint64) []byte {
	return []byte(fmt.Sprintf("%s-%d", producerID, seqNum))
}

type BloomFilter struct {
	bits []uint64
	m    uint64
	k    uint64
}

func NewBloomFilter(expected uint64, fpRate float64) *BloomFilter {
	m := uint64(-1 * float64(expected) * math.Log(fpRate) / (math.Ln2 * math.Ln2))
	k := uint64(float64(m) / float64(expected) * math.Ln2)
	size := (m + 63) / 64

	if k < 1 {
		k = 1
	}
	if k > 5 {
		k = 5
	}

	return &BloomFilter{
		bits: make([]uint64, size),
		m:    m,
		k:    k,
	}
}

func hashf(data []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(data)
	sum1 := h1.Sum64()

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sum1)
	h2 := fnv.New64()
	h2.Write(buf[:])
	sum2 := h2.Sum64()

	return sum1, sum2
}

func (bf *BloomFilter) Add(data []byte) bool {
	h1, h2 := hashf(data)

	var seen = true
	for i := uint64(0); i < bf.k; i++ {
		idx := (h1 + i*h2) % bf.m
		word := idx / 64
		bit := uint64(1) << (idx % 64)

		old := atomic.LoadUint64(&bf.bits[word])
		if old&bit == 0 {
			seen = false
			atomic.OrUint64(&bf.bits[word], bit)
		}
	}

	return seen
}
