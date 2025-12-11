package replication

import (
	"fmt"
	"testing"
)

func TestISRManager_UpdateISR_AddAndRemove(t *testing.T) {
	im := NewISRManager()
	topic := "t1"
	partition := 0
	leader := "l1"
	replica2 := "r2"
	replica3 := "r3"

	replicas1 := []string{leader, replica2, replica3}
	im.UpdateISR(topic, partition, leader, replicas1)

	isr := im.GetISR(topic, partition)
	if len(isr) != 3 {
		t.Fatalf("Expected 3 in ISR, got %d. ISR: %v", len(isr), isr)
	}

	im.mu.Lock()
	im.replicaLag[fmt.Sprintf("%s-%d-%s", topic, partition, replica3)] = 2 * 1024 * 1024 // 2MB lag
	im.mu.Unlock()

	im.UpdateISR(topic, partition, leader, replicas1)

	isr = im.GetISR(topic, partition)
	if len(isr) != 2 || !contains(isr, leader) || !contains(isr, replica2) || contains(isr, replica3) {
		t.Errorf("ISR update failed. Expected [l1, r2], got %v", isr)
	}

	im.mu.Lock()
	im.replicaLag[fmt.Sprintf("%s-%d-%s", topic, partition, replica3)] = 512 * 1024 // 512KB lag
	im.mu.Unlock()

	im.UpdateISR(topic, partition, leader, replicas1)

	isr = im.GetISR(topic, partition)
	if len(isr) != 3 {
		t.Errorf("ISR restore failed. Expected 3, got %d. ISR: %v", len(isr), isr)
	}
}

func TestISRManager_HasQuorum(t *testing.T) {
	im := NewISRManager()
	topic := "t1"
	partition := 0
	key := fmt.Sprintf("%s-%d", topic, partition)

	im.isrMap[key] = []string{"l1", "r2", "r3"}

	if !im.HasQuorum(topic, partition, 2) {
		t.Error("HasQuorum failed for required 2 (Expected true)")
	}

	if !im.HasQuorum(topic, partition, 3) {
		t.Error("HasQuorum failed for required 3 (Expected true)")
	}

	// quorom fail
	if im.HasQuorum(topic, partition, 4) {
		t.Error("HasQuorum succeeded for required 4 (Expected false)")
	}
}

func TestISRManager_GetISR(t *testing.T) {
	im := NewISRManager()
	topic := "t1"
	partition := 0
	key := fmt.Sprintf("%s-%d", topic, partition)
	expectedISR := []string{"l1", "r2"}

	im.isrMap[key] = expectedISR

	isr := im.GetISR(topic, partition)
	if len(isr) != 2 || isr[0] != "l1" || isr[1] != "r2" {
		t.Errorf("GetISR failed. Expected %v, got %v", expectedISR, isr)
	}
}
