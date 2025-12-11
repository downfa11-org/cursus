package replication

import (
	"testing"
)

func TestPreferredLeaderManager_SetAndGet(t *testing.T) {
	plm := NewPreferredLeaderManager()
	topic := "t1"
	partition := 0
	broker := "b1"

	plm.SetPreferredLeader(topic, partition, broker)

	leader, exists := plm.GetPreferredLeader(topic, partition)

	if !exists {
		t.Fatal("Preferred leader should exist")
	}
	if leader != broker {
		t.Errorf("Expected preferred leader %s, got %s", broker, leader)
	}

	_, exists = plm.GetPreferredLeader("t2", 0)
	if exists {
		t.Error("Preferred leader should not exist for t2-0")
	}
}

func TestPreferredLeaderManager_UpdateAndGetLoad(t *testing.T) {
	plm := NewPreferredLeaderManager()
	broker := "b1"

	load := plm.GetReplicaLoad(broker)
	if load != 0 {
		t.Errorf("Initial load expected 0, got %d", load)
	}

	plm.UpdateReplicaLoad(broker, 3)
	load = plm.GetReplicaLoad(broker)
	if load != 3 {
		t.Errorf("Load after +3 expected 3, got %d", load)
	}

	plm.UpdateReplicaLoad(broker, -1)
	load = plm.GetReplicaLoad(broker)
	if load != 2 {
		t.Errorf("Load after -1 expected 2, got %d", load)
	}
}

func TestPreferredLeaderManager_ShouldRebalance(t *testing.T) {
	plm := NewPreferredLeaderManager()
	current := "c1"
	preferred := "p1"

	if plm.ShouldRebalance(current, preferred) != true {
		t.Error("ShouldRebalance failed when load is balanced (0 vs 0)")
	}

	plm.UpdateReplicaLoad(current, 5)
	if plm.ShouldRebalance(current, preferred) != true {
		t.Error("ShouldRebalance failed when preferred is much lighter (5 vs 0)")
	}

	plm.UpdateReplicaLoad(preferred, 8)
	if plm.ShouldRebalance(current, preferred) != false {
		t.Error("ShouldRebalance succeeded when preferred is too heavy (5 vs 8)")
	}

	if plm.ShouldRebalance(current, current) != false {
		t.Error("ShouldRebalance failed when current == preferred (Expected false)")
	}
}
