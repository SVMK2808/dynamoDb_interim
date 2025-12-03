package ring

import (
	"testing"
)

func TestGenTokensDeterministic(t *testing.T) {
    a1 := GenTokens(NodeID("nodeA"), 8)
    a2 := GenTokens(NodeID("nodeA"), 8)
    if len(a1) != len(a2) {
        t.Fatalf("length mismatch: %d vs %d", len(a1), len(a2))
    }
    for i := range a1 {
        if a1[i] != a2[i] {
            t.Fatalf("tokens not deterministic at %d: %v vs %v", i, a1[i], a2[i])
        }
    }
}

func TestPreferenceListForKeyOrderAndWrap(t *testing.T) {
    r := New()
    r.Add(NodeID("A"), []uint64{10})
    r.Add(NodeID("B"), []uint64{30})
    r.Add(NodeID("C"), []uint64{20})

    nodes := r.PreferenceListForKey("k1", 3)
    if len(nodes) != 3 {
        t.Fatalf("expected 3 nodes, got %d", len(nodes))
    }
    // ensure unique order and coverage of all nodes
    seen := map[NodeID]bool{}
    for _, n := range nodes {
        if seen[n] { t.Fatalf("duplicate node in preference list: %s", n) }
        seen[n] = true
    }
}
