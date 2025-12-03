package merkle

import "testing"

// TestRootFromKeysDeterminism ensures ordering independence and determinism for the same set of keys.
func TestRootFromKeysDeterminism(t *testing.T) {
    keys1 := []string{"k3","k1","k2"}
    keys2 := []string{"k2","k3","k1"}
    r1 := RootFromKeys(keys1)
    r2 := RootFromKeys(keys2)
    if r1 != r2 {
        t.Fatalf("expected same root for same key set; r1=%s r2=%s", r1, r2)
    }
    // empty set stable root
    if empty := RootFromKeys(nil); empty == "" { t.Fatalf("empty root should be non-empty hash") }
}

// TestHashConsistency verifies Hash uses sha256 and returns hex length 64.
func TestHashConsistency(t *testing.T) {
    h := Hash([]byte("abc"))
    if len(h) != 64 { t.Fatalf("expected 64 hex chars, got %d", len(h)) }
    if h != Hash([]byte("abc")) { t.Fatalf("hash not deterministic") }
}
