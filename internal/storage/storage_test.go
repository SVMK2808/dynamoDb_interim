package storage

import (
	"path/filepath"
	"testing"
)

func TestPutGetIterateScan(t *testing.T) {
    dir := t.TempDir()
    st, err := Open(filepath.Join(dir, "db.bolt"))
    if err != nil { t.Fatalf("open: %v", err) }
    defer st.Close()

    items := []Item{{Key: "user:1", Value: []byte("v1"), Ctx: Context{Vc: map[string]uint64{"A":1}, Origin: "A", Ts: 1}}}
    if err := st.Put("user:1", items); err != nil { t.Fatalf("put: %v", err) }

    got, err := st.Get("user:1")
    if err != nil { t.Fatalf("get: %v", err) }
    if len(got) != 1 || string(got[0].Value) != "v1" { t.Fatalf("unexpected get: %+v", got) }

    // ScanPrefix should see the item
    seen := 0
    if err := st.ScanPrefix("user:", func(it Item) error { seen++; return nil }); err != nil { t.Fatalf("scan: %v", err) }
    if seen != 1 { t.Fatalf("expected 1 seen, got %d", seen) }

    // IterateAll should iterate once with the key and items
    count := 0
    if err := st.IterateAll(func(k string, arr []Item) error { if k=="user:1" { count += len(arr) }; return nil }); err != nil { t.Fatalf("iterate: %v", err) }
    if count != 1 { t.Fatalf("expected iterate count 1, got %d", count) }

    // ListKeysForVnodeWithPrefix ignores vnode and returns the key
    m := st.ListKeysForVnodeWithPrefix(0, []byte("user:"))
    if len(m) != 1 || string(m["user:1"]) != "v1" { t.Fatalf("unexpected list keys: %+v", m) }
}
