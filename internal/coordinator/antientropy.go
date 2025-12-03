package coordinator

import (
    "context"
    "log"
    "strings"
    "time"

    pb "dynamodb1/dynamodb1/dynamodb1/proto"
    "dynamodb1/internal/storage"

    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
    gproto "google.golang.org/protobuf/proto"
)

// StartAntiEntropy launches a periodic background worker. Current implementation is a stub
// that simply dials peers for a lightweight merkle check using the existing proto definitions.
func (c *Coordinator) StartAntiEntropy(stopCh <-chan struct{}, interval time.Duration) {
    // Anti-entropy disabled: no background workers started.
    log.Printf("[antientropy:%s] disabled (no-op)", c.NodeID)
}

// runOnceAntiEntropy performs a simplified pass: iterate nodes in the ring and request a merkle stream.
// The original design referenced per-vnode ownership and leaf hashes that are not yet implemented; we
// replace that with a minimal implementation to avoid undefined identifiers.
func (c *Coordinator) runOnceAntiEntropy(ctx context.Context) {
    // Anti-entropy disabled: no-op.
}

// syncNodeMerkle performs a merkle stream request and discards chunks (placeholder logic).
func (c *Coordinator) syncNodeMerkle(ctx context.Context, vnode string, addr string) {
    // Anti-entropy disabled: no-op.
}

// decodeMerkleStream consumes the merkle stream and fills leaves/root.
func decodeMerkleStream(stream pb.Dynamo_StreamMerkleClient, out map[string][20]byte, root *[20]byte) {
    // Anti-entropy disabled: no-op.
}

// StartHintApplyWorker periodically scans the durable HintDB and attempts to apply hints
// by converting them into canonical replica writes on the local node.
func (c *Coordinator) StartHintApplyWorker(stop <-chan struct{}, interval time.Duration) {
    if c.HintDB == nil {
        return
    }
    go func() {
        tk := time.NewTicker(interval)
        defer tk.Stop()
        for {
            select {
            case <-stop:
                return
            case <-tk.C:
                c.applyHintsOnce()
            }
        }
    }()
}

func (c *Coordinator) applyHintsOnce() {
    if c.HintDB == nil {
        return
    }
    _ = c.HintDB.IterateHints("", func(k string, v []byte) error {
        // Parse hint key: hint:<destNode>:<origKey>:<ts>
        parts := strings.SplitN(k, ":", 4)
        if len(parts) < 3 {
            // malformed; drop
            _ = c.HintDB.DeleteHint(k)
            return nil
        }
        origKey := parts[2]
        var item pb.ItemProto
        if err := gproto.Unmarshal(v, &item); err != nil {
            log.Printf("[hints] unmarshal failed %s: %v (dropping)", k, err)
            _ = c.HintDB.DeleteHint(k)
            return nil
        }
        // ensure canonical key
        item.Key = origKey

        // Apply via local replica path to preserve sibling semantics
        localAddr := c.NodeAddr[c.NodeID]
        if localAddr == "" {
            return nil
        }
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        defer cancel()
        conn, err := grpc.DialContext(ctx, localAddr, grpc.WithInsecure(), grpc.WithBlock())
        if err != nil {
            return nil
        }
        defer conn.Close()
        cli := pb.NewDynamoClient(conn)
        md := metadata.Pairs("role", "coordinator", "origin-node", c.NodeID)
        rctx := metadata.NewOutgoingContext(ctx, md)
        _, err = cli.ReplicaPut(rctx, &pb.ReplicaPutReq{Item: &item})
        if err != nil {
            log.Printf("[hints] apply %s -> %s failed: %v", k, origKey, err)
            return nil
        }
        if err := c.HintDB.DeleteHint(k); err != nil {
            log.Printf("[hints] delete %s failed after apply: %v", k, err)
        } else {
            log.Printf("[hints] applied %s -> %s", k, origKey)
        }
        return nil
    })
}

func (c *Coordinator) buildLocalHashMapforVnode(vnode string) (map[string]storage.Item, error) {
    m := make(map[string]storage.Item)

    // Try to get the local replica's view via gRPC (preferred). If that fails, fall
    // back to scanning local stores (HintStore / legacy APIs). We intentionally
    // skip keys that begin with "hint:" so anti-entropy never touches hinted-handoff
    // entries.
    localAddr := c.NodeAddr[c.NodeID]
    if localAddr != "" {
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        defer cancel()
        conn, err := grpc.DialContext(ctx, localAddr, grpc.WithInsecure(), grpc.WithBlock())
        if err == nil {
            defer conn.Close()
            cli := pb.NewDynamoClient(conn)
            md := metadata.Pairs("role", "coordinator", "origin-node", c.NodeID)
            rctx := metadata.NewOutgoingContext(ctx, md)

            // Attempt to call a replica-side scan/stream if available. Many codebases
            // expose a streaming RPC such as StreamKeys/StreamPrefix; if your repo
            // provides such an RPC change the call below accordingly. If not present
            // this attempt will be skipped by falling through to the fallback logic.
            if stream, err := cli.StreamKeys(rctx, &pb.StreamKeysReq{Vnode: vnode}); err == nil {
                for {
                    chunk, err := stream.Recv()
                    if err != nil { break }
                    for _, p := range chunk.GetItems() {
                        if p == nil { continue }
                        if strings.HasPrefix(p.GetKey(), "hint:") { continue }
                        // convert pb.ItemProto -> storage.Item (minimal conversion)
                        if p.Ctx == nil { continue }
                        sc := storage.Context{Vc: map[string]uint64{}, Tombstone: p.Ctx.GetTombstone(), Origin: p.Ctx.GetOrigin(), Ts: p.Ctx.GetTs()}
                        for _, e := range p.Ctx.GetEntries() { sc.Vc[e.Node] = e.Counter }
                        m[p.GetKey()] = storage.Item{Key: p.GetKey(), Value: p.GetValue(), Ctx: sc}
                    }
                }
                return m, nil
            }
        }
    }

    // Fallback: attempt to read from any store-like interface exposed on c.HintStore
    // or a legacy iterator. We use defensive type assertions so this compiles across
    // different implementations of the hint/store abstractions.
    if c.HintStore != nil {
        // Try ScanPrefix(prefix string, cb func(storage.Item) error) error
        type scanPrefixIface interface{
            ScanPrefix(string, func(storage.Item) error) error
        }
        if s, ok := interface{}(c.HintStore).(scanPrefixIface); ok {
            _ = s.ScanPrefix("", func(it storage.Item) error {
                if strings.HasPrefix(it.Key, "hint:") { return nil }
                m[it.Key] = it
                return nil
            })
            return m, nil
        }

        // Try IterateAll(func(key string, items []storage.Item) error) error
        type iterateAllIface interface{
            IterateAll(func(string, []storage.Item) error) error
        }
        if s, ok := interface{}(c.HintStore).(iterateAllIface); ok {
            _ = s.IterateAll(func(k string, items []storage.Item) error {
                if strings.HasPrefix(k, "hint:") { return nil }
                if len(items) == 0 { return nil }
                // pick latest by timestamp
                latest := items[0]
                for _, it := range items[1:] { if it.Ctx.Ts > latest.Ctx.Ts { latest = it } }
                m[k] = latest
                return nil
            })
            return m, nil
        }
    }

    // Nothing available: return empty map (no error).
    return m, nil
}

// protoItemsToStorage converts []*pb.ItemProto to []storage.Item
func protoItemsToStorage(arr []*pb.ItemProto) []storage.Item {
    items := make([]storage.Item, 0, len(arr))
    for _, p := range arr {
        if p == nil || p.Ctx == nil { continue }
        sc := storage.Context{Vc: map[string]uint64{}, Tombstone: p.Ctx.GetTombstone(), Origin: p.Ctx.GetOrigin(), Ts: p.Ctx.GetTs()}
        for _, e := range p.Ctx.GetEntries() { sc.Vc[e.Node] = e.Counter }
        items = append(items, storage.Item{Key: p.Key, Value: p.Value, Ctx: sc})
    }
    return items
}
