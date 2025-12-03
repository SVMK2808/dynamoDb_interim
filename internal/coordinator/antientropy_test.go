package coordinator

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "dynamodb1/dynamodb1/dynamodb1/proto"
	"dynamodb1/internal/ring"
	"dynamodb1/internal/storage"

	"google.golang.org/grpc"
)

// startReplicaServer starts a Dynamo replica gRPC server bound to 127.0.0.1:0 and returns its address and a cleanup.
// testReplica implements the subset of DynamoServer used by anti-entropy tests.
type testReplica struct {
    pb.UnimplementedDynamoServer
    NodeID string
    Store  *storage.Store
}

func (r *testReplica) ReplicaGet(ctx context.Context, req *pb.ReplicaGetReq) (*pb.ReplicaGetResp, error) {
    arr, err := r.Store.Get(req.GetKey())
    if err != nil { return &pb.ReplicaGetResp{Err: err.Error()}, nil }
    out := make([]*pb.ItemProto, 0, len(arr))
    for _, it := range arr {
        ip := &pb.ItemProto{Key: it.Key, Value: it.Value, Ctx: &pb.ContextProto{}}
        ip.Ctx.Tombstone = it.Ctx.Tombstone
        ip.Ctx.Origin = it.Ctx.Origin
        ip.Ctx.Ts = it.Ctx.Ts
        for n, c := range it.Ctx.Vc { ip.Ctx.Entries = append(ip.Ctx.Entries, &pb.VClockEntry{Node: n, Counter: c}) }
        out = append(out, ip)
    }
    return &pb.ReplicaGetResp{Items: out}, nil
}

func (r *testReplica) TransferItem(ctx context.Context, req *pb.ItemTransferReq) (*pb.ItemTransferResp, error) {
    for _, p := range req.GetItems() {
        if p == nil { continue }
        sc := storage.Context{Vc: map[string]uint64{}, Tombstone: p.Ctx.GetTombstone(), Origin: p.Ctx.GetOrigin(), Ts: p.Ctx.GetTs()}
        for _, e := range p.Ctx.GetEntries() { sc.Vc[e.Node] = e.Counter }
        existing, _ := r.Store.Get(p.Key)
        merged := append(existing[:0:0], existing...)
        incoming := storage.Item{Key: p.Key, Value: p.Value, Ctx: sc}
        // simple merge: append if not present; if identical ts/value, keep one
        found := false
        for _, it := range merged { if it.Ctx.Ts == incoming.Ctx.Ts && string(it.Value) == string(incoming.Value) { found = true; break } }
        if !found { merged = append(merged, incoming) }
        _ = r.Store.Put(p.Key, merged)
    }
    return &pb.ItemTransferResp{Ok: true}, nil
}

func merkleRoot(leaves [][]byte) []byte {
    if len(leaves) == 0 { return make([]byte, 20) }
    cur := leaves
    for len(cur) > 1 {
        next := [][]byte{}
        for i := 0; i < len(cur); i += 2 {
            if i+1 == len(cur) { h := sha1.New(); h.Write([]byte("node")); h.Write(cur[i]); h.Write(cur[i]); next = append(next, h.Sum(nil)) } else {
                h := sha1.New(); h.Write([]byte("node")); h.Write(cur[i]); h.Write(cur[i+1]); next = append(next, h.Sum(nil))
            }
        }
        cur = next
    }
    return cur[0][:20]
}

func (r *testReplica) StreamMerkle(req *pb.MerkleReq, stream pb.Dynamo_StreamMerkleServer) error {
    // collect latest sibling by ts for hashing
    type leaf struct{ key string; hash [20]byte }
    leaves := []leaf{}
    _ = r.Store.IterateAll(func(k string, items []storage.Item) error {
        if len(items) == 0 { return nil }
        latest := items[0]
        for _, it := range items[1:] { if it.Ctx.Ts > latest.Ctx.Ts { latest = it } }
        h := sha1.New(); h.Write([]byte(k)); h.Write([]byte{0}); h.Write(latest.Value)
        tsBuf := make([]byte, 8); binary.LittleEndian.PutUint64(tsBuf, uint64(latest.Ctx.Ts)); h.Write(tsBuf)
        var sum [20]byte; copy(sum[:], h.Sum(nil))
        leaves = append(leaves, leaf{key: k, hash: sum})
        return nil
    })
    // root
    hashes := make([][]byte, len(leaves))
    for i,l := range leaves { hashes[i] = l.hash[:] }
    root := merkleRoot(hashes)
    rootChunk := make([]byte, 1+20); rootChunk[0] = 0; copy(rootChunk[1:], root)
    if err := stream.Send(&pb.MerkleChunk{Vnode: req.GetVnode(), Chunk: rootChunk}); err != nil { return err }
    // single batch for test simplicity
    buf := make([]byte, 1+4)
    buf[0] = 1
    binary.LittleEndian.PutUint32(buf[1:], uint32(len(leaves)))
    for _, lf := range leaves {
        k := []byte(lf.key)
        part := make([]byte, 2+len(k)+20)
        binary.LittleEndian.PutUint16(part[:2], uint16(len(k)))
        copy(part[2:2+len(k)], k)
        copy(part[2+len(k):], lf.hash[:])
        buf = append(buf, part...)
    }
    return stream.Send(&pb.MerkleChunk{Vnode: req.GetVnode(), Chunk: buf})
}

func startReplicaServer(t *testing.T, nodeID string, st *storage.Store) (string, func()) {
    t.Helper()
    lis, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil { t.Fatalf("listen: %v", err) }
    s := grpc.NewServer()
    pb.RegisterDynamoServer(s, &testReplica{NodeID: nodeID, Store: st})
    go func() { _ = s.Serve(lis) }()
    cleanup := func() {
        s.GracefulStop()
        _ = lis.Close()
    }
    return lis.Addr().String(), cleanup
}

func openTempStore(t *testing.T) *storage.Store {
    t.Helper()
    dir := t.TempDir()
    dbPath := filepath.Join(dir, "data.db")
    st, err := storage.Open(dbPath)
    if err != nil { t.Fatalf("open store: %v", err) }
    t.Cleanup(func(){ _ = st.Close(); _ = os.RemoveAll(dir) })
    return st
}

func TestAntiEntropyRepairsMissingKeys(t *testing.T) {
    t.Skip("anti-entropy disabled; skipping test")
    // Stores
    storeA := openTempStore(t)
    storeB := openTempStore(t)

    // Put key only on B
    const keyMissing = "user:K"
    if err := storeB.Put(keyMissing, []storage.Item{{Key: keyMissing, Value: []byte("v1"), Ctx: storage.Context{Vc: map[string]uint64{}, Origin: "nodeB", Ts: time.Now().UnixNano()}}}); err != nil {
        t.Fatalf("put on B: %v", err)
    }

    // Start replicas
    addrA, cleanupA := startReplicaServer(t, "nodeA", storeA)
    defer cleanupA()
    addrB, cleanupB := startReplicaServer(t, "nodeB", storeB)
    defer cleanupB()

    // Ring and coordinator for A
    r := ring.New()
    r.Add(ring.NodeID("nodeA"), []uint64{1})
    r.Add(ring.NodeID("nodeB"), []uint64{2})
    nodeAddr := map[string]string{"nodeA": addrA, "nodeB": addrB}
    coordA := New("nodeA", r, nodeAddr, storeA, 3, 2, 2)

    // Run one anti-entropy pass
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    coordA.runOnceAntiEntropy(ctx)

    // Validate key repaired on A
    items, err := storeA.Get(keyMissing)
    if err != nil { t.Fatalf("get on A: %v", err) }
    if len(items) == 0 { t.Fatalf("expected repaired item on A, got none") }
    // Value match
    if string(items[0].Value) != "v1" {
        t.Fatalf("unexpected value on A: %q", string(items[0].Value))
    }
}

func TestAntiEntropyNoOpWhenInSync(t *testing.T) {
    t.Skip("anti-entropy disabled; skipping test")
    // Stores
    storeA := openTempStore(t)
    storeB := openTempStore(t)

    // Put same key/value on both
    const keySame = "user:S"
    it := []storage.Item{{Key: keySame, Value: []byte("same"), Ctx: storage.Context{Vc: map[string]uint64{}, Origin: "seed", Ts: time.Now().UnixNano()}}}
    if err := storeA.Put(keySame, it); err != nil { t.Fatalf("put A: %v", err) }
    if err := storeB.Put(keySame, it); err != nil { t.Fatalf("put B: %v", err) }

    addrA, cleanupA := startReplicaServer(t, "nodeA", storeA)
    defer cleanupA()
    addrB, cleanupB := startReplicaServer(t, "nodeB", storeB)
    defer cleanupB()

    r := ring.New()
    r.Add(ring.NodeID("nodeA"), []uint64{1})
    r.Add(ring.NodeID("nodeB"), []uint64{2})
    nodeAddr := map[string]string{"nodeA": addrA, "nodeB": addrB}
    coordA := New("nodeA", r, nodeAddr, storeA, 3, 2, 2)

    // Capture pre-state
    before, err := storeA.Get(keySame)
    if err != nil { t.Fatalf("get before: %v", err) }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    coordA.runOnceAntiEntropy(ctx)

    after, err := storeA.Get(keySame)
    if err != nil { t.Fatalf("get after: %v", err) }
    if len(before) != len(after) || string(after[0].Value) != "same" {
        t.Fatalf("unexpected change after anti-entropy; before=%d after=%d val=%q", len(before), len(after), string(after[0].Value))
    }
}
