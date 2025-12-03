package replica

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	pb "dynamodb1/dynamodb1/dynamodb1/proto"
	"dynamodb1/internal/coordinator"
	"dynamodb1/internal/storage"
	"dynamodb1/internal/vclock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Replica represents a node's replica server. It must implement pb.DynamoServer.
type Replica struct {
    pb.UnimplementedDynamoServer
    NodeID string
    Store  *storage.Store
    Coord  *coordinator.Coordinator // optional reference used by main initialization
}

// Forwarding control: keep replica RPCs purely node-to-node by default.
// Coordinator should perform preference-list + forwarding. To temporarily enable
// the old heuristic forwarding, call SetReplicaForwarding(true) (not recommended).
var (
	// ForwardingEnabled controls whether the replica will attempt any heuristic
	// forwarding. Default: false (disabled).
	ForwardingEnabled = false
	// ErrForwardingDisabled returned when caller attempts to use forwarding-aware helpers
	ErrForwardingDisabled = errors.New("replica forwarding is disabled; use coordinator")
)

// SetReplicaForwarding toggles replica heuristic forwarding behaviour. This is
// provided only to ease transitional testing; production runs should keep it disabled
// and rely on the Coordinator to contact replicas.
func SetReplicaForwarding(enabled bool) {
	ForwardingEnabled = enabled
}

// StreamMerkle streams a merkle representation (root + leaf batches) for this node.
// We ignore the vnode field for now and stream all keys for the replica.
// Encoding format for chunk bytes (little-endian):
// [1 byte chunkType] chunkType=0 => root hash (20 bytes SHA1)
// chunkType=1 => leaf batch: [4 bytes N] then N entries of:
//   [2 bytes keyLen][key][20 bytes sha1(key|0|value|ctxTs)]
// The first message is always root, followed by zero or more leaf batches.
func (r *Replica) StreamMerkle(req *pb.MerkleReq, stream pb.Dynamo_StreamMerkleServer) error {
	// Anti-entropy disabled. Expose as unimplemented to callers.
	return status.Errorf(codes.Unimplemented, "StreamMerkle disabled: anti-entropy turned off")
}

// StreamKeys streams canonical key -> latest-item chunks for anti-entropy.
// RPC signature: rpc StreamKeys(StreamKeysReq) returns (stream StreamKeysChunk);
// Place this method immediately after StreamMerkle (before merkleRoot).
func (r *Replica) StreamKeys(req *pb.StreamKeysReq, stream pb.Dynamo_StreamKeysServer) error {
	const batchSize = 100
	batch := make([]*pb.ItemProto, 0, batchSize)

	err := r.Store.IterateAll(func(k string, items []storage.Item) error {
		// never stream hint keys
		if strings.HasPrefix(k, "hint:") {
			return nil
		}
		if len(items) == 0 {
			return nil
		}
		// pick latest by TS (keeps canonical/latest for the key)
		latest := items[0]
		for _, it := range items[1:] {
			if it.Ctx.Ts > latest.Ctx.Ts {
				latest = it
			}
		}
		batch = append(batch, itemToProto(latest))

		if len(batch) >= batchSize {
			if err := stream.Send(&pb.StreamKeysChunk{Items: batch}); err != nil {
				return status.Errorf(codes.Internal, "stream send: %v", err)
			}
			batch = batch[:0]
		}
		return nil
	})
	if err != nil {
		return status.Errorf(codes.Internal, "iterate keys: %v", err)
	}

	if len(batch) > 0 {
		if err := stream.Send(&pb.StreamKeysChunk{Items: batch}); err != nil {
			return status.Errorf(codes.Internal, "stream send final: %v", err)
		}
	}
	return nil
}

// Anti-entropy helpers removed.
// mergeIncomingIntoExisting applies the same dominance rules as ReplicaPut:
// - if incoming dominates an existing sibling => drop that sibling
// - if an existing sibling dominates incoming => incoming ignored (not added for that comparison)
// - if concurrent => keep both
// Returns the new slice of siblings to persist.
func mergeIncomingIntoExisting(existing []storage.Item, incoming storage.Item) []storage.Item {
	kept := make([]storage.Item, 0, len(existing)+1)
	incomingKept := true

	// Build incoming vclock (vclock.VClock) from storage.Context for comparisons.
	incomingVC := make(vclock.VClock)
	for k, v := range incoming.Ctx.Vc {
		incomingVC[vclock.NodeID(k)] = v
	}

	for _, cur := range existing {
		// convert cur vc
		curVC := make(vclock.VClock)
		for k, v := range cur.Ctx.Vc {
			curVC[vclock.NodeID(k)] = v
		}

		cmp := vclock.CompareVClock(curVC, incomingVC)
		switch cmp {
		case 1:
			// cur > incoming -> keep cur; incoming is obsolete relative to this cur
			kept = append(kept, cur)
			incomingKept = false
		case -1:
			// cur < incoming -> incoming dominates cur -> drop cur (do not append)
			// (no-op)
		case 2:
			// equal: idempotent -> prefer incoming (drop cur)
			incomingKept = true
		case 0:
			// concurrent -> keep cur (and keep incoming as sibling later)
			kept = append(kept, cur)
		}
	}

	if incomingKept {
		kept = append(kept, incoming)
	}
	return kept
}


func (r *Replica) Ping(ctx context.Context, req *pb.PingReq) (*pb.PingResp, error) {
    return &pb.PingResp{Node: r.NodeID}, nil
}

// Helper: convert proto.ContextProto -> storage.Context
func protoCtxToStorage(cp *pb.ContextProto) storage.Context {
	out := storage.Context{
		Vc:        map[string]uint64{},
		Tombstone: false,
		Origin:    "",
		Ts:        0,
	}
	if cp == nil {
		return out
	}
	for _, e := range cp.Entries {
		out.Vc[e.Node] = e.Counter
	}
	out.Tombstone = cp.Tombstone
	out.Origin = cp.Origin
	out.Ts = cp.Ts
	return out
}

// Helper: storage.Context -> proto.ContextProto
func storageCtxToProto(sc storage.Context) *pb.ContextProto {
	out := &pb.ContextProto{
		Entries:   []*pb.VClockEntry{},
		Tombstone: sc.Tombstone,
		Origin:    sc.Origin,
		Ts:        sc.Ts,
	}
	for n, c := range sc.Vc {
		out.Entries = append(out.Entries, &pb.VClockEntry{Node: n, Counter: c})
	}
	return out
}

// proto Item <-> storage.Item
func protoToItem(p *pb.ItemProto) storage.Item {
	c := protoCtxToStorage(p.Ctx)
	return storage.Item{
		Key:   p.Key,
		Value: p.Value,
		Ctx:   c,
	}
}

func itemToProto(it storage.Item) *pb.ItemProto {
	return &pb.ItemProto{
		Key:   it.Key,
		Value: it.Value,
		Ctx:   storageCtxToProto(it.Ctx),
	}
}

func itemsToProto(arr []storage.Item) []*pb.ItemProto {
	out := make([]*pb.ItemProto, 0, len(arr))
	for _, it := range arr {
		out = append(out, itemToProto(it))
	}
	return out
}

// ReplicaPut implements the replica semantics
// ReplicaPut implements the replica semantics with stamping and logging.
func (r *Replica) ReplicaPut(ctx context.Context, req *pb.ReplicaPutReq) (*pb.ReplicaPutResp, error) {
    start := time.Now()
    if req == nil || req.Item == nil {
        return &pb.ReplicaPutResp{Err: "empty request"}, nil
    }

    /* detect internal (coordinator) request */
    isInternal := false
    if md, ok := metadata.FromIncomingContext(ctx); ok {
        if v := md.Get("x-dynamo-internal"); len(v) > 0 && v[0] == "true" {
            isInternal = true
        }
    }

    in := protoToItem(req.Item)
    if in.Ctx.Vc == nil {
        in.Ctx.Vc = map[string]uint64{}
    }
    if in.Ctx.Origin == "" {
        in.Ctx.Origin = r.NodeID
    }
    if in.Ctx.Ts == 0 {
        in.Ctx.Ts = time.Now().UnixNano()
    }
    if _, ok := in.Ctx.Vc[r.NodeID]; !ok {
        in.Ctx.Vc[r.NodeID] = in.Ctx.Vc[r.NodeID] + 1
    }

    log.Printf("[replica:%s] ReplicaPut key=%s vc=%v tomb=%v", r.NodeID, in.Key, in.Ctx.Vc, in.Ctx.Tombstone)

    /* reject external calls */
    if !isInternal && !ForwardingEnabled {
        return nil, status.Errorf(codes.FailedPrecondition, "clients must use coordinator")
    }

    existing, err := r.Store.Get(in.Key)
    if err != nil {
        return &pb.ReplicaPutResp{Err: fmt.Sprintf("store get: %v", err)}, nil
    }

    incomingVC := make(vclock.VClock)
    for k, v := range in.Ctx.Vc {
        incomingVC[vclock.NodeID(k)] = v
    }

    survivors := make([]storage.Item, 0, len(existing))
    needWrite := true

    for _, ex := range existing {
        curVC := make(vclock.VClock)
        for k, v := range ex.Ctx.Vc {
            curVC[vclock.NodeID(k)] = v
        }

        cmp := vclock.CompareVClock(incomingVC, curVC)
        switch cmp {
        case 1:
            continue
        case -1:
            needWrite = false
            return &pb.ReplicaPutResp{Stored: nil}, nil
        case 2:
            needWrite = false
            return &pb.ReplicaPutResp{Stored: nil}, nil
        case 0:
            survivors = append(survivors, ex)
            needWrite = true
        }
    }

    final := survivors
    if needWrite {
        final = append(final, in)
    }

    /* tombstone policy */
    keepTomb := false
    for _, s := range final {
        if s.Ctx.Tombstone {
            keepTomb = true
        }
    }
    if keepTomb {
        t := make([]storage.Item, 0, len(final))
        for _, s := range final {
            if s.Ctx.Tombstone {
                t = append(t, s)
            }
        }
        final = t
    }

    if err := r.Store.Put(in.Key, final); err != nil {
        return &pb.ReplicaPutResp{Err: fmt.Sprintf("store put: %v", err)}, nil
    }

    resp := itemsToProto(final)
    log.Printf("[replica:%s] ReplicaPut applied key=%s versions=%d dur=%s", r.NodeID, in.Key, len(resp), time.Since(start))

    return &pb.ReplicaPutResp{Stored: resp}, nil
}

// ReplicaGet: return stored siblings for a key
// ReplicaGet returns stored siblings for a key and logs the result.
func (r *Replica) ReplicaGet(ctx context.Context, req *pb.ReplicaGetReq) (*pb.ReplicaGetResp, error) {
    if req == nil {
        return &pb.ReplicaGetResp{Err: "empty request"}, nil
    }
    start := time.Now()
    arr, err := r.Store.Get(req.Key)
    if err != nil {
        return &pb.ReplicaGetResp{Err: fmt.Sprintf("store get: %v", err)}, nil
    }
    log.Printf("[replica:%s] ReplicaGet key=%s returned=%d dur=%s", r.NodeID, req.Key, len(arr), time.Since(start))
    // Check for internal metadata to allow coordinator-to-replica Get calls
	md, _ := metadata.FromIncomingContext(ctx)
	roleVals := md.Get("role")
	role := ""; if len(roleVals) > 0 { role = roleVals[0] }

    // allow-hints header check (only coordinator should set this when applying hints)
    allowHints := false
    if v := md.Get("x-dynamo-allow-hints"); len(v) > 0 && v[0] == "true" {
        allowHints = true
    }
    // if the requested key is a hint and caller isn't allowed, reject
    if strings.HasPrefix(req.Key, "hint:") && !allowHints {
        log.Printf("[replica:%s] ReplicaGet rejecting hint key=%s (allow-hints missing)", r.NodeID, req.Key)
        return nil, status.Errorf(codes.PermissionDenied, "hint keys require special allow header")
    }

	log.Printf("[replica:%s] ReplicaGet metadata role=%s origin-node=%v key=%s", r.NodeID, role, md.Get("origin-node"), req.Key)
	if role != "coordinator" && !ForwardingEnabled {
		log.Printf("[replica:%s] ReplicaGet: rejecting non-coordinator direct call key=%s role=%s", r.NodeID, req.Key, role)
		return nil, status.Errorf(codes.FailedPrecondition, "clients must use coordinator; direct replica calls are disabled")
	}

return &pb.ReplicaGetResp{Items: itemsToProto(arr)}, nil
    
}

// TransferItem applies a batch of items received from a peer during anti-entropy.
// It merges siblings using the same dominance rules as ReplicaPut and stores results.
func (r *Replica) TransferItem(ctx context.Context, req *pb.ItemTransferReq) (*pb.ItemTransferResp, error) {
	// Anti-entropy disabled. Expose as unimplemented to callers.
	return &pb.ItemTransferResp{Ok: false}, status.Errorf(codes.Unimplemented, "TransferItem disabled: anti-entropy turned off")
}
