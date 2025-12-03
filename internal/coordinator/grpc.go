package coordinator

import (
	"context"

	pb "dynamodb1/dynamodb1/proto"
	"dynamodb1/internal/storage"
)

// GRPCServer wraps the Coordinator to expose gRPC methods.
type GRPCServer struct {
	pb.UnimplementedCoordinatorServer
	Coord *Coordinator
}

func NewGRPCServer(c *Coordinator) *GRPCServer {
	return &GRPCServer{Coord: c}
}

// Put implements pb.CoordinatorServer.Put by delegating to Coordinator.Put.
func (s *GRPCServer) Put(ctx context.Context, req *pb.CoordinatorPutReq) (*pb.CoordinatorPutResp, error) {
	if s == nil || s.Coord == nil {
		return &pb.CoordinatorPutResp{Err: "coordinator not available"}, nil
	}
	if req == nil {
		return &pb.CoordinatorPutResp{Err: "empty request"}, nil
	}

	// convert proto context -> storage.Context (if provided)
	var clientCtx *storage.Context
	if req.Ctx != nil {
		clientCtx = &storage.Context{
			Vc:        map[string]uint64{},
			Tombstone: req.Ctx.Tombstone,
			Origin:    req.Ctx.Origin,
			Ts:        req.Ctx.Ts,
		}
		for _, e := range req.Ctx.Entries {
			clientCtx.Vc[e.Node] = e.Counter
		}
	}

	W := int(req.W)
	ctxs, err := s.Coord.Put(ctx, req.Key, req.Value, clientCtx, W)
	if err != nil {
		return &pb.CoordinatorPutResp{Err: err.Error()}, nil
	}

	out := &pb.CoordinatorPutResp{}
	for _, sc := range ctxs {
		cp := &pb.ContextProto{
			Tombstone: sc.Tombstone,
			Origin:    sc.Origin,
			Ts:        sc.Ts,
		}
		for k, v := range sc.Vc {
			cp.Entries = append(cp.Entries, &pb.VClockEntry{Node: k, Counter: v})
		}
		out.StoredContexts = append(out.StoredContexts, cp)
	}
	return out, nil
}

// Get implements pb.CoordinatorServer.Get by delegating to Coordinator.Get.
func (s *GRPCServer) Get(ctx context.Context, req *pb.CoordinatorGetReq) (*pb.CoordinatorGetResp, error) {
	if s == nil || s.Coord == nil {
		return &pb.CoordinatorGetResp{Err: "coordinator not available"}, nil
	}
	if req == nil {
		return &pb.CoordinatorGetResp{Err: "empty request"}, nil
	}

	R := int(req.R)
	items, err := s.Coord.Get(ctx, req.Key, R)
	if err != nil {
		return &pb.CoordinatorGetResp{Err: err.Error()}, nil
	}

	out := &pb.CoordinatorGetResp{}
	for _, it := range items {
		p := &pb.ItemProto{
			Key: it.Key,
			Value: it.Value,
			Ctx: &pb.ContextProto{
				Tombstone: it.Ctx.Tombstone,
				Origin:    it.Ctx.Origin,
				Ts:        it.Ctx.Ts,
			},
		}
		for k, v := range it.Ctx.Vc {
			p.Ctx.Entries = append(p.Ctx.Entries, &pb.VClockEntry{Node: k, Counter: v})
		}
		out.Items = append(out.Items, p)
	}
	return out, nil
}