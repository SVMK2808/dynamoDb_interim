// internal/coordinator/coordinator.go
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "dynamodb1/dynamodb1/dynamodb1/proto"
	"dynamodb1/internal/hintstore"
	"dynamodb1/internal/ring"
	"dynamodb1/internal/storage"
	"dynamodb1/internal/vclock"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Coordinator implements scatter/gather logic for Dynamo-style quorum reads/writes.
type Coordinator struct {
	NodeID       string
	Ring         *ring.Ring
	NodeAddr     map[string]string
	N            int // replication factor
	R            int
	W            int
	DialTimeout  time.Duration
	RPCTimeout   time.Duration
	CoordTimeout time.Duration
	HintStore    *storage.Store
	HintDB       *hintstore.Store

	muConns sync.Mutex
	conns   map[string]*grpc.ClientConn

	// shutdown helpers
	stopMu sync.Mutex
	stop   chan struct{}  // closed to signal shutdown
	wg     sync.WaitGroup // tracks in-flight coordinator operations
}

// New constructs a Coordinator. NodeAddr maps nodeID -> host:port.
func New(nodeID string, r *ring.Ring, nodeAddr map[string]string, hintStore *storage.Store, N, R, W int) *Coordinator {
	if N <= 0 {
		N = 3
	}
	if R <= 0 {
		R = 2
	}
	if W <= 0 {
		W = 2
	}
	return &Coordinator{
		NodeID:       nodeID,
		Ring:         r,
		NodeAddr:     nodeAddr,
		N:            N,
		R:            R,
		W:            W,
		DialTimeout:  500 * time.Millisecond,
		RPCTimeout:   800 * time.Millisecond,
		CoordTimeout: 3 * time.Second,
		HintStore:    hintStore,
		conns:        map[string]*grpc.ClientConn{},
		stop:         make(chan struct{}),
	}
}

// getClient returns a pb.DynamoClient for the given nodeID.
// It reuses pooled connections and dials with a timeout when needed.
func (c *Coordinator) getClient(nodeID string) (pb.DynamoClient, error) {
	addr, ok := c.NodeAddr[nodeID]
	if !ok || addr == "" {
		return nil, fmt.Errorf("unknown address for node %s", nodeID)
	}

	// check pool first
	c.muConns.Lock()
	conn := c.conns[addr]
	c.muConns.Unlock()

	if conn != nil {
		st := conn.GetState()
		// treat TransientFailure and Shutdown as unhealthy
		if st != connectivity.Shutdown && st != connectivity.TransientFailure {
			return pb.NewDynamoClient(conn), nil
		}
		// close stale connection and remove from pool
		log.Printf("[coord:%s] getClient: existing conn to %s in state=%s; recreating", c.NodeID, addr, st.String())
		_ = conn.Close()
		c.muConns.Lock()
		delete(c.conns, addr)
		c.muConns.Unlock()
	}

	// dial with timeout and block until READY or timeout
	log.Printf("[coord:%s] getClient: dialing %s", c.NodeID, addr)
	dialCtx, cancel := context.WithTimeout(context.Background(), c.DialTimeout)
	defer cancel()

	connDial, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Printf("[coord:%s] getClient: dial %s failed: %v", c.NodeID, addr, err)
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	// store in pool
	c.muConns.Lock()
	c.conns[addr] = connDial
	c.muConns.Unlock()

	log.Printf("[coord:%s] getClient: dial OK %s", c.NodeID, addr)
	return pb.NewDynamoClient(connDial), nil
}

// CloseConns closes all pooled gRPC connections.
func (c *Coordinator) CloseConns() {
	c.muConns.Lock()
	defer c.muConns.Unlock()
	for addr, cc := range c.conns {
		_ = cc.Close()
		delete(c.conns, addr)
		log.Printf("[coord:%s] CloseConns: closed conn to %s", c.NodeID, addr)
	}
}

// Shutdown signals the coordinator to stop, waits for outstanding operations to finish (with timeout),
// then closes pooled connections.
func (c *Coordinator) Shutdown(ctx context.Context) {
	// idempotent close of stop channel
	c.stopMu.Lock()
	select {
	case <-c.stop:
		// already closed
	default:
		close(c.stop)
	}
	c.stopMu.Unlock()

	// wait for in-flight operations to finish, but bounded by ctx
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// clean
	case <-ctx.Done():
		log.Printf("[coord:%s] Shutdown: timeout waiting for in-flight ops: %v", c.NodeID, ctx.Err())
	}

	// finally close all pooled gRPC connections
	c.CloseConns()
}

// Put implements scatter/gather write for a key/value. clientCtx optional.
// W parameter overrides coordinator default if non-zero.
func (c *Coordinator) Put(ctx context.Context, key string, val []byte, clientCtx *storage.Context, W int) ([]storage.Context, error) {
	// Do not start new work if shutdown requested.
	select {
	case <-c.stop:
		return nil, fmt.Errorf("coordinator shutting down")
	default:
	}

	// track in-flight
	c.wg.Add(1)
	defer c.wg.Done()

	if W == 0 {
		W = c.W
	}
	// PreferenceListForKey returns []ring.NodeID; convert to []string
	nodesNodeID := c.Ring.PreferenceListForKey(key, c.N)
	if len(nodesNodeID) == 0 {
		return nil, fmt.Errorf("no nodes in ring")
	}
	nodes := make([]string, 0, len(nodesNodeID))
	for _, nid := range nodesNodeID {
		nodes = append(nodes, string(nid))
	}

	log.Printf("[coord-debug:%s] reached PUT start for key=%s at %s", c.NodeID, key, time.Now().Format(time.RFC3339Nano))

	var base vclock.VClock
	if clientCtx != nil && clientCtx.Vc != nil {
		base = make(vclock.VClock)
		for k, v := range clientCtx.Vc {
			base[vclock.NodeID(k)] = v
		}
	} else {
		base = vclock.New()
	}
	base = base.Inc(vclock.NodeID(c.NodeID))

	item := storage.Item{
		Key:   key,
		Value: val,
		Ctx: storage.Context{
			Vc:        map[string]uint64{},
			Tombstone: false,
			Origin:    c.NodeID,
			Ts:        time.Now().UnixNano(),
		},
	}
	for k, v := range base {
		item.Ctx.Vc[string(k)] = v
	}

	type resp struct {
		node   string
		err    error
		stored []storage.Item
	}
	respCh := make(chan resp, len(nodes))
	ctxCoord, cancel := context.WithTimeout(ctx, c.CoordTimeout)
	defer cancel()

	log.Printf("[coord:%s] PUT start key=%q val_len=%d quorum W=%d pref=%v", c.NodeID, key, len(val), W, nodes)

    // log resolved addresses for the preference list
    addrs := make([]string, 0, len(nodes))
    for _, n := range nodes {
        addrs = append(addrs, fmt.Sprintf("%s->%s", n, c.NodeAddr[n]))
    }
    log.Printf("[coord:%s] PUT pref nodes/addrs: %v", c.NodeID, addrs)

    // record the moment before launching per-replica goroutines (single global log)
    log.Printf("[coord-debug:%s] before launching per-replica goroutines for key=%s nodes=%v at %s", c.NodeID, key, nodes, time.Now().Format(time.RFC3339Nano))
	
	for _, nid := range nodes {
		nid := nid
		go func(n string) {
			// Make RPC client
			addr := c.NodeAddr[n]
			if addr == "" {
				log.Printf("[coord:%s] PUT preflight: no address for node %s (nodeAddr missing key)", c.NodeID, n)
			}else {
				d := 400 * time.Millisecond
				conn, err := net.DialTimeout("tcp", addr, d)
				if err != nil {
					log.Printf("[coord:%s] PUT preflight: tcp dial to %s failed (node %s) timeout=%s err=%v",c.NodeID, addr, n, d, err)
				}else {
					_ = conn.Close()
					log.Printf("[coord:%s] PUT preflight: tcp connect OK to %s (node %s)", c.NodeID, addr, n)
				}
			}

			// short-circuit local replica to avoid self-dial
if n == c.NodeID {
    log.Printf("[coord:%s] local replica write for key=%s", c.NodeID, key)
   if c.HintStore == nil{
        log.Printf("[coord:%s] ERROR: local store not set!" , c.NodeID)
        respCh <- resp{node: n, err: fmt.Errorf("local store not set")}
        return
    }

	err := c.HintStore.Put(key, []storage.Item{item})
	if err != nil {
		log.Printf("[coord:%s] local Put error:%v,", c.NodeID, err)
		respCh <- resp{node: n, err: err}
		return
	}
	log.Printf("[coord:%s] local replica write OK for key=%s", c.NodeID, key)
    respCh <- resp{node: n, stored: []storage.Item{item}}
    return
}
			client, err := c.getClient(n)
			if err != nil {
				log.Printf("[coord:%s] PUT: getClient error for %s: %v", c.NodeID, n, err)
				respCh <- resp{node: n, err: err}
				return
			}

			// Build request
			rpcCtx, cancelRPC := context.WithTimeout(ctxCoord, c.RPCTimeout)
			defer cancelRPC()

			req := &pb.ReplicaPutReq{Item: &pb.ItemProto{Key: item.Key, Value: item.Value, Ctx: &pb.ContextProto{}}}
			for k, v := range item.Ctx.Vc {
				req.Item.Ctx.Entries = append(req.Item.Ctx.Entries, &pb.VClockEntry{Node: k, Counter: v})
			}
			req.Item.Ctx.Tombstone = item.Ctx.Tombstone
			req.Item.Ctx.Origin = item.Ctx.Origin
			req.Item.Ctx.Ts = item.Ctx.Ts

			log.Printf("[coord:%s] PUT: sending ReplicaPut to=%s key=%s", c.NodeID, n, item.Key)

			md := metadata.Pairs("role", "coordinator", "x-dynamo-internal", "true")
			rpcCtx = metadata.NewOutgoingContext(rpcCtx, md)
			r, err := client.ReplicaPut(rpcCtx, req)
			if err != nil {
				log.Printf("[coord:%s] PUT: ReplicaPut err to=%s key=%s err=%v", c.NodeID, n, item.Key, err)
				respCh <- resp{node: n, err: err}
				return
			}
			log.Printf("[coord:%s] PUT: ReplicaPut success from=%s key=%s", c.NodeID, n, item.Key)

			stored := make([]storage.Item, 0, len(r.Stored))
			for _, p := range r.Stored {
				st := protoToStorageItem(p)
				stored = append(stored, st)
			}
			respCh <- resp{node: n, err: nil, stored: stored}
		}(nid)
	}

	success := 0
	var storedCtxs []storage.Context
	failed := []string{}
	for i := 0; i < len(nodes); i++ {
		select {
		case <-ctxCoord.Done():
			log.Printf("[coord:%s] PUT timeout key=%q successes=%d failed=%v", c.NodeID, key, success, failed)
			return nil, fmt.Errorf("coordinator timeout: got %d successes", success)
		case r := <-respCh:
			if r.err == nil {
				success++
				for _, it := range r.stored {
					storedCtxs = append(storedCtxs, it.Ctx)
				}
				log.Printf("[coord:%s] PUT ack from=%s key=%q success=%d/%d", c.NodeID, r.node, key, success, W)
				if success >= W {
					if len(failed) > 0 {
						go c.persistHints(failed, item)
						log.Printf("[coord:%s] PUT stored hints for failed replicas=%v key=%q", c.NodeID, failed, key)
					}
					log.Printf("[coord:%s] PUT completed key=%q total_success=%d quorum=%d", c.NodeID, key, success, W)
					return dedupeContexts(storedCtxs), nil
				}
			} else {
				failed = append(failed, r.node)
				log.Printf("[coord:%s] PUT failed to replica=%s key=%q err=%v", c.NodeID, r.node, key, r.err)
			}
		}
	}
	log.Printf("[coord:%s] PUT failed key=%q successes=%d failed=%v", c.NodeID, key, success, failed)
	return nil, fmt.Errorf("put failed: successes=%d failed=%v", success, failed)
}

// Get implements quorum read.
func (c *Coordinator) Get(ctx context.Context, key string, R int) ([]storage.Item, error) {
	// Do not start new work if shutdown requested.
	select {
	case <-c.stop:
		return nil, fmt.Errorf("coordinator shutting down")
	default:
	}

	// track in-flight
	c.wg.Add(1)
	defer c.wg.Done()

	if R == 0 {
		R = c.R
	}
	nodesNodeID := c.Ring.PreferenceListForKey(key, c.N)
	if len(nodesNodeID) == 0 {
		return nil, fmt.Errorf("no nodes in ring")
	}
	nodes := make([]string, 0, len(nodesNodeID))
	for _, nid := range nodesNodeID {
		nodes = append(nodes, string(nid))
	}

	// log resolved addresses for the preference list
	addrs := make([]string, 0, len(nodes))
	for _, n := range nodes {
	    addrs = append(addrs, fmt.Sprintf("%s->%s", n, c.NodeAddr[n]))
	}
	log.Printf("[coord:%s] GET pref nodes/addrs: %v", c.NodeID, addrs)

	type resp struct {
		node  string
		items []storage.Item
		err   error
	}
	respCh := make(chan resp, len(nodes))
	ctxCoord, cancel := context.WithTimeout(ctx, c.CoordTimeout)
	defer cancel()

	log.Printf("[coord:%s] GET start key=%q quorum R=%d pref=%v", c.NodeID, key, R, nodes)

	for _, nid := range nodes {
		nid := nid
		go func(n string) {

			addr := c.NodeAddr[n]
			if addr == "" {
   				 log.Printf("[coord:%s] GET preflight: no address for node %s", c.NodeID, n)
				
			} else {
   				 d := 400 * time.Millisecond
    			conn, err := net.DialTimeout("tcp", addr, d)
    			if err != nil {
        			log.Printf("[coord:%s] GET preflight: tcp dial to %s failed (node %s) timeout=%s err=%v", c.NodeID, addr, n, d, err)
    			}else{
        			_ = conn.Close()
        			log.Printf("[coord:%s] GET preflight: tcp connect OK to %s (node %s)", c.NodeID, addr, n)
    			}
			}
			client, err := c.getClient(n)
			if err != nil {
				log.Printf("[coord:%s] GET getClient error for %s: %v", c.NodeID, n, err)
				respCh <- resp{node: n, err: err}
				return
			}
			rpcCtx, cancelRPC := context.WithTimeout(ctxCoord, c.RPCTimeout)
			defer cancelRPC()
			log.Printf("[coord:%s] GET: sending ReplicaGet to=%s key=%s", c.NodeID, n, key)

			md := metadata.Pairs("role", "coordinator", "x-dynamo-internal", "true")
			rpcCtx = metadata.NewOutgoingContext(rpcCtx, md)
			r, err := client.ReplicaGet(rpcCtx, &pb.ReplicaGetReq{Key: key})
			if err != nil {
				log.Printf("[coord:%s] GET: ReplicaGet err from=%s key=%s err=%v", c.NodeID, n, key, err)
				respCh <- resp{node: n, err: err}
				return
			}
			items := make([]storage.Item, 0, len(r.Items))
			for _, p := range r.Items {
				items = append(items, protoToStorageItem(p))
			}
			log.Printf("[coord:%s] GET: ReplicaGet success from=%s key=%s returned=%d", c.NodeID, n, key, len(items))
			respCh <- resp{node: n, items: items}
		}(nid)
	}

	success := 0
	collected := map[string][]storage.Item{}
	failures := []string{}
	for i := 0; i < len(nodes); i++ {
		select {
		case <-ctxCoord.Done():
			log.Printf("[coord:%s] GET timeout key=%q successes=%d failures=%v", c.NodeID, key, success, failures)
			return nil, fmt.Errorf("coordinator timeout: got %d responses", success)
		case r := <-respCh:
			if r.err == nil {
				success++
				collected[r.node] = r.items
				log.Printf("[coord:%s] GET ok from=%s key=%q success=%d/%d", c.NodeID, r.node, key, success, R)
				if success >= R {
					all := []storage.Item{}
					for _, arr := range collected {
						all = append(all, arr...)
					}
					resolved := ResolveItems(all)
					// Anti-entropy disabled: skip read-repair
					log.Printf("[coord:%s] GET completed key=%q total_success=%d quorum=%d versions=%d", c.NodeID, key, success, R, len(resolved))
					return resolved, nil
				}
			} else {
				failures = append(failures, r.node)
				log.Printf("[coord:%s] GET failed from=%s key=%q err=%v", c.NodeID, r.node, key, r.err)
			}
		}
	}
	return nil, fmt.Errorf("get failed: failures=%v", failures)
}

// ResolveItems: if one item dominates all others, return it; else return all concurrent siblings
func ResolveItems(items []storage.Item) []storage.Item {
	if len(items) == 0 {
		return nil
	}
	uniq := []storage.Item{}
	seen := map[string]bool{}
	for _, it := range items {
		k := ctxKeyForItem(it)
		if !seen[k] {
			seen[k] = true
			uniq = append(uniq, it)
		}
	}
	if len(uniq) == 1 {
		return uniq
	}
	for i, a := range uniq {
		dominates := true
		av := toVClock(a.Ctx)
		for j, b := range uniq {
			if i == j {
				continue
			}
			bv := toVClock(b.Ctx)
			cmp := vclock.CompareVClock(av, bv)
			// vclock.CompareVClock returns: 1 (a > b), -1 (a < b), 2 (equal), 0 (concurrent)
			if cmp != 1 && cmp != 2 {
				dominates = false
				break
			}
		}
		if dominates {
			return []storage.Item{a}
		}
	}
	return uniq
}

func (c *Coordinator) asyncReadRepair(key string, resolved []storage.Item, collected map[string][]storage.Item) {
	nodesNodeID := c.Ring.PreferenceListForKey(key, c.N)
	nodes := make([]string, 0, len(nodesNodeID))
	for _, nid := range nodesNodeID {
		nodes = append(nodes, string(nid))
	}
	for _, nid := range nodes {
		client, err := c.getClient(nid)
		if err != nil {
			// if we can't reach the node right now, store a hint
			go c.persistHints([]string{nid}, resolved[0])
			continue
		}
		theirs := collected[nid]
		need := false
		for _, r := range resolved {
			found := false
			for _, t := range theirs {
				if vclockEqual(toVClock(r.Ctx), toVClock(t.Ctx)) {
					found = true
					break
				}
			}
			if !found {
				need = true
				break
			}
		}
		if need {
			for _, it := range resolved {
				req := &pb.ReplicaPutReq{Item: &pb.ItemProto{Key: it.Key, Value: it.Value, Ctx: &pb.ContextProto{}}}
				for k, v := range it.Ctx.Vc {
					req.Item.Ctx.Entries = append(req.Item.Ctx.Entries, &pb.VClockEntry{Node: k, Counter: v})
				}
				req.Item.Ctx.Tombstone = it.Ctx.Tombstone
				req.Item.Ctx.Origin = it.Ctx.Origin
				req.Item.Ctx.Ts = it.Ctx.Ts
				ctx2, cancel2 := context.WithTimeout(context.Background(), c.RPCTimeout)
				md2 := metadata.Pairs("role", "coordinator", "x-dynamo-internal", "true")
				rpcCtx2 := metadata.NewOutgoingContext(ctx2, md2)
				_, err := client.ReplicaPut(rpcCtx2, req)
				cancel2()
				if err != nil {
					go c.persistHints([]string{nid}, it)
				}
			}
		}
	}
}

func (c *Coordinator) persistHints(targets []string, it storage.Item) {
	// Prefer the structured HintDB if configured; otherwise fall back to the raw HintStore
	if c.HintDB == nil {
		// fallback to legacy file-based hint storage (if present)
		if c.HintStore == nil {
			return
		}
		for _, t := range targets {
			key := fmt.Sprintf("hint:%s:%s:%d", t, it.Key, time.Now().UnixNano())
			_ = c.HintStore.Put(key, []storage.Item{it})
		}
		return
	}

	// build a pb.ItemProto from storage.Item
	p := &pb.ItemProto{Key: it.Key, Value: it.Value, Ctx: &pb.ContextProto{}}
	for k, v := range it.Ctx.Vc {
		p.Ctx.Entries = append(p.Ctx.Entries, &pb.VClockEntry{Node: k, Counter: v})
	}
	p.Ctx.Tombstone = it.Ctx.Tombstone
	p.Ctx.Origin = it.Ctx.Origin
	p.Ctx.Ts = it.Ctx.Ts

	for _, t := range targets {
		if err := c.persistHintForReplica(t, it.Key, p); err != nil {
			log.Printf("[coord:%s] persistHints: failed to persist hint for %s -> %v", c.NodeID, t, err)
		}
	}
}

func (c *Coordinator) persistHintForReplica(targetNode string, key string, item *pb.ItemProto) error {
	if c.HintDB == nil {
		return fmt.Errorf("hint DB not configured")
	}
	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	entry := &hintstore.HintEntry{
		Key:    key,
		Item:   b,
		Origin: c.NodeID,
		TS:     time.Now().UnixNano(),
	}
	return c.HintDB.PutHint(targetNode, entry)
}

func (c *Coordinator) ApplyHints(ctx context.Context, targetNode string) error {
	if c.HintDB == nil {
		return nil
	}

	hints, err := c.HintDB.GetHintsForNode(targetNode)
	if err != nil {
		return err
	}

	batchSize := 24
	for i := 0; i < len(hints); i += batchSize {
		end := i + batchSize
		if end > len(hints) {
			end = len(hints)
		}
		batch := hints[i:end]

		// get a client (reuses pooled connections)
		client, err := c.getClient(targetNode)
		if err != nil {
			log.Printf("[coord:%s][hints] getClient %s failed: %v", c.NodeID, targetNode, err)
			// continue to next batch after short sleep
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, h := range batch {
			// unmarshal stored item
			var itemProto pb.ItemProto
			if err := json.Unmarshal(h.Item, &itemProto); err != nil {
				log.Printf("[coord:%s][hints] unmarshal hint for %s failed: %v", c.NodeID, targetNode, err)
				continue
			}

			// Do not attempt to deliver hint keys that are themselves hint records
			if strings.HasPrefix(h.Key, "hint:") {
				log.Printf("[coord:%s][hints] skipping nested hint key %s", c.NodeID, h.Key)
				// try to delete this stored hint entry to avoid repeated work
				tsStr := fmt.Sprintf("%d", h.TS)
				_ = c.HintDB.DeleteHintNodeKey(targetNode, h.Key, tsStr)
				continue
			}

			// Build ReplicaPut request and perform node-to-node RPC
			req := &pb.ReplicaPutReq{Item: &pb.ItemProto{Key: itemProto.Key, Value: itemProto.Value, Ctx: &pb.ContextProto{}}}
			if itemProto.Ctx != nil {
				for _, e := range itemProto.Ctx.Entries {
					req.Item.Ctx.Entries = append(req.Item.Ctx.Entries, &pb.VClockEntry{Node: e.Node, Counter: e.Counter})
				}
				req.Item.Ctx.Tombstone = itemProto.Ctx.Tombstone
				req.Item.Ctx.Origin = itemProto.Ctx.Origin
				req.Item.Ctx.Ts = itemProto.Ctx.Ts
			}

			rpcCtx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
			// mark as internal request and explicitly allow hints
			md := metadata.Pairs("role", "coordinator", "x-dynamo-internal", "true", "x-dynamo-allow-hints", "true")
			rpcCtx = metadata.NewOutgoingContext(rpcCtx, md)
			_, err := client.ReplicaPut(rpcCtx, req)
			cancel()
			if err != nil {
				log.Printf("[coord:%s][hints] apply hint %s -> %s failed: %v", c.NodeID, h.Key, targetNode, err)
				continue
			}

			// on success delete hint entry
			tsStr := fmt.Sprintf("%d", h.TS)
			if err := c.HintDB.DeleteHintNodeKey(targetNode, h.Key, tsStr); err != nil {
				log.Printf("[coord:%s][hints] delete hint record failed: %v", c.NodeID, err)
			} else {
				log.Printf("[coord:%s][hints] applied and deleted hint %s -> %s", c.NodeID, h.Key, targetNode)
			}
		}

		// small pause between batches
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// ApplyHintsAsync launches ApplyHints(targetNode) in a goroutine with retry + backoff.
func (c *Coordinator) ApplyHintsAsync(targetNode string) {
	go func() {
		backoff := 200 * time.Millisecond
		maxBackoff := 5 * time.Second

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := c.ApplyHints(ctx, targetNode)
			cancel()

			if err == nil {
				return
			}

			// exponential backoff
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}()
}

// StartHintGC runs a background GC loop that removes stale hints older than ttl.
// stopCh should be closed on shutdown (e.g., pass c.stop).
func (c *Coordinator) StartHintGC(stopCh <-chan struct{}, interval time.Duration, ttl time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				cutoff := time.Now().Add(-ttl).UnixNano()
				removed := 0
				var err error

				// Prefer HintDB if configured; do not assume storage.Store exposes GC helpers.
				if c.HintDB != nil {
					removed, err = c.HintDB.GCOlderThan(cutoff)
				} else {
					// No structured hint DB available; skip GC for now and log a notice.
					log.Printf("[hint-gc:%s] HintDB not configured; skipping hint GC (cutoff=%d)", c.NodeID, cutoff)
					continue
				}

				if err != nil {
					log.Printf("[hint-gc:%s] error: %v", c.NodeID, err)
				} else if removed > 0 {
					log.Printf("[hint-gc:%s] removed %d stale hints (cutoff=%d)", c.NodeID, removed, cutoff)
				}
			}
		}
	}()
}


// OnNodeAlive is meant to be called by gossip when a previously-dead node becomes alive.
func (c *Coordinator) OnNodeAlive(nodeID string) {
	c.ApplyHintsAsync(nodeID)
}

// Helpers ---------------------------------------------------------


func protoToStorageItem(p *pb.ItemProto) storage.Item {
	it := storage.Item{
		Key:   p.Key,
		Value: p.Value,
		Ctx: storage.Context{
			Vc:        map[string]uint64{},
			Tombstone: false,
			Origin:    "",
			Ts:        0,
		},
	}
	if p.Ctx != nil {
		for _, e := range p.Ctx.Entries {
			it.Ctx.Vc[e.Node] = e.Counter
		}
		it.Ctx.Tombstone = p.Ctx.Tombstone
		it.Ctx.Origin = p.Ctx.Origin
		it.Ctx.Ts = p.Ctx.Ts
	}
	return it
}

func toVClock(sc storage.Context) vclock.VClock {
	vc := vclock.New()
	for k, v := range sc.Vc {
		vc[vclock.NodeID(k)] = v
	}
	return vc
}

func vclockEqual(a, b vclock.VClock) bool {
	return vclock.CompareVClock(a, b) == 2
}

func dedupeContexts(in []storage.Context) []storage.Context {
	out := []storage.Context{}
	seen := map[string]bool{}
	for _, c := range in {
		key := fmt.Sprintf("%v|%s", c.Vc, c.Origin)
		if !seen[key] {
			seen[key] = true
			out = append(out, c)
		}
	}
	return out
}

// ctxKeyForItem returns a stable string key for an item based on its context and origin.
func ctxKeyForItem(it storage.Item) string {
	return fmt.Sprintf("%v|%s|%d", it.Ctx.Vc, it.Ctx.Origin, it.Ctx.Ts)
}