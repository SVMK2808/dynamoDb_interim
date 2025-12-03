package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"dynamodb1/internal/coordinator"
	"dynamodb1/internal/gossip"
	"dynamodb1/internal/hintstore"
	"dynamodb1/internal/replica"
	"dynamodb1/internal/ring"
	"dynamodb1/internal/storage"

	pb "dynamodb1/dynamodb1/dynamodb1/proto"
	pb2 "dynamodb1/dynamodb1/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	id := flag.String("id", "nodeA", "node id")
	port := flag.Int("port", 7001, "grpc port")
	data := flag.String("data", "./data.db", "bolt path")

	// cluster peers: comma separated list of nodeID=host:port (including this node optionally)
	peers := flag.String("peers", "nodeA=localhost:7001,nodeB=localhost:7002,nodeC=localhost:7003", "comma-separated nodeID=addr list")
	vnodeCount := flag.Int("vnodes", 16, "virtual nodes per physical node")
	N := flag.Int("N", 3, "replication factor (preference list size)")
	R := flag.Int("R", 2, "read quorum")
	W := flag.Int("W", 2, "write quorum")

	flag.Parse()

	// Validate tunable consistency flags (N, R, W) with sensible defaults
	if *N < 1 {
		log.Printf("[bootstrap] invalid N=%d; defaulting to 3", *N)
		*N = 3
	}
	if *R < 1 {
		log.Printf("[bootstrap] invalid R=%d; defaulting to 2", *R)
		*R = 2
	}
	if *W < 1 {
		log.Printf("[bootstrap] invalid W=%d; defaulting to 2", *W)
		*W = 2
	}
	if *R > *N || *W > *N {
		log.Fatalf("[bootstrap] invalid quorum settings: R=%d W=%d must be <= N=%d", *R, *W, *N)
	}

	st, err := storage.Open(*data)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// build nodeAddr map from peers flag
	nodeAddr := map[string]string{}
	for _, entry := range strings.Split(*peers, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("bad peers entry: %s (expected nodeID=host:port)", entry)
		}
		nodeID := parts[0]
		addr := parts[1]
		nodeAddr[nodeID] = addr
	}

	// log mapping for sanity
	log.Printf("[bootstrap] nodeAddr mapping: %v", nodeAddr)

	// create ring and add vnodes for every node in nodeAddr (so PreferenceListForKey returns full preference lists)
	r := ring.New()
	myNodeID := ring.NodeID(*id)
	for nid := range nodeAddr {
		nodeID := ring.NodeID(nid)
		var tokens []uint64
		tokensFromDisk, err := ring.LoadTokens(*data, nodeID)
		if err == nil {
			tokens = tokensFromDisk
			log.Printf("[bootstrap] loaded %d vnode tokens for node %s from disk", len(tokens), nid)
		} else {
			if os.IsNotExist(err) {
				tokens = ring.GenTokens(nodeID, *vnodeCount)
				if perr := ring.PersistTokens(*data, nodeID, tokens); perr != nil {
					log.Printf("[bootstrap] warning: failed to persist tokens for node %s: %v", nid, perr)
				} else {
					log.Printf("[bootstrap] persisted %d vnode tokens for node %s", len(tokens), nid)
				}
			} else {
				log.Fatalf("cannot load tokens for node %s: %v", nid, err)
			}
		}
		r.Add(nodeID, tokens)
		log.Printf("[bootstrap] ring: added node=%s vnodes=%d", nid, len(tokens))
	}

	// sanity: if local node wasn't present in peers list, add it and its tokens
	if _, ok := nodeAddr[*id]; !ok {
		log.Printf("[bootstrap] warning: local node %s not present in peers list; adding local node entry", *id)
		nodeAddr[*id] = fmt.Sprintf("localhost:%d", *port)

		var tokens []uint64
		tokensFromDisk, err := ring.LoadTokens(*data, myNodeID)
		if err == nil {
			tokens = tokensFromDisk
			log.Printf("[bootstrap] loaded %d vnode tokens for local node %s from disk", len(tokens), *id)
		} else {
			if os.IsNotExist(err) {
				tokens = ring.GenTokens(myNodeID, *vnodeCount)
				if perr := ring.PersistTokens(*data, myNodeID, tokens); perr != nil {
					log.Printf("[bootstrap] warning: failed to persist tokens for local node %s: %v", *id, perr)
				} else {
					log.Printf("[bootstrap] persisted %d vnode tokens for local node %s", len(tokens), *id)
				}
			} else {
				log.Fatalf("cannot load tokens for local node %s: %v", *id, err)
			}
		}
		r.Add(myNodeID, tokens)
	}

	// create coordinator and attach to replica
	peerList := make([]string, 0, len(nodeAddr))
	for nid := range nodeAddr {
		if nid != *id{
			peerList = append(peerList, nid)
		}
	}

	cfg := gossip.SWIMConfig{
		PushInterval:    1 * time.Second,
		PingTimeout:     300 * time.Millisecond,
		IndirectTimeout: 300 * time.Millisecond,
		SuspectTimeout:  3 * time.Second,
		IndirectFanout:  3,
	}
	gossip.BootstrapNodeAddr = nodeAddr
	bindAddr := nodeAddr[*id]
	g := gossip.NewSWIM(*id, peerList, cfg, r, bindAddr)
	stopGossip := make(chan struct{})
	log.Println("[boot] SWIM starting")
	go g.Start(stopGossip)

	// Load tokens from disk (if any) and broadcast our join to peers so they 
	// add us to their ring. Ignore load error (will generate tokens later if missing)
	tokensFromDisk, _ := ring.LoadTokens(*data, myNodeID)

	// ensure local ring contains self immediately
	if r != nil {
		r.Add(ring.NodeID(*id), tokensFromDisk)
		log.Printf("[ring] added self=%s vnodes=%d (startup)", *id, len(tokensFromDisk))
	}

	// BroadcastJoin expects id, addr and token list

	g.BroadcastJoin(*id, nodeAddr[*id], tokensFromDisk)
	log.Printf("[gossip] BroadcastJoin sent id=%s addr=%s tokens=%d", *id, nodeAddr[*id], len(tokensFromDisk))
	// --- end gossip initialization --
	coord := coordinator.New(*id, r, nodeAddr, st, *N, *R, *W)
	// Initialize durable hint store and worker
	hintsPath := *data + ".hints.db"
	if strings.HasSuffix(*data, ".db") {
		hintsPath = strings.TrimSuffix(*data, ".db") + ".hints.db"
	}
	// ensure directory exists
	if dir := filepath.Dir(hintsPath); dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	hs, err := hintstore.New(hintsPath)
	if err != nil {
		log.Fatalf("hintstore init: %v", err)
	}
	defer hs.Close()
	coord.HintDB = hs
	// optional: tune coordinator timeouts if you want
	coord.DialTimeout = 1 * time.Second
	coord.RPCTimeout = 2 * time.Second
	coord.CoordTimeout = 8 * time.Second

	// create replica and inject coordinator
	rep := &replica.Replica{
		NodeID: *id,
		Store:  st,
		Coord:  coord, // <-- important: make sure Replica struct has Coord *coordinator.Coordinator
	}

	// start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()

	reflection.Register(s)
	pb.RegisterDynamoServer(s, rep)

	coordGRPC := coordinator.NewGRPCServer(coord)
	pb2.RegisterCoordinatorServer(s, coordGRPC)

	// anti-entropy disabled: no worker started

	// start hint apply worker
	hintsStop := make(chan struct{})
	coord.StartHintApplyWorker(hintsStop, 5*time.Second)

	go func() {
		log.Printf("starting gRPC on %d (id=%s)", *port, *id)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// Top-level context for this process; will be canceled on shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = ctx

	// NOTE: startup sanity coordinator Put/Get removed to avoid blocking startup.
	// If you need automated health checks, run them from a separate admin command
	// or make them non-blocking (small timeouts). For now log that sanity test is disabled.
	log.Printf("[bootstrap] coordinator sanity test disabled to avoid blocking startup")

	// lifecycle
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})
	// Improved shutdown sequence:
	// - listen for SIGINT/SIGTERM
	// - cancel the top-level context to notify background workers
	// - tell coordinator to shutdown (with bounded timeout)
	// - gracefully stop gRPC server, but force stop after a timeout
	// - ensure we always close coordinator connections as a last resort
	go func() {
		<-stop
		log.Printf("shutdown requested: starting graceful shutdown...")

		// 1) Cancel top-level contexts / notify workers
		cancel()

		// 2) Coordinator cleanup (close client conns, etc.) with bounded timeout
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 8*time.Second)
		// coord.Shutdown currently does not return an error; call it and ensure the
		// timeout cancel is invoked.
		coord.Shutdown(shutdownCtx)
		shutdownCancel()
		log.Printf("coord shutdown completed")

	// anti-entropy disabled: nothing to stop
		close(hintsStop)

		close(stopGossip)
		log.Printf("gossip stopped")

		// 3) stop accepting new RPCs and let in-flight RPCs finish
		graceDone := make(chan struct{})
		go func() {
			s.GracefulStop() // blocks until handlers return
			close(graceDone)
		}()

		// 4) wait with timeout, then force stop
		forceTimeout := 10 * time.Second
		select {
		case <-graceDone:
			log.Println("gRPC graceful shutdown completed")
		case <-time.After(forceTimeout):
			log.Printf("gRPC graceful shutdown timed out after %s, forcing stop", forceTimeout)
			// ensure coordinator connections closed and force stop
			coord.CloseConns()
			s.Stop()
		}

		close(done)
	}()

	<-done
	log.Printf("server exiting")
}