// internal/gossip/gossip_swim.go
package gossip

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"dynamodb1/internal/ring"
)

// SWIM-style gossip (clean, compile-ready).
// Transport is pluggable via registration of send callbacks.

// BootstrapNodeAddr populated at process startup from cmd/server/main.go
// map[nodeID] = "host:port"
var BootstrapNodeAddr map[string]string
type MemberState int

type JoinMessage struct {
	NodeID 	string
	Addr	string
	Tokens	[]uint64
}

const (
	MemberAlive MemberState = iota
	MemberSuspect
	MemberDead
)

func (s MemberState) String() string {
	switch s {
	case MemberAlive:
		return "Alive"
	case MemberSuspect:
		return "Suspect"
	case MemberDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

type MemberInfo struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	Inc      uint64   `json:"inc"`
	Up       bool     `json:"up"`
	LastSeen int64    `json:"last_seen"`
	State    MemberState
	Tokens   []uint64

	SuspectAt int64    `json:"-"`
	SuspectInc uint64	`json:"-"`
}

// UDP transport: binds a PacketConn and exposes Send/Incoming
type udpTransport struct{
    conn net.PacketConn
    incoming chan []byte
    closed   chan struct{}
    bindAddr string
}

func newUDPTransport(bindAddr string) (*udpTransport, error) {
    // bind to the exact host:port (e.g., "localhost:7001")
    pc, err := net.ListenPacket("udp", bindAddr)
    if err != nil {
        log.Printf("[gossip] udp bind failed addr=%s err=%v", bindAddr, err)
        return nil, err
    }
    log.Printf("[gossip] udp bound addr=%s", bindAddr)
    t := &udpTransport{
        conn: pc,
        incoming: make(chan []byte, 256),
        closed: make(chan struct{}),
        bindAddr: bindAddr,
    }
    go t.readLoop()
    return t, nil
}

func (t *udpTransport) readLoop() {
    buf := make([]byte, 4096)
    for {
        n, addr, err := t.conn.ReadFrom(buf)
        if err != nil {
            select {
            case <-t.closed:
                return
            default:
            }
            log.Printf("[gossip] udp read error: %v", err)
            continue
        }
        // copy payload for the channel
        pkt := make([]byte, n)
        copy(pkt, buf[:n])
        // optionally decode and push higher-level WireMsg; push raw for your handler
        t.incoming <- pkt
        log.Printf("[gossip] udp recv %d bytes from %s", n, addr.String())
    }
}

func (t *udpTransport) Send(addr string, payload []byte) error {
    // Resolve the target address before attempting to write. If resolveAddr
    // returns nil, log and return an error instead of passing nil to WriteTo
    // which causes "missing address" / "invalid argument" errors.
    udpAddr := resolveAddr(addr)
    if udpAddr == nil {
        err := fmt.Errorf("invalid udp address: %s", addr)
        log.Printf("[gossip] udp send to=%s err=%v", addr, err)
        return err
    }

    // Write the payload to the resolved UDP address
    _, err := t.conn.WriteTo(payload, udpAddr)
    if err != nil {
        log.Printf("[gossip] udp send to=%s err=%v", addr, err)
    }
    return err
}

func (t *udpTransport) Close() {
    close(t.closed)
    t.conn.Close()
    close(t.incoming)
}

func resolveAddr(addr string) net.Addr {
   udpAddr, err := net.ResolveUDPAddr("udp", addr)
if err != nil {
    log.Printf("[gossip] resolveAddr: invalid addr=%q err=%v", addr, err)
    return nil
}
return udpAddr
}

// decodeWireAndDispatch decodes a received UDP packet (JSON) and
// dispatches it to the appropriate SWIM handler on g.
// Supported wire formats:
//  - JSON array of MemberInfo  -> push gossip (ReceiveGossip)
//  - {"type":"ping","from": "...","seq": N} -> ReceivePing
//  - {"type":"ack","from":"...","seq": N} -> ReceiveAck
//  - {"type":"pingreq","from":"...","target":"...","seq": N} -> ReceivePingReq
func decodeWireAndDispatch(pkt []byte, g *SWIMGossip) {
	// try MemberInfo slice (push gossip)
	var members []MemberInfo
	if err := json.Unmarshal(pkt, &members); err == nil && len(members) > 0 {
		// pick sender address from first member if present, otherwise empty string
		from := ""
		if members[0].Addr != "" {
			from = members[0].Addr
		}
		g.ReceiveGossip(from, members)
		return
	}

	// try generic control message
	var ctl struct {
		Type   string `json:"type"`
		From   string `json:"from"`
		Target string `json:"target,omitempty"`
		Seq    uint64 `json:"seq,omitempty"`
	}
	if err := json.Unmarshal(pkt, &ctl); err != nil {
		// unknown payload; log and ignore
		log.Printf("[gossip] decode: unknown packet (len=%d) err=%v", len(pkt), err)
		return
	}

	switch ctl.Type {
	case "ping":
		g.ReceivePing(ctl.From, ctl.Seq)
	case "ack":
		g.ReceiveAck(ctl.From, ctl.Seq)
	case "pingreq":
		// pingreq: ctl.From is requester, ctl.Target is the ultimate target
		g.ReceivePingReq(ctl.From, ctl.Target, ctl.Seq)
	default:
		log.Printf("[gossip] decode: unsupported type=%q from=%s", ctl.Type, ctl.From)
	}
}

// Transport callback types.
type SendPingFunc func(peer string, from string, seq uint64) error
type SendPingReqFunc func(peer string, from string, target string, seq uint64) error
type SendAckFunc func(peer string, seq uint64) error
type SendGossipFunc func(peer string, members []MemberInfo) error

type SWIMConfig struct {
	PushInterval    time.Duration
	PingTimeout     time.Duration
	IndirectTimeout time.Duration
	SuspectTimeout  time.Duration
	IndirectFanout  int
}

type SWIMGossip struct {
	self string

	mu      sync.Mutex
	members map[string]MemberInfo
	peers   []string

	pendingMu sync.Mutex
	pending   map[uint64]chan struct{}
	seqGen    uint64

	cfg     SWIMConfig
	rnd     *rand.Rand
	ringRef *ring.Ring
	udp 	*udpTransport

	// transport callbacks
	sendPing    SendPingFunc
	sendPingReq SendPingReqFunc
	sendAck     SendAckFunc
	sendGossip  SendGossipFunc
}

func (g * SWIMGossip) adaptiveSuspectTimeout() time.Duration {
	g.mu.Lock()
	n := len(g.members)
	base := g.cfg.SuspectTimeout
	g.mu.Unlock()
	if n <= 1 {
		return base
	}

	scale := math.Log1p(float64(n))
	if scale < 1.0 {
		scale = 1.0
	}
	return time.Duration(float64(base) * scale)
}

// NewSWIM constructs a SWIM instance with sane defaults when values are zero.
func NewSWIM(self string, peers []string, cfg SWIMConfig, ringRef *ring.Ring, bindAddr string) *SWIMGossip {
	if cfg.PushInterval <= 0 {
		cfg.PushInterval = 1 * time.Second
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 500 * time.Millisecond
	}
	if cfg.IndirectTimeout <= 0 {
		cfg.IndirectTimeout = 400 * time.Millisecond
	}
	if cfg.SuspectTimeout <= 0 {
		cfg.SuspectTimeout = 5 * time.Second
	}
	if cfg.IndirectFanout <= 0 {
		cfg.IndirectFanout = 3
	}
	g := &SWIMGossip{
		self:    self,
		members: map[string]MemberInfo{},
		peers:   append([]string{}, peers...),
		pending: map[uint64]chan struct{}{},
		cfg:     cfg,
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
		ringRef: ringRef,
	}
	now := time.Now().UnixNano()
	g.members[self] = MemberInfo{
		ID:       self,
		Addr:     self,
		Inc:      1,
		Up:       true,
		LastSeen: now,
		State:    MemberAlive,
	}

	    // bind UDP transport for SWIM on provided bindAddr
    if bindAddr != "" {
        t, err := newUDPTransport(bindAddr)
        if err != nil {
            // do not silently ignore bind failure — log and continue (or choose to fatal)
            log.Printf("[gossip] warning: could not bind udp transport on %s: %v", bindAddr, err)
        } else {
            g.udp = t
            // start a goroutine to deliver raw packets from transport into higher-level handlers
			            // wire send callbacks to transport if not already supplied
            // helper to resolve a peer identifier to an address (thread-safe)
// helper: resolve peer ID -> network addr (thread-safe)
// Order: 1) membership table (live), 2) BootstrapNodeAddr (startup wiring), 3) raw input
resolvePeerAddr := func(peer string) string {
    // prefer address learned from gossip
    g.mu.Lock()
    if m, ok := g.members[peer]; ok && m.Addr != "" {
        addr := m.Addr
        g.mu.Unlock()
        return addr
    }
    g.mu.Unlock()

    // fallback to bootstrap mapping provided at startup
    if BootstrapNodeAddr != nil {
        if a, ok := BootstrapNodeAddr[peer]; ok && a != "" {
            return a
        }
    }

    // final fallback: assume caller already passed host:port
    return peer
}

// wire send callbacks to transport using resolved peer addresses
g.sendGossip = func(peer string, members []MemberInfo) error {
    if g.udp == nil { return fmt.Errorf("no udp transport") }
    addr := resolvePeerAddr(peer)
    b, err := json.Marshal(members)
    if err != nil { return err }
    return g.udp.Send(addr, b)
}
g.sendPing = func(peer string, from string, seq uint64) error {
    if g.udp == nil { return fmt.Errorf("no udp transport") }
    addr := resolvePeerAddr(peer)
    msg := struct {
        Type string `json:"type"`
        From string `json:"from"`
        Seq  uint64 `json:"seq"`
    }{Type: "ping", From: from, Seq: seq}
    b, err := json.Marshal(msg)
    if err != nil { return err }
    return g.udp.Send(addr, b)
}
g.sendAck = func(peer string, seq uint64) error {
    if g.udp == nil { return fmt.Errorf("no udp transport") }
    addr := resolvePeerAddr(peer)
    msg := struct {
        Type string `json:"type"`
        From string `json:"from"`
        Seq  uint64 `json:"seq"`
    }{Type: "ack", From: g.self, Seq: seq}
    b, err := json.Marshal(msg)
    if err != nil { return err }
    return g.udp.Send(addr, b)
}
g.sendPingReq = func(peer string, from string, target string, seq uint64) error {
    if g.udp == nil { return fmt.Errorf("no udp transport") }
    addr := resolvePeerAddr(peer)
    msg := struct {
        Type   string `json:"type"`
        From   string `json:"from"`
        Target string `json:"target"`
        Seq    uint64 `json:"seq"`
    }{Type: "pingreq", From: from, Target: target, Seq: seq}
    b, err := json.Marshal(msg)
    if err != nil { return err }
    return g.udp.Send(addr, b)
}
            go func() {
                for pkt := range t.incoming {
					//Try to decode gossip push ([]MemberInfo)
                    var members []MemberInfo
                    if err := json.Unmarshal(pkt, &members); err == nil && len(members) > 0 {
                        // we received a push-gossip packet
                        // the sender address is encoded in MemberInfo.From/Addr; pick first sender address if needed
                        // call merge on main goroutine (ReceiveGossip is safe): use the Addr as from
                        from := ""
                        if len(members) > 0 {
                            from = members[0].Addr
                        }
                        g.ReceiveGossip(from, members)
                        continue
                    }
					//Fallback: decode control messages (ping / ack/ pingreq)
                    decodeWireAndDispatch(pkt, g)
                }
            }()
        }
    }
	return g
}

// Registration functions (thread-safe)
func (g *SWIMGossip) RegisterSendPing(fn SendPingFunc)    { g.mu.Lock(); g.sendPing = fn; g.mu.Unlock() }
func (g *SWIMGossip) RegisterSendPingReq(fn SendPingReqFunc) { g.mu.Lock(); g.sendPingReq = fn; g.mu.Unlock() }
func (g *SWIMGossip) RegisterSendAck(fn SendAckFunc)       { g.mu.Lock(); g.sendAck = fn; g.mu.Unlock() }
func (g *SWIMGossip) RegisterSendGossip(fn SendGossipFunc) { g.mu.Lock(); g.sendGossip = fn; g.mu.Unlock() }

// Start runs pinger, gossip pusher, and sweeper loops until stop is closed.
func (g *SWIMGossip) Start(stop <-chan struct{}) {
	// pinger loop
	go func() {
		j := time.Duration(float64(g.cfg.PushInterval) * (0.9 + rand.Float64() * 0.2))
		t := time.NewTicker(j)
		defer t.Stop()
		log.Println("[gossip] pinger loop started")
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				g.pingRandom()
			}
		}
	}()

	// gossip push loop
	go func() {
		j := time.Duration(float64(g.cfg.PushInterval) * (0.9 + rand.Float64() *0.2))
		t := time.NewTicker(j)
		defer t.Stop()
		log.Println("[gossip] gossip loop started")
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				g.pushGossip()
			}
		}
	}()

	// suspect sweeper
	go func() {
		j := time.Duration(float64(g.cfg.PushInterval) * (0.9 + rand.Float64() *0.2))
		t := time.NewTicker(j)
		defer t.Stop()
		log.Println("[gossip] sweeper loop started")
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				g.sweepSuspects()
			}
		}
	}()
}

// Update merges an externally-sourced MemberInfo into the view.
func (g *SWIMGossip) Update(m MemberInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixNano()
	if cur, ok := g.members[m.ID]; ok {
		if m.Inc > cur.Inc {
			log.Printf("[gossip] update: newer inc id=%s %d>%d up=%v", m.ID, m.Inc, cur.Inc, m.Up)
			m.LastSeen = now
			if m.Up {
				m.State = MemberAlive
			}
			g.members[m.ID] = m
			return
		}
		if m.Inc == cur.Inc {
			if m.Up && !cur.Up {
				cur.Up = true
				cur.State = MemberAlive
				log.Printf("[gossip] update: equal inc id=%s inc=%d up:%v->%v", m.ID, cur.Inc, cur.Up, m.Up)
			}
			cur.LastSeen = now
			g.members[m.ID] = cur
			return
		}
		// older incarnation - ignore
		return
	}
	// new
	m.LastSeen = now
	if m.Up {
		m.State = MemberAlive
	}
	log.Printf("[gossip] update: new member id=%s inc=%d up=%v", m.ID, m.Inc, m.Up)
	g.members[m.ID] = m
}

// Members snapshot
func (g *SWIMGossip) Members() []MemberInfo {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]MemberInfo, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, m)
	}
	return out
}

// ReceiveGossip merges a pushed member list from peer `from`.
func (g *SWIMGossip) ReceiveGossip(from string, incoming []MemberInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()
	log.Printf("[gossip] recv gossip from %s items=%d", from, len(incoming))
	now := time.Now().UnixNano()
	// mark sender seen
	if cur, ok := g.members[from]; ok {
		cur.LastSeen = now
		cur.Up = true
		cur.State = MemberAlive
		g.members[from] = cur
	} else {
		g.members[from] = MemberInfo{ID: from, Addr: from, Inc: 1, Up: true, LastSeen: now, State: MemberAlive}
	}
	// merge incoming
	for _, im := range incoming {
		if im.ID == "" {
			continue
		}

		if _, ok := g.members[im.ID]; !ok && len(im.Tokens) > 0{
			g.mu.Unlock()
			g.handleJoin(JoinMessage{NodeID: im.ID, Addr: im.Addr, Tokens: im.Tokens})
			g.mu.Lock()
		}

		if cur, ok := g.members[im.ID]; ok {
			if im.Inc > cur.Inc {
				im.LastSeen = now
				log.Printf("[gossip] merge newer inc id=%s %d>%d up=%v", im.ID, im.Inc, cur.Inc, im.Up)
				g.members[im.ID] = im
			} else if im.Inc == cur.Inc {
				if im.Up && !cur.Up {
					cur.Up = true
					cur.State = MemberAlive
				}
				log.Printf("[gossip] merge alive id=%s inc=%d (down->up)", im.ID, cur.Inc)
				
				cur.LastSeen = now
				if im.Up {
					cur.SuspectAt = 0
					cur.SuspectInc = 0
				}
				g.members[im.ID] = cur
			}
		} else {
			im.LastSeen = now
			log.Printf("[gossip] new member from gossip id=%s inc=%d up=%v", im.ID, im.Inc, im.Up)
			g.members[im.ID] = im
		}
		if len(im.Tokens) > 0 && g.ringRef != nil {
			// ring.Add expects a ring.NodeID type (alias of string); cast explicitly.
			g.ringRef.Add(ring.NodeID(im.ID), im.Tokens)
		}
	}
}

// ReceivePing called by transport when a Ping RPC arrives (from -> this node).
// We update sender's LastSeen and send ack back via registered sendAck.
func (g *SWIMGossip) ReceivePing(from string, seq uint64) {
	log.Printf("[gossip] recv ping from %s seq=%d -> ack", from, seq)
	g.mu.Lock()
	now := time.Now().UnixNano()
	if cur, ok := g.members[from]; ok {
		cur.LastSeen = now
		cur.Up = true
		cur.State = MemberAlive
		cur.SuspectAt = 0
		cur.SuspectInc = 0
		g.members[from] = cur
	} else {
		g.members[from] = MemberInfo{ID: from, Addr: from, Inc: 1, Up: true, LastSeen: now, State: MemberAlive}
	}
	sendAck := g.sendAck
	g.mu.Unlock()

	if sendAck != nil {
		if err := sendAck(from, seq); err != nil {
			log.Printf("[gossip] sendAck to %s failed: %v", from, err)
		}
	}
}

// ReceivePingReq is invoked when this node is asked to help ping target on behalf of requester `from`.
func (g *SWIMGossip) ReceivePingReq(from string, target string, seq uint64) {
	log.Printf("[gossip] recv ping-req from %s target=%s seq=%d", from, target, seq)
	g.mu.Lock()
	now := time.Now().UnixNano()
	if cur, ok := g.members[from]; ok {
		cur.LastSeen = now
		cur.Up = true
		cur.State = MemberAlive
		g.members[from] = cur
	} else {
		g.members[from] = MemberInfo{ID: from, Addr: from, Inc: 1, Up: true, LastSeen: now, State: MemberAlive}
	}
	sendPing := g.sendPing
	sendAck := g.sendAck
	g.mu.Unlock()

	if sendPing == nil {
		return
	}

	ackCh := g.waitForAck(seq)
	defer g.clearPending(seq)

	// ask target to ping helper (i.e., we ping target)
	_ = sendPing(target, g.self, seq) // best-effort

	select {
	case <-ackCh:
		// forward ack to original requester
		if sendAck != nil {
			if err := sendAck(from, seq); err != nil {
				log.Printf("[gossip] helper forward ack to %s failed: %v", from, err)
			}
		}
	case <-time.After(g.cfg.IndirectTimeout):
		// no ack received by helper within indirect timeout
	}
}

// ReceiveAck should be called by transport when an ack for seq arrives from `from`.
// It will notify pending waiters.
func (g *SWIMGossip) ReceiveAck(from string, seq uint64) {
	g.mu.Lock()
	now := time.Now().UnixNano()
	if cur, ok := g.members[from]; ok {
		cur.LastSeen = now
		cur.Up = true
		cur.State = MemberAlive
		cur.SuspectAt = 0
		cur.SuspectInc = 0
		g.members[from] = cur
		log.Printf("[gossip] ack from %s -> mark alive (inc=%d)", from, cur.Inc)
	} else {
		g.members[from] = MemberInfo{ID: from, Addr: from, Inc: 1, Up: true, LastSeen: now, State: MemberAlive}
	}
	g.mu.Unlock()

	g.pendingMu.Lock()
	ch, ok := g.pending[seq]
	if ok && ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	g.pendingMu.Unlock()
}

// pingRandom selects a target and performs direct ping, falling back to indirect ping on timeout.
func (g *SWIMGossip) pingRandom() {
	peer := g.pickRandomPeer()
	if peer == "" {
		return
	}
	log.Printf("[gossip] probe direct target=%s", peer)
	seq := g.nextSeq()
	ackCh := g.registerPending(seq)
	defer g.clearPending(seq)

	sendPing := g.sendPing
	if sendPing == nil {
		// no transport => treat as failure after timeout
		go func(p string, s uint64) {
			time.Sleep(g.cfg.PingTimeout)
			g.onPingFailed(p)
		}(peer, seq)
		return
	}

	if err := sendPing(peer, g.self, seq); err != nil {
		log.Printf("[gossip] direct ping to %s error: %v", peer, err)
		g.indirectPing(peer, seq, ackCh)
		return
	}

	select {
	case <-ackCh:
		log.Printf("[gossip] direct ack from %s seq=%d", peer, seq)
		g.markAlive(peer)
		return
	case <-time.After(g.cfg.PingTimeout):
		log.Printf("[gossip] direct timeout target=%s seq=%d -> indirect", peer, seq)
		g.indirectPing(peer, seq, ackCh)
	}
}

// indirectPing asks helpers to ping target. If no helper or no ack, mark suspect.
func (g *SWIMGossip) indirectPing(target string, seq uint64, ackCh chan struct{}) {
	helpers := g.selectHelpers(target, g.cfg.IndirectFanout)
	log.Printf("[gossip] ping-req target=%s helpers=%v seq=%d", target, helpers, seq)
	if len(helpers) == 0 {
		log.Printf("[gossip] no helpers available for target=%s", target)
		g.onPingFailed(target)
		return
	}
	log.Printf("[gossip] no transport for ping-req; target=%s", target)
	sendPingReq := g.sendPingReq
	if sendPingReq == nil {
		log.Printf("[gossip] no transport for ping-req; target=%s", target)
		g.onPingFailed(target)
		return
	}
	for _, h := range helpers {
		go func(helper string) {
			if err := sendPingReq(helper, g.self, target, seq); err != nil {
				log.Printf("[gossip] sendPingReq to %s failed: %v", helper, err)
			}
		}(h)
	}
	select {
	case <-ackCh:
		log.Printf("[gossip] indirect ack for %s seq=%d", target, seq)
		g.markAlive(target)
		return
	case <-time.After(g.cfg.IndirectTimeout):
		log.Printf("[gossip] indirect timeout target=%s seq=%d", target, seq)
		g.onPingFailed(target)
	}
}

func (g *SWIMGossip) pickRandomPeer() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	candidatesAlive := []string{}
	candidatesAny := []string{}
	for id, m := range g.members {
		if id == g.self {
			continue
		}
		candidatesAny = append(candidatesAny, id)
		if m.Up && m.State == MemberAlive {
			candidatesAlive = append(candidatesAlive, id)
		}
	}
	var list []string
	if len(candidatesAlive) > 0 {
		list = candidatesAlive
	} else {
		list = candidatesAny
	}
	if len(list) == 0 {
		if len(g.peers) == 0 {
			return ""
		}
		return g.peers[g.rnd.Intn(len(g.peers))]
	}
	return list[g.rnd.Intn(len(list))]
}

func (g *SWIMGossip) selectHelpers(target string, k int) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	cands := []string{}
	for id, m := range g.members {
		if id == g.self || id == target {
			continue
		}
		if m.State == MemberAlive || m.State == MemberSuspect {
			cands = append(cands, id)
		}
	}
	g.rnd.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	if len(cands) <= k {
		return cands
	}
	return cands[:k]
}

// mark suspect and schedule dead transition
func (g *SWIMGossip) onPingFailed(target string) {
    g.mu.Lock()
    now := time.Now().UnixNano()
    cur, ok := g.members[target]
    if !ok {
        cur = MemberInfo{ID: target, Addr: target, Inc: 1}
    }
    if cur.State != MemberSuspect {
        cur.State = MemberSuspect
        cur.Up = false
        cur.LastSeen = now
        cur.SuspectAt = now
        cur.SuspectInc = cur.Inc
        g.members[target] = cur
        log.Printf("[gossip] member %s marked suspect (inc=%d) after direct+indirect timeout", target, cur.Inc)
    }
    g.mu.Unlock()

    // confirm-dead only if still suspect with same incarnation after the window
    timeout := g.adaptiveSuspectTimeout()
    time.AfterFunc(timeout, func() {
        g.mu.Lock()
        m, ok := g.members[target]
        if ok && m.State == MemberSuspect && m.SuspectAt != 0 && m.SuspectInc == m.Inc {
            m.State = MemberDead
            m.Up = false
            g.members[target] = m
            log.Printf("[gossip] member %s marked dead after %s (inc=%d)", target, timeout, m.Inc)
        }
        g.mu.Unlock()
		
    })
}
func (g *SWIMGossip) markAlive(id string) {
	log.Printf("[gossip] mark alive id=%s", id)
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixNano()
	if m, ok := g.members[id]; ok {
		m.Up = true
		m.State = MemberAlive
		m.SuspectAt = 0
		m.SuspectInc = 0
		m.LastSeen = now
		g.members[id] = m
	} else {
		g.members[id] = MemberInfo{ID: id, Addr: id, Inc: 1, Up: true, LastSeen: now, State: MemberAlive}
	}
}

func (g *SWIMGossip) pushGossip() {
	
	g.mu.Lock()
	if len(g.peers) == 0 {
		g.mu.Unlock()
		return
	}
	peer := g.peers[g.rnd.Intn(len(g.peers))]
	log.Printf("[gossip] push gossip to %s", peer)
	members := make([]MemberInfo, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, m)
	}
	send := g.sendGossip
	g.mu.Unlock()

	if send == nil {
		return
	}
	go func() {
		if err := send(peer, members); err != nil {
			log.Printf("[gossip] push gossip to %s failed: %v", peer, err)
		}
	}()
}

func (g *SWIMGossip) sweepSuspects() {
    g.mu.Lock()
    now := time.Now().UnixNano()
    threshold := int64(g.adaptiveSuspectTimeout())
    for id, m := range g.members {
        if id == g.self {
            continue
        }
        if m.State == MemberSuspect && m.SuspectAt != 0 {
            age := now - m.SuspectAt
            if age > threshold && m.SuspectInc == m.Inc {
                m.State = MemberDead
                m.Up = false
                g.members[id] = m
                log.Printf("[gossip] sweep: member %s moved suspect->dead (age=%dms)", id, age/1e6)
            }
        }
    }
    g.mu.Unlock()
}
// ---- pending ack helpers ----
func (g *SWIMGossip) nextSeq() uint64 {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	g.seqGen++
	if g.seqGen == 0 {
		g.seqGen = 1
	}
	return g.seqGen
}

func (g *SWIMGossip) registerPending(seq uint64) chan struct{} {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	ch := make(chan struct{}, 1)
	g.pending[seq] = ch
	return ch
}

func (g *SWIMGossip) waitForAck(seq uint64) chan struct{} {
	g.pendingMu.Lock()
	ch, ok := g.pending[seq]
	if !ok {
		ch = make(chan struct{}, 1)
		g.pending[seq] = ch
	}
	g.pendingMu.Unlock()
	return ch
}

func (g *SWIMGossip) clearPending(seq uint64) {
	g.pendingMu.Lock()
	ch, ok := g.pending[seq]
	if ok {
		// close only if channel not closed; safe because we control lifecycle (registered -> clear)
		close(ch)
		delete(g.pending, seq)
	}
	g.pendingMu.Unlock()
}

// Convenience setters for transports
func (g *SWIMGossip) SetSendPing(fn SendPingFunc)    { g.mu.Lock(); g.sendPing = fn; g.mu.Unlock() }
func (g *SWIMGossip) SetSendPingReq(fn SendPingReqFunc) { g.mu.Lock(); g.sendPingReq = fn; g.mu.Unlock() }
func (g *SWIMGossip) SetSendAck(fn SendAckFunc)       { g.mu.Lock(); g.sendAck = fn; g.mu.Unlock() }
func (g *SWIMGossip) SetSendGossip(fn SendGossipFunc) { g.mu.Lock(); g.sendGossip = fn; g.mu.Unlock() }

// Helpers for external control
func (g *SWIMGossip) MarkAlive(id string) { g.markAlive(id) }
func (g *SWIMGossip) AddPeer(peer string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, p := range g.peers {
		if p == peer {
			return
		}
	}
	g.peers = append(g.peers, peer)
}
func (g *SWIMGossip) RemovePeer(peer string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(g.peers))
	for _, p := range g.peers {
		if p != peer {
			out = append(out, p)
		}
	}
	g.peers = out
}

func (g *SWIMGossip) BroadcastJoin(id, addr string, tokens []uint64) {
	// Build member record while holding lock, copy transport & peers,
	// then unlock before performing network sends.
	g.mu.Lock()
	m := MemberInfo{
		ID:       id,
		Addr:     addr,
		Inc:      1,
		Up:       true,
		LastSeen: time.Now().UnixNano(),
		State:    MemberAlive,
		Tokens:   tokens,
	}
	// update local members map
	g.members[id] = m
	if g.ringRef != nil && len(tokens) > 0 {
		// ring.Add expects a ring.NodeID type (alias of string); cast explicitly.
		g.ringRef.Add(ring.NodeID(id), tokens)
	}
	// copy references we need while still holding the lock
	send := g.sendGossip
	peers := append([]string{}, g.peers...)
	g.mu.Unlock()

	log.Printf("[gossip] broadcasting join to %d peers addr=%s tokens=%d", len(peers), addr, len(tokens))

	if send == nil {
		return
	}
	for _, peer := range peers {
		p := peer
		go func() {
			if err := send(p, []MemberInfo{m}); err != nil {
				log.Printf("[gossip] BroadcastJoin send to %s failed: %v", p, err)
			}
		}()
	}
}

func (g *SWIMGossip) handleJoin(jm JoinMessage){
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ringRef == nil {
		return
	}
	log.Printf("[gossip] received join: node=%s addr=%s tokens=%d", jm.NodeID, jm.Addr, len(jm.Tokens))
	g.ringRef.Add(ring.NodeID(jm.NodeID), jm.Tokens)
	g.members[jm.NodeID] = MemberInfo{ID: jm.NodeID, Addr: jm.Addr, Up:true, Inc:uint64(time.Now().UnixNano())}
}

// DumpMembers logs current members and their token counts.
func (g *SWIMGossip) DumpMembers() {
	g.mu.Lock()
	defer g.mu.Unlock()
	log.Println("[gossip] Current members:")
	for id, m := range g.members {
		log.Printf("  ID: %s, Addr: %s, State: %s, Tokens: %d", id, m.Addr, m.State.String(), len(m.Tokens))
	}
}
