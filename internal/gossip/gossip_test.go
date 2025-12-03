package gossip

import (
	"dynamodb1/internal/ring"
	"encoding/json"
	"testing"
	"time"
)

// TestNewSWIMInitializesSelf verifies NewSWIM seeds self member correctly.
func TestNewSWIMInitializesSelf(t *testing.T) {
    r := ring.New()
    g := NewSWIM("nodeA", []string{"nodeB"}, SWIMConfig{}, r, "")
    if g == nil { t.Fatalf("expected SWIM instance") }
    g.mu.Lock()
    selfInfo, ok := g.members["nodeA"]
    g.mu.Unlock()
    if !ok || !selfInfo.Up || selfInfo.State != MemberAlive { t.Fatalf("self member not initialized correctly: %+v", selfInfo) }
}

// TestDecodeWireAndDispatchPing exercises decode of a ping control packet.
func TestDecodeWireAndDispatchPing(t *testing.T) {
    r := ring.New()
    g := NewSWIM("nodeA", []string{}, SWIMConfig{}, r, "")
    // send a ping control message; expect no panic and pending map unchanged (side-effect minimal)
    beforeSeq := g.seqGen
    pkt := []byte(`{"type":"ping","from":"nodeB","seq":42}`)
    decodeWireAndDispatch(pkt, g)
    if g.seqGen != beforeSeq { t.Fatalf("unexpected seqGen change on ping decode") }
}

// TestReceiveGossipMerges ensures ReceiveGossip updates membership.
func TestReceiveGossipMerges(t *testing.T) {
    r := ring.New()
    g := NewSWIM("nodeA", []string{}, SWIMConfig{}, r, "")
    members := []MemberInfo{{ID: "nodeB", Addr: "nodeB", Inc: 1, Up: true, LastSeen: time.Now().UnixNano(), State: MemberAlive}}
    b, _ := json.Marshal(members)
    decodeWireAndDispatch(b, g)
    g.mu.Lock()
    _, ok := g.members["nodeB"]
    g.mu.Unlock()
    if !ok { t.Fatalf("expected nodeB to be merged into membership") }
}
