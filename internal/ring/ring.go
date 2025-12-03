// internal/ring/ring.go
package ring

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"sync"
)

type NodeID string

// Ring is a simple consistent-hash ring mapping token -> node.
// It is safe for concurrent use.
type Ring struct {
	mu sync.RWMutex

	// token -> node
	tokens map[uint64]NodeID

	// node -> list of tokens (for removal / iteration convenience)
	nodeTokens map[NodeID][]uint64

	// sorted token list for fast preference lookup
	sortedTokens []uint64
}

// New returns an empty ring.
func New() *Ring {
	return &Ring{
		tokens:      make(map[uint64]NodeID),
		nodeTokens:  make(map[NodeID][]uint64),
		sortedTokens: []uint64{},
	}
}

// GenTokens produces deterministic pseudo-random tokens for a node.
// Keep this stable across restarts so vnodes land in same place.
func GenTokens(node NodeID, vnodes int) []uint64 {
	out := make([]uint64, 0, vnodes)
	for i := 0; i < vnodes; i++ {
		h := sha1.Sum([]byte(fmt.Sprintf("%s-%d", node, i)))
		// use first 8 bytes as uint64
		val := binary.BigEndian.Uint64(h[:8])
		out = append(out, val)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Add adds a physical node with the provided tokens (vnodes). Safe concurrently.
func (r *Ring) Add(node NodeID, tokens []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// add tokens and update nodeTokens
	for _, t := range tokens {
		// if token already present, overwrite node assignment
		prev, ok := r.tokens[t]
		if ok {
			// remove from previous nodeTokens list
			pls := r.nodeTokens[prev]
			newlist := make([]uint64, 0, len(pls))
			for _, x := range pls {
				if x != t {
					newlist = append(newlist, x)
				}
			}
			r.nodeTokens[prev] = newlist
		}
		r.tokens[t] = node
		r.nodeTokens[node] = append(r.nodeTokens[node], t)
	}

	// rebuild sortedTokens
	r.rebuildSorted()
}

// Remove removes a physical node and its tokens from the ring.
func (r *Ring) Remove(node NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	toks := r.nodeTokens[node]
	for _, t := range toks {
		delete(r.tokens, t)
	}
	delete(r.nodeTokens, node)
	r.rebuildSorted()
}

// rebuildSorted rebuilds the cached, sorted token slice.
// Caller must hold r.mu.
func (r *Ring) rebuildSorted() {
	s := make([]uint64, 0, len(r.tokens))
	for t := range r.tokens {
		s = append(s, t)
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	r.sortedTokens = s
}

// PreferenceListForKey returns up to `count` nodeIDs responsible for the given key.
// It returns a *copy* (new slice) so caller can use it safely without concurrent mutation issues.
func (r *Ring) PreferenceListForKey(key string, count int) []NodeID {
	
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedTokens) == 0 {
		return nil
	}

	h := sha1.Sum([]byte(key))
	keyTok := binary.BigEndian.Uint64(h[:8])

	// binary search to find first token >= keyTok
	idx := sort.Search(len(r.sortedTokens), func(i int) bool { return r.sortedTokens[i] >= keyTok })
	if idx == len(r.sortedTokens) {
		// wrap
		idx = 0
	}

	result := make([]NodeID, 0, count)
	seen := map[NodeID]bool{}
	i := idx
	for len(result) < count && len(seen) < len(r.nodeTokens) {
		tok := r.sortedTokens[i]
		node := r.tokens[tok]
		if !seen[node] {
			result = append(result, node)
			seen[node] = true
		}
		i++
		if i >= len(r.sortedTokens) {
			i = 0
		}
		// if we've looped a lot and nodes < count, break to avoid infinite loop
		if len(seen) == 0 && len(r.sortedTokens) == 0 {
			break
		}
	}

	// return a copy (already new slice) — safe for concurrent use
	log.Printf("[ring] PreferenceListForKey key=%q count=%d -> %v", key, count, result)
	return result
}

// Nodes returns a snapshot list of nodes currently in ring (copy).
func (r *Ring) Nodes() []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]NodeID, 0, len(r.nodeTokens))
	for n := range r.nodeTokens {
		out = append(out, n)
	}
	return out
}