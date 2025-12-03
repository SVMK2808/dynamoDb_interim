package storage

import (
	"crypto/sha1"
	"encoding/hex"
	"sort"
)

// MerkleNode represents a hash at a prefix or leaf.
type MerkleNode struct {
	Prefix string // hex-encoded prefix identifying subtree (e.g., key prefix or range)
	Hash   string // hex-encoded hash
	Leaf   bool   // true if this is a leaf
}

func sha1Sum(b []byte) []byte {
	h := sha1.New()
	h.Write(b)
	return h.Sum(nil)
}

func leafHash(key, val []byte) []byte {
	h := sha1.New()
	h.Write([]byte("leaf:"))
	h.Write(key)
	h.Write([]byte{0})
	h.Write(val)
	return h.Sum(nil)
}

func innerHash(left, right []byte) []byte {
	h := sha1.New()
	h.Write([]byte("node:"))
	h.Write(left)
	h.Write(right)
	return h.Sum(nil)
}

// BuildMerkleRootFromSortedLeaves returns the merkle root (raw bytes) given sorted leaves.
func BuildMerkleRootFromSortedLeaves(leaves [][]byte) []byte {
	if len(leaves) == 0 {
		return nil
	}
	cur := leaves
	for len(cur) > 1 {
		var next [][]byte
		for i := 0; i < len(cur); i += 2 {
			if i+1 == len(cur) {
				// duplicate last
				next = append(next, innerHash(cur[i], cur[i]))
			} else {
				next = append(next, innerHash(cur[i], cur[i+1]))
			}
		}
		cur = next
	}
	return cur[0]
}

// ComputeMerkleRootForPrefix constructs the Merkle root for keys in a vnode filtered by prefix.
// - vnode: vnode id to restrict on (store side must know which keys belong to vnode).
// - prefix: key prefix (may be empty for whole vnode).
// - store: store pointer implementing ListKeysForVnodeWithPrefix
func ComputeMerkleRootForPrefix(store *Store, vnode uint64, prefix []byte) (root []byte, leavesCount int) {
	// Gather keys and values from store for this vnode and prefix
	kvs := store.ListKeysForVnodeWithPrefix(vnode, prefix) // map[string][]byte

	// Build sorted leaf hashes
	leaves := make([][]byte, 0, len(kvs))
	for k, v := range kvs {
		leaves = append(leaves, leafHash([]byte(k), v))
	}
	// Deterministic order
	sort.Slice(leaves, func(i, j int) bool {
		return hex.EncodeToString(leaves[i]) < hex.EncodeToString(leaves[j])
	})
	root = BuildMerkleRootFromSortedLeaves(leaves)
	return root, len(leaves)
}

// StreamMerkleNodesForPrefix yields MerkleNode messages for a prefix by returning root for the prefix
// plus optionally child prefixes (coarse to fine). This is a helper you can use to implement a server
// streaming RPC that returns nodes for a given vnode/prefix. For simplicity we only return the root
// and optionally leaf digests when the key-count <= threshold.
func StreamMerkleNodesForPrefix(store *Store, vnode uint64, prefix []byte, threshold int) ([]MerkleNode, error) {
	// If threshold <= 0, we just return the root for this prefix.
	root, cnt := ComputeMerkleRootForPrefix(store, vnode, prefix)
	nodes := make([]MerkleNode, 0, 1)
	nodes = append(nodes, MerkleNode{
		Prefix: hex.EncodeToString(prefix),
		Hash:   hex.EncodeToString(root),
		Leaf:   cnt <= threshold,
	})
	// If leaf-sized, include leaf hashes as separate entries
	if cnt > 0 && cnt <= threshold {
		// include each leaf
		kvs := store.ListKeysForVnodeWithPrefix(vnode, prefix)
		keys := make([]string, 0, len(kvs))
		for k := range kvs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h := leafHash([]byte(k), kvs[k])
			nodes = append(nodes, MerkleNode{
				Prefix: hex.EncodeToString([]byte(k)), // use full key as prefix for leaf
				Hash:   hex.EncodeToString(h),
				Leaf:   true,
			})
		}
	}
	return nodes, nil
}