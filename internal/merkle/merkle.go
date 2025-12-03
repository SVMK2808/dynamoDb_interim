package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)


func Hash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }


func RootFromKeys(keys []string) string {
	// Filter out hinted keys and any internal meta keys before computing the merkle root.
	// Hint keys are expected to have the prefix "hint:" (e.g. "hint:nodeA:user:101:...").
	// This keeps the merkle tree focused on user data keys only.
	filtered := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(k, "hint:") {
			continue
		}
		// skip any other internal prefixes if you use them, for example:
		// if strings.HasPrefix(k, "_meta:") { continue }
		filtered = append(filtered, k)
	}

	s := make([]string, len(filtered))
	copy(s, filtered)
	sort.Strings(s)
	if len(s) == 0 {
		return Hash([]byte{})
	}
	leafHashes := make([]string, len(s))
	for i, k := range s {
		leafHashes[i] = Hash([]byte(k))
	}
	for len(leafHashes) > 1 {
		next := []string{}
		for i := 0; i < len(leafHashes); i += 2 {
			if i+1 == len(leafHashes) {
				next = append(next, leafHashes[i])
			} else {
				next = append(next, Hash([]byte(leafHashes[i]+leafHashes[i+1])))
			}
		}
		leafHashes = next
	}
	return leafHashes[0]
}