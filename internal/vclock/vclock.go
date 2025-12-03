package vclock

import (
	"fmt"
	"sort"
	"strings"
)

type NodeID string

type VClock map[NodeID]uint64

func New() VClock { return VClock{} }

func (vc VClock) Clone() VClock {
	out := VClock{}
	for k, v := range vc {
		out[k] = v
	}
	return out
}

func (vc VClock) Inc(node NodeID) VClock {
	n := vc.Clone()
	n[node] = n[node] + 1
	return n
}

// CompareVClock returns -1 if a<b, 1 if a>b, 0 if concurrent, 2 if equal
func CompareVClock(a, b VClock) int {
	aLeB := true
	bLeA := true
	for k, av := range a {
		if bv, ok := b[k]; ok {
			if av > bv {
				aLeB = false
			}
			if bv > av {
				bLeA = false
			}
		} else {
			if av > 0 {
				aLeB = false
			}
		}
	}
	for k, bv := range b {
		if av, ok := a[k]; ok {
			if bv > av {
				bLeA = false
			}
			if av > bv {
				aLeB = false
			}
		} else {
			if bv > 0 {
				bLeA = false
			}
		}
	}
	if aLeB && bLeA {
		return 2
	}
	if aLeB && !bLeA {
		return -1
	}
	if !aLeB && bLeA {
		return 1
	}
	return 0
}

func Merge(a, b VClock) VClock {
	out := New()
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if ov, ok := out[k]; !ok || v > ov {
			out[k] = v
		}
	}
	return out
}

func (vc VClock) String() string {
	parts := make([]string, 0, len(vc))
	for k, v := range vc {
		parts = append(parts, fmt.Sprintf("%s:%d", string(k), v))
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}
