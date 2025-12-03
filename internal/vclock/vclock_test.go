package vclock

import "testing"

func TestCompareVClockCases(t *testing.T) {
    // a > b
    a := VClock{"A": 2}
    b := VClock{"A": 1}
    if got := CompareVClock(a, b); got != 1 { t.Fatalf("expected 1 (a>b), got %d", got) }

    // a < b
    a = VClock{"A": 1}
    b = VClock{"A": 2}
    if got := CompareVClock(a, b); got != -1 { t.Fatalf("expected -1 (a<b), got %d", got) }

    // equal
    a = VClock{"A": 1, "B": 3}
    b = VClock{"A": 1, "B": 3}
    if got := CompareVClock(a, b); got != 2 { t.Fatalf("expected 2 (equal), got %d", got) }

    // concurrent: A higher on A, B higher on B
    a = VClock{"A": 2, "B": 1}
    b = VClock{"A": 1, "B": 2}
    if got := CompareVClock(a, b); got != 0 { t.Fatalf("expected 0 (concurrent), got %d", got) }
}
