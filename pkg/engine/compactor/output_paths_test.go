package compactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIndexMergeOutputPath_Deterministic verifies the same inputs always
// produce the same path. This is what makes racing coordinators safe: they
// produce identical plans → identical paths → racing workers existence-check
// and dedupe.
func TestIndexMergeOutputPath_Deterministic(t *testing.T) {
	window := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	ids := []string{"obj-a#0", "obj-b#3", "obj-a#1"}

	p1 := IndexMergeOutputPath("tenant-1", window, 1, 0, ids)
	p2 := IndexMergeOutputPath("tenant-1", window, 1, 0, ids)
	require.Equal(t, p1, p2, "identical inputs must produce identical paths")
}

// TestIndexMergeOutputPath_ShuffledSectionIDsStable verifies the section-ID
// slice order does not affect the path. The planner emits piles in a
// deterministic order today, but cross-coordinator safety requires the path
// to be invariant under any permutation of the same section ID set.
func TestIndexMergeOutputPath_ShuffledSectionIDsStable(t *testing.T) {
	window := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	idsA := []string{"obj-a#0", "obj-b#3", "obj-c#1"}
	idsB := []string{"obj-c#1", "obj-a#0", "obj-b#3"} // same set, shuffled

	pA := IndexMergeOutputPath("tenant-1", window, 1, 0, idsA)
	pB := IndexMergeOutputPath("tenant-1", window, 1, 0, idsB)
	require.Equal(t, pA, pB, "section ID order must not affect the path")
}

// TestIndexMergeOutputPath_AnyInputChangeChangesPath verifies every input
// participates in the hash. If any of (tenant, window, planVersion, binIndex,
// sectionIDs) changes, the path must change. Otherwise distinct plans could
// alias to the same output and a stale-rewrite race becomes possible.
func TestIndexMergeOutputPath_AnyInputChangeChangesPath(t *testing.T) {
	window := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	base := IndexMergeOutputPath("tenant-1", window, 1, 0, []string{"a#0", "b#0"})

	cases := map[string]string{
		"tenant":       IndexMergeOutputPath("tenant-2", window, 1, 0, []string{"a#0", "b#0"}),
		"window":       IndexMergeOutputPath("tenant-1", window.Add(time.Hour), 1, 0, []string{"a#0", "b#0"}),
		"plan version": IndexMergeOutputPath("tenant-1", window, 2, 0, []string{"a#0", "b#0"}),
		"bin index":    IndexMergeOutputPath("tenant-1", window, 1, 1, []string{"a#0", "b#0"}),
		"section ids":  IndexMergeOutputPath("tenant-1", window, 1, 0, []string{"a#0", "b#1"}),
	}
	for name, got := range cases {
		t.Run(name, func(t *testing.T) {
			require.NotEqual(t, base, got, "%s change must change the path", name)
		})
	}
}
