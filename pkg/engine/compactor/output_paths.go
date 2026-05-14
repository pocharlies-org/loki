package compactor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// IndexMergeOutputPath generates a deterministic object-storage key for an
// IndexMerge task's output. Two coordinators that read the same source ToC
// and plan the same task produce identical paths and dedupe via the
// bucket's view: the second worker's existence-check finds the file and
// short-circuits.
//
// Path layout: indexes/tenants/<tenant>/<sha2>/<sha-rest>
//
// SHA-256 input (newline-separated, canonical form):
//
//	v=<planVersion>
//	t=<tenant>
//	w=<windowStartUnixNanos>
//	b=<binIndex>
//	s=<sortedSectionID>   (one line per ID, lexicographically sorted)
//	...
//
// The recommended sectionID form is "<objectPath>#<sectionIndex>" — any
// tuple unique within the input set works. The function sorts a copy so
// callers may pass sectionIDs in whatever order the planner emits.
// The tenant string is mixed into the hash so outputs for different tenants
// are guaranteed to differ even with otherwise-identical inputs.
func IndexMergeOutputPath(
	tenant string,
	windowStart time.Time,
	planVersion uint,
	binIndex int,
	sectionIDs []string,
) string {
	sorted := append([]string(nil), sectionIDs...)
	sort.Strings(sorted)

	var b strings.Builder
	fmt.Fprintf(&b, "v=%d\n", planVersion)
	fmt.Fprintf(&b, "t=%s\n", tenant)
	fmt.Fprintf(&b, "w=%d\n", windowStart.UTC().UnixNano())
	fmt.Fprintf(&b, "b=%d\n", binIndex)
	for _, id := range sorted {
		fmt.Fprintf(&b, "s=%s\n", id)
	}

	sum := sha256.Sum256([]byte(b.String()))
	encoded := hex.EncodeToString(sum[:])
	return fmt.Sprintf("indexes/tenants/%s/%s/%s", tenant, encoded[:2], encoded[2:])
}
