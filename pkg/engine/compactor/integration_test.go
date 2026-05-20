package compactor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/scheduler"
	"github.com/grafana/loki/v3/pkg/engine/internal/scheduler/wire"
	"github.com/grafana/loki/v3/pkg/engine/internal/worker"
	"github.com/grafana/loki/v3/pkg/engine/internal/workflow"
)

// TestCoordinator_EndToEnd_FullCycle drives the coordinator against a real
// scheduler + worker pair wired in-process via wire.Local transport. Asserts:
//
//   - Phase 1 wrote the expected number of placeholder index objects at
//     deterministic paths.
//   - Phase 2 atomically swapped the ToC: source paths removed, output paths
//     added with the right timestamps.
//   - Other tenants' rows survive byte-equivalent across the swap.
//
// The stub IndexMerge does not actually merge sections, but the orchestration
// shape is identical to the real-executor case: the test exercises the full
// scheduler → worker → ReplaceIndexPointers flow.
func TestCoordinator_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bucket := objstore.NewInMemBucket()
	window := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC).Truncate(metastore.MetastoreWindowSize)

	// Seed: 3 indexes for "acme" with overlapping timestamp ranges (forces >1 pile).
	// Plus an "untouched" tenant with one index to verify cross-tenant
	// isolation across the ToC swap.
	writeToCWithIndexes(t, ctx, bucket, map[string][]testIndex{
		"acme": {
			{path: "indexes/aa/src-0", start: window.Add(1 * time.Hour), end: window.Add(5 * time.Hour)},
			{path: "indexes/bb/src-1", start: window.Add(2 * time.Hour), end: window.Add(6 * time.Hour)},
			{path: "indexes/cc/src-2", start: window.Add(3 * time.Hour), end: window.Add(7 * time.Hour)},
		},
		"untouched": {
			{path: "indexes/aa/src-0", start: window.Add(1 * time.Hour), end: window.Add(5 * time.Hour)},
			{path: "indexes/dd/idx-d-0", start: window.Add(1 * time.Hour), end: window.Add(2 * time.Hour)},
		},
	})

	// Bring up a real scheduler + worker pair using wire.Local transport.
	sched, _ := startInProcessSchedulerAndWorker(t, ctx, bucket)

	// Construct the metastore writer + coordinator.
	tocWriter := metastore.NewTableOfContentsWriter(bucket, log.NewNopLogger())
	c := &coordinator{
		cfg: Config{
			Enabled:                   true,
			PollingInterval:           1 * time.Second,
			MaxRunsPerTask:            2,
			IndexMergeTaskTTL:         10 * time.Second,
			ToCConsolidateTimeout:     10 * time.Second,
			ConsolidationSLO:          3 * time.Hour,
			MaxRunningCompactionTasks: 4,
			PlanVersion:               1,
			Scheduler:                 SchedulerConfig{Endpoint: defaultEndpoint},
		},
		logger: log.NewNopLogger(),
		bucket: bucket,
		runPlan: func(rpCtx context.Context, opts workflow.Options, plan *physical.Plan) error {
			return runPlan(rpCtx, log.NewNopLogger(), sched, opts, plan)
		},
		metastoreWriter: tocWriter,
		clock:           func() time.Time { return window.Add(1 * time.Hour) },
		metrics:         newCoordinatorMetrics(prometheus.NewRegistry()),
	}

	// --- Cycle 1: 3 sources → ⌈P/K⌉ outputs ---
	preCycle1 := mustLoadTenant(t, ctx, bucket, window, "acme")
	require.Len(t, preCycle1, 3, "sanity: 3 source indexes seeded")
	_, err := c.runTenantCycle(ctx, "acme", window, preCycle1)
	require.NoError(t, err)

	postCycle1 := mustLoadTenants(t, ctx, bucket, window)
	require.Less(t, len(postCycle1["acme"]), 3,
		"cycle 1 must reduce acme's index count from 3 to fewer")
	require.Equal(t,
		[]string{"indexes/aa/src-0", "indexes/dd/idx-d-0"},
		pathsOf(postCycle1["untouched"]),
		"untouched tenant must be byte-identical across the swap")

	// Phase 1 output objects must exist at the deterministic paths.
	for _, entry := range postCycle1["acme"] {
		_, err := bucket.Attributes(ctx, entry.Path)
		require.NoError(t, err, "phase 1 output %q must exist after the swap", entry.Path)
	}
	// Pre-swap source paths must be gone from acme's section.
	acmePaths := pathsOf(postCycle1["acme"])
	for _, p := range []string{"indexes/aa/src-0", "indexes/bb/src-1", "indexes/cc/src-2"} {
		require.NotContains(t, acmePaths, p,
			"source path %q must be removed from acme's section after the swap", p)
	}

	// But the same path must remain present in any OTHER tenant's section that
	// also referenced it — ReplaceIndexPointers is scoped to one tenant.
	// indexes/aa/src-0 is shared between acme and untouched here.
	untouchedPaths := pathsOf(postCycle1["untouched"])
	for _, p := range []string{"indexes/aa/src-0", "indexes/dd/idx-d-0"} {
		require.Contains(t, untouchedPaths, p,
			"shared path %q must remain in untouched's section; the swap is tenant-scoped", p)
	}

	// --- Cycle 2: drive against the post-swap ToC. Should converge further. ---
	indexesC2 := mustLoadTenant(t, ctx, bucket, window, "acme")
	if len(indexesC2) > 1 {
		_, err := c.runTenantCycle(ctx, "acme", window, indexesC2)
		require.NoError(t, err)
		postCycle2 := mustLoadTenants(t, ctx, bucket, window)
		t.Logf("cycle 2: acme went from %d → %d indexes", len(indexesC2), len(postCycle2["acme"]))
		require.LessOrEqual(t, len(postCycle2["acme"]), len(postCycle1["acme"]),
			"cycle 2 must not increase index count")
	}

	// --- Cycle 3+: drive until convergence (≤1 index). Bounded by max-iters
	// so a regression doesn't infinite-loop. ---
	for i := range 5 {
		acmeIdx := mustLoadTenant(t, ctx, bucket, window, "acme")
		if len(acmeIdx) <= 1 {
			break
		}
		_, err := c.runTenantCycle(ctx, "acme", window, acmeIdx)
		require.NoError(t, err)
		t.Logf("convergence loop iter %d: acme → %d indexes", i,
			len(mustLoadTenant(t, ctx, bucket, window, "acme")))
	}
	final := mustLoadTenants(t, ctx, bucket, window)
	require.LessOrEqual(t, len(final["acme"]), 1,
		"after multiple cycles, acme must converge to ≤ 1 covering index")
	require.ElementsMatch(t,
		[]string{"indexes/aa/src-0", "indexes/dd/idx-d-0"},
		pathsOf(final["untouched"]),
		"untouched tenant must remain byte-identical across all cycles (including the path shared with acme)")
}

// startInProcessSchedulerAndWorker brings up a wire.Local scheduler + worker
// pair sharing the supplied bucket. Both register cleanup hooks on t.
func startInProcessSchedulerAndWorker(t *testing.T, ctx context.Context, bucket objstore.Bucket) (*scheduler.Scheduler, *worker.Worker) {
	t.Helper()

	schedulerListener := &wire.Local{Address: wire.LocalScheduler}
	workerListener := &wire.Local{Address: wire.LocalWorker}
	dialer := wire.NewLocalDialer(schedulerListener, workerListener)

	sched, err := scheduler.New(scheduler.Config{
		Logger:   log.NewNopLogger(),
		Listener: schedulerListener,
	})
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, sched.Service()))
	t.Cleanup(func() {
		stopCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = services.StopAndAwaitTerminated(stopCtx, sched.Service())
	})

	ms := metastore.NewObjectMetastore(bucket, metastore.Config{}, log.NewNopLogger(),
		metastore.NewObjectMetastoreMetrics(prometheus.NewRegistry()))
	w, err := worker.New(worker.Config{
		Logger:           log.NewNopLogger(),
		Bucket:           bucket,
		Metastore:        ms,
		BatchSize:        2048,
		Dialer:           dialer,
		Listener:         workerListener,
		SchedulerAddress: wire.LocalScheduler,
		NumThreads:       2,
	})
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, w.Service()))
	t.Cleanup(func() {
		stopCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = services.StopAndAwaitTerminated(stopCtx, w.Service())
	})

	return sched, w
}

func mustLoadTenants(t *testing.T, ctx context.Context, b objstore.Bucket, window time.Time) tenantIndexes {
	t.Helper()
	got, err := loadTenantIndexes(ctx, b, window)
	require.NoError(t, err)
	return got
}

func mustLoadTenant(t *testing.T, ctx context.Context, b objstore.Bucket, window time.Time, tenant string) []indexEntry {
	t.Helper()
	return mustLoadTenants(t, ctx, b, window)[tenant]
}

func pathsOf(entries []indexEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Path
	}
	return out
}

// TestCoordinator_OrphanReuse_OnePathDriftsPerAddition documents the
// observable boundary of the orphan-reuse property under ToC mutations
// between cycles.
//
// Property: patience-sort's best-fit appending attaches any incoming entry
// to the existing run with the largest top strictly less than the entry's
// MinTimestamp. Consequently, every non-head addition extends EXACTLY one
// existing run and therefore changes EXACTLY one task's OutputIndexPath.
// The other K-1 tasks' paths are preserved across cycles.
//
// Operational consequence: each ReplaceIndexPointers retry-exhaustion event
// produces at most one wasted K-way merge and one permanent bucket orphan
// per affected pile, bounded per cycle. Orphans are query-invisible and
// reclaimed by bucket lifecycle policy.
//
// The two sub-tests demonstrate the property at two boundary cases:
//   - Pure-tail addition (G.MinTs > all existing run tops): G attaches to
//     the last run; the last task's path drifts.
//   - Near-tail addition (G.MinTs slots between two run tops): G attaches
//     to the larger of the two; that pile's task drifts.
//
// In both cases the number of preserved paths is the same (2 of 3); only
// which task drifts differs.
func TestCoordinator_OrphanReuse_OnePathDriftsPerAddition(t *testing.T) {
	cases := []struct {
		name   string
		gStart time.Duration
		gEnd   time.Duration
	}{
		{
			name:   "pure-tail-add", // G attaches to Run 5 (top=11h30m) → Task 2 drifts.
			gStart: 11*time.Hour + 45*time.Minute,
			gEnd:   11*time.Hour + 50*time.Minute,
		},
		{
			name:   "near-tail-add-between-run2-and-run3", // G attaches to Run 2 (top=9h) → Task 1 drifts.
			gStart: 9*time.Hour + 30*time.Minute,
			gEnd:   9*time.Hour + 45*time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			bucket := objstore.NewInMemBucket()
			window := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC).Truncate(metastore.MetastoreWindowSize)

			// 6 entries with overlapping ranges → patience-sort places each in its
			// own run (P=6); K=2 → 3 tasks.
			seedAtoF := map[string][]testIndex{
				"acme": {
					{path: "indexes/aa/src-A", start: window.Add(1 * time.Hour), end: window.Add(7 * time.Hour)},
					{path: "indexes/bb/src-B", start: window.Add(2 * time.Hour), end: window.Add(8 * time.Hour)},
					{path: "indexes/cc/src-C", start: window.Add(3 * time.Hour), end: window.Add(9 * time.Hour)},
					{path: "indexes/dd/src-D", start: window.Add(4 * time.Hour), end: window.Add(10 * time.Hour)},
					{path: "indexes/ee/src-E", start: window.Add(5 * time.Hour), end: window.Add(11 * time.Hour)},
					{path: "indexes/ff/src-F", start: window.Add(6 * time.Hour), end: window.Add(11*time.Hour + 30*time.Minute)},
				},
			}
			writeToCWithIndexes(t, ctx, bucket, seedAtoF)

			sched, _ := startInProcessSchedulerAndWorker(t, ctx, bucket)
			realToCWriter := metastore.NewTableOfContentsWriter(bucket, log.NewNopLogger())
			gated := &gatedReplacer{inner: realToCWriter, failWithExhausted: true}

			c := &coordinator{
				cfg: Config{
					Enabled:                   true,
					PollingInterval:           1 * time.Second,
					MaxRunsPerTask:            2,
					IndexMergeTaskTTL:         10 * time.Second,
					ToCConsolidateTimeout:     10 * time.Second,
					MaxRunningCompactionTasks: 4,
					PlanVersion:               1,
					ConsolidationSLO:          3 * time.Hour,
					Scheduler:                 SchedulerConfig{Endpoint: defaultEndpoint},
				},
				logger:          log.NewNopLogger(),
				bucket:          bucket,
				metastoreWriter: gated,
				clock:           func() time.Time { return window.Add(1 * time.Hour) },
				metrics:         newCoordinatorMetrics(prometheus.NewRegistry()),
			}

			// --- Cycle N: capture OutputIndexPaths; Phase 2 gated to fail. ---
			var (
				cycleNMu      sync.Mutex
				cycleNOutputs []string
			)
			c.runPlan = func(rpCtx context.Context, opts workflow.Options, plan *physical.Plan) error {
				root, err := plan.Root()
				if err != nil {
					return err
				}
				if im, ok := root.(*physical.IndexMerge); ok {
					cycleNMu.Lock()
					cycleNOutputs = append(cycleNOutputs, im.OutputIndexPath)
					cycleNMu.Unlock()
				}
				return runPlan(rpCtx, log.NewNopLogger(), sched, opts, plan)
			}

			cycleNEntries, err := loadTenantIndexes(ctx, bucket, window)
			require.NoError(t, err)
			require.Len(t, cycleNEntries["acme"], 6)

			_, err = c.runTenantCycle(ctx, "acme", window, cycleNEntries["acme"])
			require.Error(t, err)
			require.ErrorIs(t, err, metastore.ErrReplaceRetriesExhausted)

			cycleNMu.Lock()
			require.Len(t, cycleNOutputs, 3, "K=2 with P=6 ⇒ 3 tasks")
			capturedN := append([]string(nil), cycleNOutputs...)
			cycleNMu.Unlock()

			require.InDelta(t, 1.0,
				testutil.ToFloat64(c.metrics.tocSwapRetryExhaustedTotal.WithLabelValues("acme")), 0.001)

			for _, p := range capturedN {
				_, err := bucket.Attributes(ctx, p)
				require.NoError(t, err, "cycle N Phase-1 output %q must exist (orphan)", p)
			}

			// --- Tail-add G inside the window ---
			// writeToCWithIndexes appends, so only add the new G entry.
			tailAdded := map[string][]testIndex{
				"acme": {
					{
						path:  "indexes/gg/src-G",
						start: window.Add(tc.gStart),
						end:   window.Add(tc.gEnd),
					},
				},
			}
			writeToCWithIndexes(t, ctx, bucket, tailAdded)

			// --- Cycle N+1: Phase 2 allowed to succeed ---
			gated.failWithExhausted = false

			var (
				cycleN1Mu      sync.Mutex
				cycleN1Outputs []string
			)
			c.runPlan = func(rpCtx context.Context, opts workflow.Options, plan *physical.Plan) error {
				root, err := plan.Root()
				if err != nil {
					return err
				}
				if im, ok := root.(*physical.IndexMerge); ok {
					cycleN1Mu.Lock()
					cycleN1Outputs = append(cycleN1Outputs, im.OutputIndexPath)
					cycleN1Mu.Unlock()
				}
				return runPlan(rpCtx, log.NewNopLogger(), sched, opts, plan)
			}

			cycleN1Entries, err := loadTenantIndexes(ctx, bucket, window)
			require.NoError(t, err)
			require.Len(t, cycleN1Entries["acme"], 7)

			_, err = c.runTenantCycle(ctx, "acme", window, cycleN1Entries["acme"])
			require.NoError(t, err)

			cycleN1Mu.Lock()
			capturedN1 := append([]string(nil), cycleN1Outputs...)
			cycleN1Mu.Unlock()
			require.Len(t, capturedN1, 3,
				"adding G extends an existing run, P stays at 6, ⇒ still 3 tasks")

			// --- Assertions: exactly 2 of 3 paths preserved, 1 drifts ---
			cycleNSet := make(map[string]struct{}, len(capturedN))
			for _, p := range capturedN {
				cycleNSet[p] = struct{}{}
			}
			reused, fresh := 0, 0
			for _, p := range capturedN1 {
				if _, ok := cycleNSet[p]; ok {
					reused++
				} else {
					fresh++
				}
			}
			require.Equal(t, 2, reused,
				"K-1 paths must preserve under any single ToC addition; got reused=%d", reused)
			require.Equal(t, 1, fresh,
				"exactly one path must drift (the task containing the run G extended); got fresh=%d", fresh)

			// The drifted path is at binIndex=tc.driftIdx. The capture order
			// inside an errgroup is not ordered, but the cycle-N path at the
			// expected drift index is the one that's NOT in capturedN1's intersection.
			// Reconstruct the drift index by checking which cycle-N path went missing.
			cycleN1Set := make(map[string]struct{}, len(capturedN1))
			for _, p := range capturedN1 {
				cycleN1Set[p] = struct{}{}
			}
			missingFromN1 := -1
			for i, p := range capturedN {
				if _, ok := cycleN1Set[p]; !ok {
					require.Equal(t, -1, missingFromN1, "at most one cycle-N path may go missing")
					missingFromN1 = i
				}
			}
			require.GreaterOrEqual(t, missingFromN1, 0, "exactly one cycle-N path must be missing from cycle N+1")



			// The cycle-N orphan at the drifted path is now dead bytes in the
			// bucket — present, but not referenced by the post-swap ToC. Verify
			// both the orphan-still-exists and the orphan-not-referenced.
			deadPath := capturedN[missingFromN1]
			_, err = bucket.Attributes(ctx, deadPath)
			require.NoError(t, err, "drifted-task orphan %q must still exist in the bucket", deadPath)

			postSwap := mustLoadTenant(t, ctx, bucket, window, "acme")
			postSwapPaths := pathsOf(postSwap)
			require.NotContains(t, postSwapPaths, deadPath,
				"the drifted-task orphan must NOT be referenced by the post-swap ToC")
		})
	}
}

// gatedReplacer wraps a real *metastore.TableOfContentsWriter and lets tests
// toggle a "fail with ErrReplaceRetriesExhausted" mode. The synthetic error
// path is acceptable here because retry-exhaustion's real wrapping is
// covered separately in pkg/dataobj/metastore.
type gatedReplacer struct {
	inner             *metastore.TableOfContentsWriter
	failWithExhausted bool
}

func (g *gatedReplacer) ReplaceIndexPointers(
	ctx context.Context,
	window time.Time,
	tenant string,
	oldPaths []string,
	newEntries []metastore.TableOfContentsEntry,
) (bool, error) {
	if g.failWithExhausted {
		return false, fmt.Errorf("%w: simulated", metastore.ErrReplaceRetriesExhausted)
	}
	return g.inner.ReplaceIndexPointers(ctx, window, tenant, oldPaths, newEntries)
}
