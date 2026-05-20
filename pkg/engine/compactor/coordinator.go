package compactor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	v2 "github.com/grafana/loki/v3/pkg/dataobj/compaction/v2"
	compactionv2pb "github.com/grafana/loki/v3/pkg/dataobj/compaction/v2/proto"
	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/engine/internal/workflow"
)

// tocReplacer is the subset of *metastore.TableOfContentsWriter the
// coordinator needs.
type tocReplacer interface {
	ReplaceIndexPointers(
		ctx context.Context,
		window time.Time,
		tenant string,
		oldPaths []string,
		newEntries []metastore.TableOfContentsEntry,
	) (bool, error)
}

// runFunc executes one single-root physical.Plan as a workflow.
// Injected via the coordinator's runPlan field so unit tests can swap in a
// recorder without standing up a scheduler + worker pair.
type runFunc func(ctx context.Context, opts workflow.Options, plan *physical.Plan) error

// coordinator drives the per-cycle compaction loop. It is stateless across
// cycles: every poll tick re-reads the most-recent ToC and starts from
// scratch. Crash recovery comes from re-planning over the next poll's ToC
// view + idempotent ReplaceIndexPointers.
type coordinator struct {
	cfg             Config
	logger          log.Logger
	bucket          objstore.Bucket
	runPlan         runFunc
	metastoreWriter tocReplacer
	// clock is injected so tests can pin the current time; production
	// wiring sets it to time.Now.
	clock   func() time.Time
	metrics *coordinatorMetrics
}

// newCoordinator constructs a coordinator wired to a real
// *metastore.TableOfContentsWriter and a workflow.Runner. The runPlan field
// is set to the package-private runPlan helper closing over the supplied
// runner so unit tests can override it independently.
func newCoordinator(
	cfg Config,
	logger log.Logger,
	bucket objstore.Bucket,
	runner workflow.Runner,
	metastoreWriter *metastore.TableOfContentsWriter,
	reg prometheus.Registerer,
) *coordinator {
	return &coordinator{
		cfg:    cfg,
		logger: logger,
		bucket: bucket,
		runPlan: func(ctx context.Context, opts workflow.Options, plan *physical.Plan) error {
			return runPlan(ctx, logger, runner, opts, plan)
		},
		metastoreWriter: metastoreWriter,
		clock:           time.Now,
		metrics:         newCoordinatorMetrics(reg),
	}
}

// Run blocks until ctx is cancelled, ticking every cfg.PollingInterval and
// running one compaction cycle per tick. Per-cycle errors are logged and
// swallowed; the next tick is always attempted.
func (c *coordinator) Run(ctx context.Context) error {
	level.Info(c.logger).Log(
		"msg", "starting dataobj compaction coordinator",
		"polling_interval", c.cfg.PollingInterval,
		"max_runs_per_task", c.cfg.MaxRunsPerTask,
		"plan_version", c.cfg.PlanVersion,
	)

	// Run one cycle immediately on startup; subsequent cycles are
	// ticker-driven. Without this initial tick a fresh coordinator
	// would wait a full polling_interval before doing anything.
	c.runCycle(ctx)

	t := time.NewTicker(c.cfg.PollingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.runCycle(ctx)
		}
	}
}

// runCycle performs one full poll iteration: reads the most-recent ToC and runs
// compaction for every tenant whose window has > 1 index. All errors are logged
// and swallowed; the loop is designed to recover on the next tick by
// re-planning against the post-swap ToC.
func (c *coordinator) runCycle(ctx context.Context) {
	start := c.clock()
	currentWindow := start.UTC().Truncate(metastore.MetastoreWindowSize)
	previousWindow := currentWindow.Add(-metastore.MetastoreWindowSize)

	currentIdx, err := loadTenantIndexes(ctx, c.bucket, currentWindow)
	if err != nil {
		if !c.bucket.IsObjNotFoundErr(err) {
			level.Warn(c.logger).Log("msg", "cycle aborted: load current ToC",
				"window", currentWindow, "err", err)
			return
		}
		level.Debug(c.logger).Log("msg", "no ToC for current window", "window", currentWindow)
		currentIdx = tenantIndexes{}
	}

	// Previous window is optional; missing is normal at cluster bootstrap.
	previousIdx, err := loadTenantIndexes(ctx, c.bucket, previousWindow)
	if err != nil {
		if c.bucket.IsObjNotFoundErr(err) {
			level.Debug(c.logger).Log("msg", "no ToC for previous window", "window", previousWindow)
		} else {
			level.Warn(c.logger).Log("msg", "load previous ToC for SLI (best-effort)",
				"window", previousWindow, "err", err)
		}
		previousIdx = tenantIndexes{}
	}

	// Union tenants from both windows so a tenant present only in previous
	// (e.g., stopped writing) still gets observability.
	allTenants := make(map[string]struct{}, len(currentIdx)+len(previousIdx))
	for t := range currentIdx {
		allTenants[t] = struct{}{}
	}
	for t := range previousIdx {
		allTenants[t] = struct{}{}
	}
	if len(allTenants) == 0 {
		level.Debug(c.logger).Log("msg", "cycle: no tenants in any ToC", "window", currentWindow)
		return
	}

	var (
		converged = 0
		compacted = 0
		failed    = 0
	)
	for tenant := range allTenants {
		curEntries := currentIdx[tenant]
		prevEntries := previousIdx[tenant]

		// Compaction work runs only on current-window entries; previous-window
		// data is observed for SLI purposes only.
		if len(curEntries) <= 1 {
			level.Debug(c.logger).Log("msg", "cycle: tenant converged, skipping",
				"tenant", tenant, "indexes", len(curEntries))
			converged++
		} else {
			postEntries, terr := c.runTenantCycle(ctx, tenant, currentWindow, curEntries)
			if terr != nil {
				level.Warn(c.logger).Log("msg", "tenant cycle failed",
					"tenant", tenant, "window", currentWindow, "err", terr)
				failed++
			} else {
				compacted++
				curEntries = postEntries
			}
		}

		c.metrics.emitSLO(tenant, curEntries, prevEntries, c.clock(), c.cfg.ConsolidationSLO)
	}

	duration := c.clock().Sub(start)
	level.Info(c.logger).Log(
		"msg", "cycle complete",
		"window", currentWindow,
		"duration", duration,
		"tenants_total", len(allTenants),
		"tenants_compacted", compacted,
		"tenants_converged", converged,
		"tenants_failed", failed,
	)

	if duration > c.cfg.PollingInterval {
		level.Warn(c.logger).Log(
			"msg", "cycle duration exceeded polling interval; next tick will be dropped",
			"duration", duration,
			"polling_interval", c.cfg.PollingInterval,
		)
	}
}

// runTenantCycle performs the per-(tenant, window) compaction.
//
// Returns the entries after compaction (which may differ from input on success)
// and any error. Returned errors are wrapped for the caller; (swapped=false,
// err=nil) from ReplaceIndexPointers is treated as success because it signals
// either a race-loss to a sibling coordinator or that the cycle's source paths
// were already removed by a previous cycle.
func (c *coordinator) runTenantCycle(
	ctx context.Context,
	tenant string,
	window time.Time,
	entries []indexEntry,
) ([]indexEntry, error) {
	// Plan.
	sections := sectionRefsFor(entries)
	tasks := v2.Plan(ctx, sections, tenant, c.cfg.MaxRunsPerTask)
	if len(tasks) == 0 {
		level.Debug(c.logger).Log("msg", "tenant cycle: planner produced no tasks",
			"tenant", tenant, "window", window)
		return nil, nil
	}

	// Compute deterministic output paths per task.
	outputs := make([]string, len(tasks))
	for i, ts := range tasks {
		outputs[i] = IndexMergeOutputPath(tenant, window, c.cfg.PlanVersion, i, taskSectionIDs(ts.Runs))
	}

	g, gctx := errgroup.WithContext(ctx)
	if c.cfg.MaxRunningCompactionTasks > 0 {
		g.SetLimit(c.cfg.MaxRunningCompactionTasks)
	}
	for i, ts := range tasks {
		g.Go(func() error {
			plan := buildIndexMergePlan(tenant, window, ts, outputs[i], c.cfg.IndexMergeTaskTTL)
			opts := workflow.Options{
				Tenant: tenant,
				Actor:  []string{"compaction", "phase1"},
			}
			return c.runPlan(gctx, opts, plan)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("phase1: %w", err)
	}

	// Replace index pointers with new indexes
	oldPaths := make([]string, len(entries))
	for i, e := range entries {
		oldPaths[i] = e.Path
	}
	newEntries := makeTocEntries(tasks, outputs)

	phase2Ctx, cancel := context.WithTimeout(ctx, c.cfg.ToCConsolidateTimeout)
	defer cancel()
	swapped, err := c.metastoreWriter.ReplaceIndexPointers(phase2Ctx, window, tenant, oldPaths, newEntries)
	if err != nil {
		if errors.Is(err, metastore.ErrReplaceRetriesExhausted) {
			c.metrics.incRetryExhausted(tenant)
		}
		return nil, fmt.Errorf("phase2 ReplaceIndexPointers: %w", err)
	}
	if !swapped {
		// Race-loss or already-converged: treated as success. Keep the
		// pre-cycle view since we didn't actually swap.
		level.Debug(c.logger).Log("msg", "phase2 race-loss / already-converged",
			"tenant", tenant, "window", window)
		return entries, nil
	}

	// Construct post-swap entries for the caller.
	postEntries := make([]indexEntry, len(newEntries))
	for i, e := range newEntries {
		postEntries[i] = indexEntry{Path: e.Path, Start: e.StartTime, End: e.EndTime}
	}
	level.Info(c.logger).Log("msg", "tenant cycle complete",
		"tenant", tenant, "window", window,
		"phase1_tasks", len(tasks),
		"removed_indexes", len(oldPaths),
		"added_indexes", len(outputs),
	)
	return postEntries, nil
}

// taskSectionIDs returns canonical "<ObjectPath>#<SectionIndex>" IDs for
// every section across all Runs in a task. Used as input to
// IndexMergeOutputPath. The output is unsorted; IndexMergeOutputPath sorts
// internally so order here doesn't affect the resulting path.
func taskSectionIDs(runs []*compactionv2pb.RunRef) []string {
	var ids []string
	for _, r := range runs {
		for _, s := range r.Sections {
			ids = append(ids, fmt.Sprintf("%s#%d", s.ObjectPath, s.SectionIndex))
		}
	}
	return ids
}

// makeTocEntries pairs each output path with the time bounds derived
// from its task's source SectionRefs. tasks[i] is the TaskSpec that produced
// outputs[i].
//
// Time bounds: min(MinTimestamp) / max(MaxTimestamp) across all SectionRefs
// in the task. We derive bounds coordinator-side instead of reading them
// back from the produced index because IndexMerge cannot widen the source
// timestamp range — the union of source bounds is necessarily tight.
func makeTocEntries(
	tasks []*compactionv2pb.TaskSpec,
	outputs []string,
) []metastore.TableOfContentsEntry {
	entries := make([]metastore.TableOfContentsEntry, len(outputs))
	for i, ts := range tasks {
		minTS, maxTS := int64(0), int64(0)
		first := true
		for _, run := range ts.Runs {
			for _, sec := range run.Sections {
				if first {
					minTS = sec.MinTimestamp
					maxTS = sec.MaxTimestamp
					first = false
					continue
				}
				if sec.MinTimestamp < minTS {
					minTS = sec.MinTimestamp
				}
				if sec.MaxTimestamp > maxTS {
					maxTS = sec.MaxTimestamp
				}
			}
		}
		entries[i] = metastore.TableOfContentsEntry{
			Path:      outputs[i],
			StartTime: time.Unix(0, minTS).UTC(),
			EndTime:   time.Unix(0, maxTS).UTC(),
		}
	}
	return entries
}
