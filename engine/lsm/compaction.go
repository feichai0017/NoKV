package lsm

import (
	"errors"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/feichai0017/NoKV/engine/lsm/table"
)

// compactDef carries one in-flight compaction's executable state. It binds a
// pure plan.Plan (from the plan subpackage) to live tables and level handlers
// for the duration of one compaction run.
//
// Compaction flow: the picker chooses a Priority, the planner produces a
// plan.Plan from snapshot table metadata, then the executor binds that Plan
// into a compactDef so it can resolve table IDs back to live *table.Table handles
// and drive the merge.
type compactDef struct {
	compactorId int
	spec        plan.Plan
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table.Table
	bot []*table.Table

	splits []plan.KeyRange

	thisSize int64

	adjusted float64
}

// targetFileSize returns the per-file size budget configured for the compaction
// destination. SSTable builders use this to know when to flush the current
// builder and start a new file.
func (cd *compactDef) targetFileSize() int64 {
	return cd.fileSize(cd.spec.ThisLevel)
}

// fileSize returns the per-file size budget for the requested level. Returns
// zero when level is neither this nor next, so callers can detect mis-routed
// queries explicitly.
func (cd *compactDef) fileSize(level int) int64 {
	switch level {
	case cd.spec.ThisLevel:
		return cd.spec.ThisFileSize
	case cd.spec.NextLevel:
		return cd.spec.NextFileSize
	default:
		return 0
	}
}

// stateEntry derives the conflict-state entry registered with plan.State so
// concurrent compactors can detect overlap by table id and key range.
func (cd *compactDef) stateEntry() plan.StateEntry {
	return cd.spec.StateEntry(cd.thisSize)
}

// setNextLevel binds the destination level handler for the compaction and
// updates the embedded plan.Plan with the level's per-file size target.
// Passing a nil next leaves the embedded plan untouched.
func (cd *compactDef) setNextLevel(t plan.Targets, next *levelHandler) {
	cd.nextLevel = next
	if next == nil {
		return
	}
	cd.spec.NextLevel = next.levelNum
	cd.spec.NextFileSize = t.FileSizeForLevel(next.levelNum)
}

// applyPlan replaces the embedded plan with p while preserving the
// builder-relevant fields (file sizes, landing mode, drop prefixes, stats
// tag) that the executor already populated.
func (cd *compactDef) applyPlan(p plan.Plan) {
	p.ThisFileSize = cd.spec.ThisFileSize
	p.NextFileSize = cd.spec.NextFileSize
	p.LandingMode = cd.spec.LandingMode
	p.DropPrefixes = cd.spec.DropPrefixes
	p.StatsTag = cd.spec.StatsTag
	cd.spec = p
}

// lockLevels takes read locks on the source and (when distinct) destination
// level handlers. It is intentionally separate from the constructor so the
// caller can choose whether to lock; e.g. L0→L0 takes one shared lock to
// avoid double-RLock deadlocks on the same handler.
func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	if cd.nextLevel != cd.thisLevel {
		cd.nextLevel.RLock()
	}
}

// unlockLevels releases the locks taken by lockLevels in reverse order.
func (cd *compactDef) unlockLevels() {
	if cd.nextLevel != cd.thisLevel {
		cd.nextLevel.RUnlock()
	}
	cd.thisLevel.RUnlock()
}

type scheduler struct {
	compactor *compactor
	policy    *SchedulerPolicy
	triggerCh chan struct{}
	maxRuns   int
	logger    *slog.Logger
}

func newScheduler(c *compactor, maxRuns int, mode string, logger *slog.Logger) *scheduler {
	if maxRuns <= 0 {
		maxRuns = 1
	} else if maxRuns > 4 {
		maxRuns = 4
	}
	if logger == nil {
		logger = slog.Default()
	}
	cr := &scheduler{
		compactor: c,
		policy:    NewSchedulerPolicy(mode),
		triggerCh: make(chan struct{}, 16),
		maxRuns:   maxRuns,
		logger:    logger,
	}
	cr.Trigger()
	return cr
}

func (cr *scheduler) Trigger() {
	select {
	case cr.triggerCh <- struct{}{}:
	default:
	}
}

func (cr *scheduler) Start(id int, closeCh <-chan struct{}, done func()) {
	if done != nil {
		defer done()
	}
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(500)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-closeCh:
		randomDelay.Stop()
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-closeCh:
			return
		case <-cr.triggerCh:
			cr.runCycle(id)
		case <-ticker.C:
			cr.runCycle(id)
		}
	}
}

func (cr *scheduler) runCycle(id int) {
	ranAny := false
	for range cr.maxRuns {
		if id == 0 {
			cr.compactor.adjustThrottle()
		}
		if !cr.runOnce(id) {
			break
		}
		ranAny = true
		if id == 0 {
			cr.compactor.adjustThrottle()
		}
		if !cr.compactor.needsCompaction() {
			break
		}
	}
	if ranAny && cr.compactor.needsCompaction() {
		cr.Trigger()
	}
}

func (cr *scheduler) runOnce(id int) bool {
	prios := cr.compactor.pickCompactLevels()
	prios = cr.policy.Arrange(id, prios)
	for _, p := range prios {
		if id == 0 && p.Level == 0 {
			// keep scanning level zero first for worker #0
		} else if p.Adjusted < 1.0 {
			break
		}
		if cr.run(id, p) {
			return true
		}
	}
	return false
}

func (cr *scheduler) RunOnce(id int) bool {
	return cr.runOnce(id)
}

func (cr *scheduler) run(id int, p plan.Priority) bool {
	start := time.Now()
	err := cr.compactor.doCompact(id, p)
	cr.policy.Observe(FeedbackEvent{
		WorkerID: id,
		Priority: p,
		Err:      err,
		Duration: time.Since(start),
	})
	switch err {
	case nil:
		return true
	case ErrFillTables:
	default:
		cr.logger.Error("doCompact failed", "worker", id, "level", p.Level, "score", p.Score, "adjusted", p.Adjusted, "err", err)
	}
	return false
}

func (lsm *LSM) newCompactStatus() *plan.State {
	return plan.NewState(lsm.option.MaxLevelNum)
}
const (
	// PolicyLeveled keeps the default leveled-style execution ordering.
	PolicyLeveled = "leveled"
	// PolicyTiered prioritizes landing-buffer convergence before regular compaction.
	PolicyTiered = "tiered"
	// PolicyHybrid adapts between leveled and tiered ordering by landing pressure.
	PolicyHybrid = "hybrid"
)

const (
	// l0ReliefScoreMin marks an L0 task as backlog-relief work.
	l0ReliefScoreMin = 1.0
	// l0CriticalScore triggers a hard "L0 first" slot for worker 0.
	l0CriticalScore = 2.0
	// hybridTieredThreshold controls when hybrid switches to tiered behavior.
	hybridTieredThreshold = 3.0
)

// queueQuota controls how many tasks from each queue are emitted per round.
type queueQuota struct {
	l0      int
	keep    int
	drain   int
	regular int
}

type priorityQueues struct {
	l0      []plan.Priority
	keep    []plan.Priority
	drain   []plan.Priority
	regular []plan.Priority
}

// FeedbackEvent captures one compaction execution outcome.
//
// The scheduler policy consumes this signal to tune scheduling decisions in
// subsequent rounds without modifying picker behavior.
type FeedbackEvent struct {
	WorkerID int
	Priority plan.Priority
	Err      error
	Duration time.Duration
}

// SchedulerPolicy reorders compaction priorities selected by the picker.
//
// It does not change candidate generation; it only changes execution order.
// The mode stays concrete and local: there is one policy object with one
// explicit behavior switch, not a plugin surface.
type SchedulerPolicy struct {
	mode        string
	landingBias atomic.Int32
}

// NewSchedulerPolicy constructs a compaction scheduler policy by name.
// Unknown names gracefully fall back to leveled behavior.
func NewSchedulerPolicy(name string) *SchedulerPolicy {
	return &SchedulerPolicy{mode: normalizePolicyMode(name)}
}

func normalizePolicyMode(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", PolicyLeveled:
		return PolicyLeveled
	case PolicyTiered:
		return PolicyTiered
	case PolicyHybrid:
		return PolicyHybrid
	default:
		return PolicyLeveled
	}
}

// Arrange reorders priorities according to the configured compaction mode.
func (p *SchedulerPolicy) Arrange(workerID int, priorities []plan.Priority) []plan.Priority {
	mode := PolicyLeveled
	if p != nil {
		mode = p.mode
	}
	switch mode {
	case PolicyTiered:
		return p.arrangeTiered(workerID, priorities)
	case PolicyHybrid:
		return p.arrangeHybrid(workerID, priorities)
	default:
		return arrangeLeveled(workerID, priorities)
	}
}

// Observe landing runtime execution feedback and updates scheduler bias when
// the configured mode uses landing-aware ordering.
func (p *SchedulerPolicy) Observe(event FeedbackEvent) {
	if p == nil {
		return
	}
	switch p.mode {
	case PolicyTiered, PolicyHybrid:
		p.observeTiered(event)
	}
}

// arrangeLeveled applies the default leveled ordering.
//
// Design:
// - Worker 0 keeps L0 relief first to reduce write stalls quickly.
// - Other workers keep picker order untouched.
func arrangeLeveled(workerID int, priorities []plan.Priority) []plan.Priority {
	if workerID == 0 {
		return plan.MoveL0ToFront(priorities)
	}
	return priorities
}

// arrangeTiered prioritizes landing convergence with guarded fairness.
//
// Design:
//   - Split priorities into four queues: L0 relief, landing-keep, landing-drain,
//     and regular.
//   - Interleave queues by pressure-aware quotas instead of draining one queue
//     completely, to avoid starvation.
//   - Worker 0 reserves one hard L0 slot under critical backlog.
func (p *SchedulerPolicy) arrangeTiered(workerID int, priorities []plan.Priority) []plan.Priority {
	if len(priorities) <= 1 {
		return priorities
	}
	ordered := append([]plan.Priority(nil), priorities...)
	if !hasLandingWork(ordered) {
		return arrangeLeveled(workerID, ordered)
	}
	queues := classifyQueues(ordered)
	landingScore := maxScore(queues.keep, queues.drain)
	quota := p.effectiveQuota(landingScore)
	return arrangeByQueues(workerID, queues, quota)
}

// observeTiered observes runtime execution feedback and updates landing-aware
// scheduling bias.
func (p *SchedulerPolicy) observeTiered(event FeedbackEvent) {
	if p == nil {
		return
	}
	if !event.Priority.LandingMode.UsesLanding() {
		// Non-landing success gradually decays stale landing bias.
		if event.Err == nil {
			p.decayBiasTowardsZero()
		}
		return
	}
	if event.Err == nil {
		p.shiftBias(1)
		return
	}
	// ErrFillTables is a transient capacity miss; treat as neutral.
	if errors.Is(event.Err, ErrFillTables) {
		p.decayBiasTowardsZero()
		return
	}
	p.shiftBias(-1)
}

// arrangeHybrid adapts between leveled and tiered scheduling.
//
// Design:
// - Low landing pressure: keep leveled behavior for stable mixed workloads.
// - High landing pressure: switch to tiered queue scheduling.
func (p *SchedulerPolicy) arrangeHybrid(workerID int, priorities []plan.Priority) []plan.Priority {
	if len(priorities) <= 1 {
		return priorities
	}
	ordered := append([]plan.Priority(nil), priorities...)
	if !hasLandingWork(ordered) {
		return arrangeLeveled(workerID, ordered)
	}
	queues := classifyQueues(ordered)
	landingScore := maxScore(queues.keep, queues.drain)
	if landingScore < hybridTieredThreshold {
		return arrangeLeveled(workerID, ordered)
	}
	quota := p.effectiveQuota(landingScore)
	return arrangeByQueues(workerID, queues, quota)
}

func hasLandingWork(priorities []plan.Priority) bool {
	return slices.ContainsFunc(priorities, func(p plan.Priority) bool {
		return p.LandingMode.UsesLanding()
	})
}

func classifyQueues(priorities []plan.Priority) priorityQueues {
	var q priorityQueues
	for _, p := range priorities {
		switch {
		case p.Level == 0 && p.Adjusted >= l0ReliefScoreMin:
			q.l0 = append(q.l0, p)
		case p.LandingMode == plan.LandingKeep:
			q.keep = append(q.keep, p)
		case p.LandingMode == plan.LandingDrain:
			q.drain = append(q.drain, p)
		default:
			q.regular = append(q.regular, p)
		}
	}
	sortByAdjustedDesc(q.l0)
	sortByAdjustedDesc(q.keep)
	sortByAdjustedDesc(q.drain)
	sortByAdjustedDesc(q.regular)
	return q
}

func tieredQuotaByPressure(landingScore float64) queueQuota {
	switch {
	case landingScore >= 6:
		// Severe landing backlog: aggressively drain/merge landing first.
		return queueQuota{l0: 2, keep: 3, drain: 3, regular: 1}
	case landingScore >= 3:
		// Balanced mode: keep landing and regular making progress together.
		return queueQuota{l0: 2, keep: 2, drain: 2, regular: 2}
	default:
		// Mild landing pressure: preserve regular throughput while still servicing landing.
		return queueQuota{l0: 2, keep: 1, drain: 1, regular: 3}
	}
}

func (p *SchedulerPolicy) effectiveQuota(landingScore float64) queueQuota {
	quota := tieredQuotaByPressure(landingScore)
	return applyLandingBias(quota, int(p.landingBias.Load()))
}

func applyLandingBias(quota queueQuota, bias int) queueQuota {
	if bias == 0 {
		return quota
	}
	if bias > 0 {
		shift := min(bias, 2)
		quota.keep += shift
		quota.drain += shift
		quota.regular = max(1, quota.regular-shift)
		return quota
	}
	shift := min(-bias, 2)
	for range shift {
		if quota.keep > 1 {
			quota.keep--
			quota.regular++
		}
		if quota.drain > 1 {
			quota.drain--
			quota.regular++
		}
	}
	return quota
}

func (p *SchedulerPolicy) shiftBias(delta int32) {
	for {
		old := p.landingBias.Load()
		next := clampI32(old+delta, -2, 2)
		if p.landingBias.CompareAndSwap(old, next) {
			return
		}
	}
}

func (p *SchedulerPolicy) decayBiasTowardsZero() {
	for {
		old := p.landingBias.Load()
		var next int32
		switch {
		case old > 0:
			next = old - 1
		case old < 0:
			next = old + 1
		default:
			return
		}
		if p.landingBias.CompareAndSwap(old, next) {
			return
		}
	}
}

func clampI32(v, lo, hi int32) int32 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func maxScore(slices ...[]plan.Priority) float64 {
	maxScore := 0.0
	for _, items := range slices {
		for _, p := range items {
			if p.Adjusted > maxScore {
				maxScore = p.Adjusted
			}
		}
	}
	return maxScore
}

func arrangeByQueues(workerID int, q priorityQueues, quota queueQuota) []plan.Priority {
	total := len(q.l0) + len(q.keep) + len(q.drain) + len(q.regular)
	if total == 0 {
		return nil
	}
	out := make([]plan.Priority, 0, total)

	// Worker 0 reserves one critical L0 slot to quickly relieve write pressure.
	if workerID == 0 && len(q.l0) > 0 && q.l0[0].Adjusted >= l0CriticalScore {
		out = append(out, q.l0[0])
		q.l0 = q.l0[1:]
	}

	emit := func(queue *[]plan.Priority, n int) {
		if n <= 0 {
			return
		}
		for range n {
			if len(*queue) == 0 {
				return
			}
			out = append(out, (*queue)[0])
			*queue = (*queue)[1:]
		}
	}

	for len(out) < total {
		before := len(out)
		emit(&q.l0, quota.l0)
		emit(&q.keep, quota.keep)
		emit(&q.drain, quota.drain)
		emit(&q.regular, quota.regular)
		if len(out) == before {
			// Fallback drain to avoid dead loops when some quotas are zero.
			out = append(out, q.l0...)
			out = append(out, q.keep...)
			out = append(out, q.drain...)
			out = append(out, q.regular...)
			break
		}
	}
	return out
}

func sortByAdjustedDesc(priorities []plan.Priority) {
	sort.SliceStable(priorities, func(i, j int) bool {
		if priorities[i].Adjusted == priorities[j].Adjusted {
			return priorities[i].Score > priorities[j].Score
		}
		return priorities[i].Adjusted > priorities[j].Adjusted
	})
}
