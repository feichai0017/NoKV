package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

type compaction struct {
	owner     *levelManager
	policy    *SchedulerPolicy
	triggerCh chan struct{}
	maxRuns   int
	logger    *slog.Logger
}

func newCompaction(owner *levelManager, maxRuns int, mode string, logger *slog.Logger) *compaction {
	if maxRuns <= 0 {
		maxRuns = 1
	} else if maxRuns > 4 {
		maxRuns = 4
	}
	if logger == nil {
		logger = slog.Default()
	}
	cr := &compaction{
		owner:     owner,
		policy:    NewSchedulerPolicy(mode),
		triggerCh: make(chan struct{}, 16),
		maxRuns:   maxRuns,
		logger:    logger,
	}
	cr.Trigger()
	return cr
}

func (cr *compaction) Trigger() {
	select {
	case cr.triggerCh <- struct{}{}:
	default:
	}
}

func (cr *compaction) Start(id int, closeCh <-chan struct{}, done func()) {
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

func (cr *compaction) runCycle(id int) {
	ranAny := false
	for range cr.maxRuns {
		if id == 0 {
			cr.owner.adjustThrottle()
		}
		if !cr.runOnce(id) {
			break
		}
		ranAny = true
		if id == 0 {
			cr.owner.adjustThrottle()
		}
		if !cr.owner.needsCompaction() {
			break
		}
	}
	if ranAny && cr.owner.needsCompaction() {
		cr.Trigger()
	}
}

func (cr *compaction) runOnce(id int) bool {
	prios := cr.owner.pickCompactLevels()
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

func (cr *compaction) RunOnce(id int) bool {
	return cr.runOnce(id)
}

func (cr *compaction) run(id int, p Priority) bool {
	start := time.Now()
	err := cr.owner.doCompact(id, p)
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

func (lsm *LSM) newCompactStatus() *State {
	return NewState(lsm.option.MaxLevelNum)
}

// adjustThrottle updates write admission state using a two-stage model:
// slowdown (pace writes) and stop (block writes). Hysteresis is applied to
// avoid oscillation under heavy compaction pressure.
func (lm *levelManager) adjustThrottle() {
	if lm == nil || lm.lsm == nil || len(lm.levels) == 0 {
		return
	}
	l0Tables := lm.levels[0].numTables()
	_, maxScore := lm.compactionStats()

	l0Slow := lm.opt.L0SlowdownWritesTrigger
	l0Stop := lm.opt.L0StopWritesTrigger
	l0Resume := lm.opt.L0ResumeWritesTrigger

	scoreSlow := lm.opt.CompactionSlowdownTrigger
	scoreStop := lm.opt.CompactionStopTrigger
	scoreResume := lm.opt.CompactionResumeTrigger

	stopCond := l0Tables >= l0Stop
	slowCond := l0Tables >= l0Slow || maxScore >= scoreSlow
	resumeCond := l0Tables <= l0Resume && maxScore <= scoreResume

	cur := lm.lsm.ThrottleState()
	target := cur
	switch cur {
	case WriteThrottleStop:
		if stopCond {
			target = WriteThrottleStop
		} else if slowCond {
			target = WriteThrottleSlowdown
		} else if resumeCond {
			target = WriteThrottleNone
		}
	case WriteThrottleSlowdown:
		if stopCond {
			target = WriteThrottleStop
		} else if resumeCond {
			target = WriteThrottleNone
		}
	default:
		if stopCond {
			target = WriteThrottleStop
		} else if slowCond {
			target = WriteThrottleSlowdown
		} else {
			target = WriteThrottleNone
		}
	}
	l0Pressure := normalizedThrottlePressure(float64(l0Tables), float64(l0Slow), float64(l0Stop))
	scorePressure := normalizedThrottlePressure(maxScore, scoreSlow, scoreStop)
	pressure := max(l0Pressure, scorePressure)
	switch target {
	case WriteThrottleNone:
		pressure = 0
	case WriteThrottleStop:
		pressure = 1000
	case WriteThrottleSlowdown:
		if pressure == 0 {
			pressure = 1
		}
	}
	rate := uint64(0)
	if target == WriteThrottleSlowdown {
		rate = throttleRateForPressure(
			uint32(pressure),
			lm.opt.WriteThrottleMinRate,
			lm.opt.WriteThrottleMaxRate,
		)
	}
	lm.lsm.throttleWrites(target, uint32(pressure), rate)
}

func normalizedThrottlePressure(value, slowdown, stop float64) int {
	if stop <= slowdown {
		if value >= stop {
			return 1000
		}
		return 0
	}
	if value <= slowdown {
		return 0
	}
	if value >= stop {
		return 1000
	}
	ratio := (value - slowdown) / (stop - slowdown)
	if ratio <= 0 {
		return 0
	}
	if ratio >= 1 {
		return 1000
	}
	return int(ratio*1000 + 0.5)
}

func throttleRateForPressure(pressure uint32, minRate, maxRate int64) uint64 {
	if pressure == 0 || maxRate <= 0 {
		return 0
	}
	if minRate <= 0 {
		minRate = maxRate
	}
	if maxRate < minRate {
		maxRate = minRate
	}
	ratio := float64(pressure) / 1000
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	curve := ratio * ratio
	rate := float64(maxRate) - (float64(maxRate-minRate) * curve)
	if rate < float64(minRate) {
		rate = float64(minRate)
	}
	return uint64(rate + 0.5)
}

const (
	// PolicyLeveled keeps the legacy leveled-style execution ordering.
	PolicyLeveled = "leveled"
	// PolicyTiered prioritizes staging-buffer convergence before regular compaction.
	PolicyTiered = "tiered"
	// PolicyHybrid adapts between leveled and tiered ordering by staging pressure.
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
	l0      []Priority
	keep    []Priority
	drain   []Priority
	regular []Priority
}

// FeedbackEvent captures one compaction execution outcome.
//
// The scheduler policy consumes this signal to tune scheduling decisions in
// subsequent rounds without modifying picker behavior.
type FeedbackEvent struct {
	WorkerID int
	Priority Priority
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
	stagingBias atomic.Int32
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
func (p *SchedulerPolicy) Arrange(workerID int, priorities []Priority) []Priority {
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

// Observe staging runtime execution feedback and updates scheduler bias when
// the configured mode uses staging-aware ordering.
func (p *SchedulerPolicy) Observe(event FeedbackEvent) {
	if p == nil {
		return
	}
	switch p.mode {
	case PolicyTiered, PolicyHybrid:
		p.observeTiered(event)
	}
}

// arrangeLeveled preserves legacy behavior.
//
// Design:
// - Worker 0 keeps L0 relief first to reduce write stalls quickly.
// - Other workers keep picker order untouched.
func arrangeLeveled(workerID int, priorities []Priority) []Priority {
	if workerID == 0 {
		return MoveL0ToFront(priorities)
	}
	return priorities
}

// arrangeTiered prioritizes staging convergence with guarded fairness.
//
// Design:
//   - Split priorities into four queues: L0 relief, staging-keep, staging-drain,
//     and regular.
//   - Interleave queues by pressure-aware quotas instead of draining one queue
//     completely, to avoid starvation.
//   - Worker 0 reserves one hard L0 slot under critical backlog.
func (p *SchedulerPolicy) arrangeTiered(workerID int, priorities []Priority) []Priority {
	if len(priorities) <= 1 {
		return priorities
	}
	ordered := append([]Priority(nil), priorities...)
	if !hasStagingWork(ordered) {
		return arrangeLeveled(workerID, ordered)
	}
	queues := classifyQueues(ordered)
	stagingScore := maxScore(queues.keep, queues.drain)
	quota := p.effectiveQuota(stagingScore)
	return arrangeByQueues(workerID, queues, quota)
}

// observeTiered observes runtime execution feedback and updates staging-aware
// scheduling bias.
func (p *SchedulerPolicy) observeTiered(event FeedbackEvent) {
	if p == nil {
		return
	}
	if !event.Priority.StagingMode.UsesStaging() {
		// Non-staging success gradually decays stale staging bias.
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
// - Low staging pressure: keep leveled behavior for stable mixed workloads.
// - High staging pressure: switch to tiered queue scheduling.
func (p *SchedulerPolicy) arrangeHybrid(workerID int, priorities []Priority) []Priority {
	if len(priorities) <= 1 {
		return priorities
	}
	ordered := append([]Priority(nil), priorities...)
	if !hasStagingWork(ordered) {
		return arrangeLeveled(workerID, ordered)
	}
	queues := classifyQueues(ordered)
	stagingScore := maxScore(queues.keep, queues.drain)
	if stagingScore < hybridTieredThreshold {
		return arrangeLeveled(workerID, ordered)
	}
	quota := p.effectiveQuota(stagingScore)
	return arrangeByQueues(workerID, queues, quota)
}

func hasStagingWork(priorities []Priority) bool {
	return slices.ContainsFunc(priorities, func(p Priority) bool {
		return p.StagingMode.UsesStaging()
	})
}

func classifyQueues(priorities []Priority) priorityQueues {
	var q priorityQueues
	for _, p := range priorities {
		switch {
		case p.Level == 0 && p.Adjusted >= l0ReliefScoreMin:
			q.l0 = append(q.l0, p)
		case p.StagingMode == StagingKeep:
			q.keep = append(q.keep, p)
		case p.StagingMode == StagingDrain:
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

func tieredQuotaByPressure(stagingScore float64) queueQuota {
	switch {
	case stagingScore >= 6:
		// Severe staging backlog: aggressively drain/merge staging first.
		return queueQuota{l0: 2, keep: 3, drain: 3, regular: 1}
	case stagingScore >= 3:
		// Balanced mode: keep staging and regular making progress together.
		return queueQuota{l0: 2, keep: 2, drain: 2, regular: 2}
	default:
		// Mild staging pressure: preserve regular throughput while still servicing staging.
		return queueQuota{l0: 2, keep: 1, drain: 1, regular: 3}
	}
}

func (p *SchedulerPolicy) effectiveQuota(stagingScore float64) queueQuota {
	quota := tieredQuotaByPressure(stagingScore)
	return applyStagingBias(quota, int(p.stagingBias.Load()))
}

func applyStagingBias(quota queueQuota, bias int) queueQuota {
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
		old := p.stagingBias.Load()
		next := clampI32(old+delta, -2, 2)
		if p.stagingBias.CompareAndSwap(old, next) {
			return
		}
	}
}

func (p *SchedulerPolicy) decayBiasTowardsZero() {
	for {
		old := p.stagingBias.Load()
		var next int32
		switch {
		case old > 0:
			next = old - 1
		case old < 0:
			next = old + 1
		default:
			return
		}
		if p.stagingBias.CompareAndSwap(old, next) {
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

func maxScore(slices ...[]Priority) float64 {
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

func arrangeByQueues(workerID int, q priorityQueues, quota queueQuota) []Priority {
	total := len(q.l0) + len(q.keep) + len(q.drain) + len(q.regular)
	if total == 0 {
		return nil
	}
	out := make([]Priority, 0, total)

	// Worker 0 reserves one critical L0 slot to quickly relieve write pressure.
	if workerID == 0 && len(q.l0) > 0 && q.l0[0].Adjusted >= l0CriticalScore {
		out = append(out, q.l0[0])
		q.l0 = q.l0[1:]
	}

	emit := func(queue *[]Priority, n int) {
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

func sortByAdjustedDesc(priorities []Priority) {
	sort.SliceStable(priorities, func(i, j int) bool {
		if priorities[i].Adjusted == priorities[j].Adjusted {
			return priorities[i].Score > priorities[j].Score
		}
		return priorities[i].Adjusted > priorities[j].Adjusted
	})
}

// KeyRange describes a compaction key span.
type KeyRange struct {
	Left  []byte
	Right []byte
	Inf   bool
}

// InfRange matches all keys.
var InfRange = KeyRange{Inf: true}

func (r KeyRange) IsEmpty() bool {
	return len(r.Left) == 0 && len(r.Right) == 0 && !r.Inf
}

func (r KeyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.Left, r.Right, r.Inf)
}

func (r KeyRange) Equals(dst KeyRange) bool {
	return bytes.Equal(r.Left, dst.Left) &&
		bytes.Equal(r.Right, dst.Right) &&
		r.Inf == dst.Inf
}

func (r *KeyRange) Extend(kr KeyRange) {
	if kr.IsEmpty() {
		return
	}
	if r.IsEmpty() {
		*r = kr
	}
	if len(r.Left) == 0 || kv.CompareInternalKeys(kr.Left, r.Left) < 0 {
		r.Left = kr.Left
	}
	if len(r.Right) == 0 || kv.CompareInternalKeys(kr.Right, r.Right) > 0 {
		r.Right = kr.Right
	}
	if kr.Inf {
		r.Inf = true
	}
}

func (r KeyRange) OverlapsWith(dst KeyRange) bool {
	// Empty keyRange always overlaps.
	if r.IsEmpty() {
		return true
	}
	// Empty dst doesn't overlap with anything.
	if dst.IsEmpty() {
		return false
	}
	if r.Inf || dst.Inf {
		return true
	}

	// [dst.left, dst.right] ... [r.left, r.right]
	if kv.CompareInternalKeys(r.Left, dst.Right) > 0 {
		return false
	}
	// [r.left, r.right] ... [dst.left, dst.right]
	if kv.CompareInternalKeys(r.Right, dst.Left) < 0 {
		return false
	}
	return true
}

// StateEntry captures the metadata tracked during compaction scheduling.
//
// IntraLevel marks the entry as a within-level compaction (e.g. L0→L0) that
// claims its input by table ID only and does NOT register a key range with
// the level. This lets multiple workers run concurrent intra-level
// compactions on disjoint table sets even when the picked SSTs have
// overlapping keyranges (the common case for L0 random writes). Inter-level
// compactions (L0→Lbase, Ln→Ln+1) keep the original range-based locking.
type StateEntry struct {
	ThisLevel  int
	NextLevel  int
	ThisRange  KeyRange
	NextRange  KeyRange
	ThisSize   int64
	TableIDs   []uint64
	IntraLevel bool
}

// LevelsLocked is a marker to indicate level locks are held by the caller.
type LevelsLocked struct{}

// State tracks compaction ranges and in-flight table IDs.
type State struct {
	sync.RWMutex
	levels []*levelState
	tables map[uint64]struct{}
}

type levelState struct {
	ranges  []KeyRange
	delSize int64
}

// NewState allocates a compaction state tracker.
func NewState(maxLevels int) *State {
	cs := &State{
		levels: make([]*levelState, 0, maxLevels),
		tables: make(map[uint64]struct{}),
	}
	for range maxLevels {
		cs.levels = append(cs.levels, &levelState{})
	}
	return cs
}

// Overlaps reports whether the range overlaps with an in-flight compaction.
func (cs *State) Overlaps(level int, kr KeyRange) bool {
	cs.RLock()
	defer cs.RUnlock()
	if level < 0 || level >= len(cs.levels) {
		return false
	}
	return cs.levels[level].overlapsWith(kr)
}

// DelSize returns the accumulated compaction size for a level.
func (cs *State) DelSize(level int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	if level < 0 || level >= len(cs.levels) {
		return 0
	}
	return cs.levels[level].delSize
}

// HasRanges returns true if any level currently tracks a compaction range.
func (cs *State) HasRanges() bool {
	cs.RLock()
	defer cs.RUnlock()
	for _, lvl := range cs.levels {
		if len(lvl.ranges) != 0 {
			return true
		}
	}
	return false
}

// HasTable reports whether a table fid is already being compacted.
func (cs *State) HasTable(fid uint64) bool {
	cs.RLock()
	defer cs.RUnlock()
	_, ok := cs.tables[fid]
	return ok
}

// AddRangeWithTables records a range and table IDs under compaction.
func (cs *State) AddRangeWithTables(level int, kr KeyRange, tableIDs []uint64) {
	cs.Lock()
	defer cs.Unlock()
	if level < 0 || level >= len(cs.levels) {
		return
	}
	cs.levels[level].ranges = append(cs.levels[level].ranges, kr)
	for _, fid := range tableIDs {
		cs.tables[fid] = struct{}{}
	}
}

// Delete clears state for a completed compaction.
//
// IntraLevel entries (L0→L0) only registered table IDs, so Delete only
// undoes the table claims and skips the range bookkeeping.
func (cs *State) Delete(entry StateEntry) error {
	cs.Lock()
	defer cs.Unlock()

	if entry.ThisLevel < 0 || entry.ThisLevel >= len(cs.levels) {
		return nil
	}
	if entry.NextLevel < 0 || entry.NextLevel >= len(cs.levels) {
		return nil
	}

	thisLevel := cs.levels[entry.ThisLevel]
	nextLevel := cs.levels[entry.NextLevel]

	if !entry.IntraLevel {
		// Validate all affected ranges first so Delete remains atomic on error.
		found := thisLevel.contains(entry.ThisRange)
		if entry.ThisLevel != entry.NextLevel && !entry.NextRange.IsEmpty() {
			found = nextLevel.contains(entry.NextRange) && found
		}
		if !found {
			return fmt.Errorf(
				"compact state delete: keyRange not found; this=%s thisLevel=%d thisState=%s next=%s nextLevel=%d nextState=%s",
				entry.ThisRange,
				entry.ThisLevel,
				thisLevel.debug(),
				entry.NextRange,
				entry.NextLevel,
				nextLevel.debug(),
			)
		}
		_ = thisLevel.remove(entry.ThisRange)
		if entry.ThisLevel != entry.NextLevel && !entry.NextRange.IsEmpty() {
			_ = nextLevel.remove(entry.NextRange)
		}
	}

	thisLevel.delSize -= entry.ThisSize

	for _, fid := range entry.TableIDs {
		_, ok := cs.tables[fid]
		utils.CondPanicFunc(!ok, func() error {
			return fmt.Errorf("cs.tables is nil")
		})
		delete(cs.tables, fid)
	}
	return nil
}

// CompareAndAdd reserves ranges and table IDs if they do not overlap.
//
// IntraLevel entries (L0→L0) skip range overlap checks entirely and only
// claim by table ID — multiple concurrent within-level compactions are
// safe as long as their table sets are disjoint (which the picker
// guarantees via state.HasTable).
func (cs *State) CompareAndAdd(_ LevelsLocked, entry StateEntry) bool {
	cs.Lock()
	defer cs.Unlock()

	if entry.ThisLevel < 0 || entry.ThisLevel >= len(cs.levels) {
		return false
	}
	if entry.NextLevel < 0 || entry.NextLevel >= len(cs.levels) {
		return false
	}

	thisLevel := cs.levels[entry.ThisLevel]
	nextLevel := cs.levels[entry.NextLevel]

	if !entry.IntraLevel {
		if thisLevel.overlapsWith(entry.ThisRange) {
			return false
		}
		if nextLevel.overlapsWith(entry.NextRange) {
			return false
		}
		thisLevel.ranges = append(thisLevel.ranges, entry.ThisRange)
		nextLevel.ranges = append(nextLevel.ranges, entry.NextRange)
	}
	// Intra-level entries do not register a range — the table-id set is the
	// only claim. Other workers checking state.HasTable will skip these
	// tables; range overlap from peer L0→Lbase is correctly NOT blocked.
	thisLevel.delSize += entry.ThisSize
	for _, fid := range entry.TableIDs {
		cs.tables[fid] = struct{}{}
	}
	return true
}

func (ls *levelState) overlapsWith(dst KeyRange) bool {
	for _, r := range ls.ranges {
		if r.OverlapsWith(dst) {
			return true
		}
	}
	return false
}

func (ls *levelState) remove(dst KeyRange) bool {
	final := ls.ranges[:0]
	var found bool
	for _, r := range ls.ranges {
		if !r.Equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	ls.ranges = final
	return found
}

func (ls *levelState) contains(dst KeyRange) bool {
	for _, r := range ls.ranges {
		if r.Equals(dst) {
			return true
		}
	}
	return false
}

func (ls *levelState) debug() string {
	var b bytes.Buffer
	for _, r := range ls.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}
