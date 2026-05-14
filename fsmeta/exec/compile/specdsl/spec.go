package specdsl

// OpSpec is the handwritten semantic description consumed by peras-opgen.
// Each fsmeta operation carries its own spec so generated compiler code can
// evolve toward per-op specialization without hiding semantics behind helpers.
type OpSpec struct {
	Name              string
	FileName          string
	ProgramType       string
	ValuesType        string
	RequestType       string
	CompileName       string
	LoweringName      string
	Materialize       string
	OperationKind     string
	Durability        string
	PredicateCount    int
	EffectCount       int
	HasOptions        bool
	Emitter           string
	Materializer      string
	Effects           []EffectSpec
	Predicates        []PredicateSpec
	Guards            []GuardSpec
	OptionalGuards    []GuardSpec
	Authority         AuthoritySpec
	Eligibility       string
	SlowReason        string
	SlowFallbacks     []string
	DurabilityBarrier bool
	WatchAtSeal       bool
}

type AuthoritySpec struct {
	Parents         []string
	Inodes          []string
	Broad           bool
	AllowOpaqueKeys bool
}

type PredicateSpec struct {
	Name       string
	Kind       string
	Key        string
	Repeatable bool
}

type EffectSpec struct {
	Name        string
	Kind        string
	Key         string
	ValueName   string
	ValueSource string
}

type GuardSpec struct {
	Name      string
	Guard     string
	Condition string
}
