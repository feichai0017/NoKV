package specdsl

// OpSpec is the handwritten semantic description consumed by peras-opgen.
// Each fsmeta operation carries its own spec so generated compiler code can
// evolve toward per-op specialization without hiding semantics behind helpers.
type OpSpec struct {
	Name           string
	FileName       string
	ProgramType    string
	ValuesType     string
	RequestType    string
	CompileName    string
	LoweringName   string
	Materialize    string
	OperationKind  string
	Durability     string
	PredicateCount int
	EffectCount    int
	HasOptions     bool
	Emitter        string
	Materializer   string
	Effects        []EffectSpec
	Predicates     []PredicateSpec
	ConflictNames  []string
}

type PredicateSpec struct {
	Name string
	Kind string
}

type EffectSpec struct {
	Name      string
	Kind      string
	ValueName string
}
