// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package specdsl

// OpSpec is the handwritten semantic description consumed by fsmeta-opgen.
// Each fsmeta operation carries its own spec so generated compiler code can
// evolve toward per-op specialization without hiding semantics behind helpers.
type OpSpec struct {
	Name              string
	FileName          string
	ProgramType       string
	RequestType       string
	CompileName       string
	PlanName          string
	OperationKind     string
	Durability        string
	PredicateCount    int
	EffectCount       int
	HasOptions        bool
	Emitter           string
	Effects           []EffectSpec
	Predicates        []PredicateSpec
	Guards            []GuardSpec
	OptionalGuards    []GuardSpec
	Authority         AuthoritySpec
	Eligibility       string
	SlowReason        string
	SlowFallbacks     []string
	RequestChecks     []string
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
}

type GuardSpec struct {
	Name      string
	Guard     string
	Condition string
}

type ReadSpec struct {
	Name          string
	OperationKind string
	KeyShape      string
	Authority     string
	Source        string
}
