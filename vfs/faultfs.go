package vfs

import (
	"fmt"
	"os"
	"sync"
)

// Op identifies a filesystem operation for failure injection.
type Op string

const (
	OpOpen      Op = "open"
	OpOpenFile  Op = "open_file"
	OpMkdirAll  Op = "mkdir_all"
	OpRemove    Op = "remove"
	OpRename    Op = "rename"
	OpStat      Op = "stat"
	OpReadFile  Op = "read_file"
	OpWriteFile Op = "write_file"
	OpTruncate  Op = "truncate"
	OpGlob      Op = "glob"
	OpHostname  Op = "hostname"
)

// Hook is invoked before each filesystem operation.
// Returning a non-nil error simulates an operation failure.
type Hook func(op Op, path string) error

// FaultRule describes one fault-injection rule for FaultFS.
// A rule matches by operation and optional exact path, then starts injecting
// errors at TriggerAt (1-based) and keeps injecting until MaxFailures is
// reached. MaxFailures=0 means unlimited after TriggerAt.
type FaultRule struct {
	Op          Op
	Path        string
	Err         error
	TriggerAt   uint64
	MaxFailures uint64

	matched uint64
	failed  uint64
}

// FaultPolicy evaluates a list of fault-injection rules in order.
// The first rule that injects an error stops evaluation for that operation.
type FaultPolicy struct {
	mu    sync.Mutex
	rules []FaultRule
}

// NewFaultPolicy builds a policy from the provided rules.
func NewFaultPolicy(rules ...FaultRule) *FaultPolicy {
	p := &FaultPolicy{
		rules: make([]FaultRule, 0, len(rules)),
	}
	for _, rule := range rules {
		p.AddRule(rule)
	}
	return p
}

// AddRule appends a rule to the policy.
func (p *FaultPolicy) AddRule(rule FaultRule) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, normalizeFaultRule(rule))
}

// Hook evaluates policy rules and returns an injected error when matched.
// Hook satisfies the Hook function contract and can be passed to NewFaultFS.
func (p *FaultPolicy) Hook(op Op, path string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.rules {
		if err := p.rules[i].evaluate(op, path); err != nil {
			return err
		}
	}
	return nil
}

// FailOnceRule creates a rule that fails exactly once on the first match.
func FailOnceRule(op Op, path string, err error) FaultRule {
	return FaultRule{
		Op:          op,
		Path:        path,
		Err:         err,
		TriggerAt:   1,
		MaxFailures: 1,
	}
}

// FailOnNthRule creates a rule that fails exactly once on the Nth match.
// nth<=1 is treated as 1.
func FailOnNthRule(op Op, path string, nth uint64, err error) FaultRule {
	if nth == 0 {
		nth = 1
	}
	return FaultRule{
		Op:          op,
		Path:        path,
		Err:         err,
		TriggerAt:   nth,
		MaxFailures: 1,
	}
}

// FailAfterNthRule creates a rule that starts failing from the Nth match and
// keeps failing on every subsequent match. nth<=1 is treated as 1.
func FailAfterNthRule(op Op, path string, nth uint64, err error) FaultRule {
	if nth == 0 {
		nth = 1
	}
	return FaultRule{
		Op:        op,
		Path:      path,
		Err:       err,
		TriggerAt: nth,
	}
}

// NewFaultFSWithPolicy returns a FaultFS configured from a rule policy.
func NewFaultFSWithPolicy(base FS, policy *FaultPolicy) *FaultFS {
	if policy == nil {
		return NewFaultFS(base, nil)
	}
	return NewFaultFS(base, policy.Hook)
}

// FaultFS decorates an FS and injects failures via Hook.
type FaultFS struct {
	base FS
	hook Hook
}

// NewFaultFS returns an FS wrapper that can inject operation failures.
func NewFaultFS(base FS, hook Hook) *FaultFS {
	return &FaultFS{
		base: Ensure(base),
		hook: hook,
	}
}

func (f *FaultFS) before(op Op, path string) error {
	if f == nil || f.hook == nil {
		return nil
	}
	return f.hook(op, path)
}

func normalizeFaultRule(rule FaultRule) FaultRule {
	if rule.TriggerAt == 0 {
		rule.TriggerAt = 1
	}
	return rule
}

func (r *FaultRule) evaluate(op Op, path string) error {
	if !r.matches(op, path) {
		return nil
	}
	r.matched++
	if r.matched < r.TriggerAt {
		return nil
	}
	if r.MaxFailures > 0 && r.failed >= r.MaxFailures {
		return nil
	}
	r.failed++
	if r.Err != nil {
		return r.Err
	}
	return fmt.Errorf("faultfs injected failure: op=%s path=%s", op, path)
}

func (r *FaultRule) matches(op Op, path string) bool {
	if r.Op != "" && r.Op != op {
		return false
	}
	if r.Path != "" && r.Path != path {
		return false
	}
	return true
}

// Open opens an existing file for reading.
func (f *FaultFS) Open(name string) (*os.File, error) {
	if err := f.before(OpOpen, name); err != nil {
		return nil, err
	}
	return f.base.Open(name)
}

// OpenFile opens or creates a file.
func (f *FaultFS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if err := f.before(OpOpenFile, name); err != nil {
		return nil, err
	}
	return f.base.OpenFile(name, flag, perm)
}

// MkdirAll creates a directory hierarchy.
func (f *FaultFS) MkdirAll(path string, perm os.FileMode) error {
	if err := f.before(OpMkdirAll, path); err != nil {
		return err
	}
	return f.base.MkdirAll(path, perm)
}

// Remove removes a file or empty directory.
func (f *FaultFS) Remove(name string) error {
	if err := f.before(OpRemove, name); err != nil {
		return err
	}
	return f.base.Remove(name)
}

// Rename renames (moves) a file or directory.
func (f *FaultFS) Rename(oldPath, newPath string) error {
	if err := f.before(OpRename, oldPath+"->"+newPath); err != nil {
		return err
	}
	return f.base.Rename(oldPath, newPath)
}

// Stat returns file metadata.
func (f *FaultFS) Stat(name string) (os.FileInfo, error) {
	if err := f.before(OpStat, name); err != nil {
		return nil, err
	}
	return f.base.Stat(name)
}

// ReadFile reads an entire file.
func (f *FaultFS) ReadFile(name string) ([]byte, error) {
	if err := f.before(OpReadFile, name); err != nil {
		return nil, err
	}
	return f.base.ReadFile(name)
}

// WriteFile writes an entire file.
func (f *FaultFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if err := f.before(OpWriteFile, name); err != nil {
		return err
	}
	return f.base.WriteFile(name, data, perm)
}

// Truncate resizes a file.
func (f *FaultFS) Truncate(name string, size int64) error {
	if err := f.before(OpTruncate, name); err != nil {
		return err
	}
	return f.base.Truncate(name, size)
}

// Glob expands filesystem patterns.
func (f *FaultFS) Glob(pattern string) ([]string, error) {
	if err := f.before(OpGlob, pattern); err != nil {
		return nil, err
	}
	return f.base.Glob(pattern)
}

// Hostname returns local hostname.
func (f *FaultFS) Hostname() (string, error) {
	if err := f.before(OpHostname, ""); err != nil {
		return "", err
	}
	return f.base.Hostname()
}
