// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package architecture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

const modulePath = "github.com/feichai0017/NoKV"

type GoPackage struct {
	ImportPath string
	Imports    []string
}

type ImportRule struct {
	Name          string
	PackagePrefix string
	PackageExact  bool
	Forbidden     []string
	Exempt        []string
}

type Violation struct {
	Rule    string
	Package string
	Import  string
}

var importRules = []ImportRule{
	{
		Name:          "root package stays free of distributed assembly",
		PackagePrefix: modulePath,
		PackageExact:  true,
		Forbidden: []string{
			modulePath + "/fsmeta",
			modulePath + "/raftstore/localmeta",
			modulePath + "/raftstore/raftlog",
			modulePath + "/raftstore/snapshot",
			modulePath + "/raftstore/mvcc",
			modulePath + "/raftstore/mode",
		},
	},
	{
		Name:          "local db stays free of distributed assembly",
		PackagePrefix: modulePath + "/local",
		Forbidden: []string{
			modulePath + "/fsmeta",
			modulePath + "/coordinator",
			modulePath + "/meta/root",
			modulePath + "/raftstore",
			modulePath + "/txn/percolator",
		},
	},
	{
		Name:          "txn layer stays below distributed assembly",
		PackagePrefix: modulePath + "/txn",
		Forbidden: []string{
			modulePath + "/fsmeta",
			modulePath + "/coordinator",
			modulePath + "/meta/root",
			modulePath + "/raftstore",
		},
	},
	{
		Name:          "txn mvcc stays protocol-neutral",
		PackagePrefix: modulePath + "/txn/mvcc",
		Forbidden: []string{
			modulePath + "/txn/percolator",
			modulePath + "/txn/latch",
		},
	},
	{
		Name:          "txn storage stays protocol-neutral",
		PackagePrefix: modulePath + "/txn/storage",
		Forbidden: []string{
			modulePath + "/txn/percolator",
			modulePath + "/txn/latch",
			modulePath + "/txn/mvcc",
		},
	},
	{
		Name:          "txn latch stays protocol-neutral",
		PackagePrefix: modulePath + "/txn/latch",
		Forbidden: []string{
			modulePath + "/txn/percolator",
			modulePath + "/txn/mvcc",
			modulePath + "/txn/storage",
		},
	},
	{
		Name:          "local runtime stays free of global error taxonomy",
		PackagePrefix: modulePath + "/local",
		Forbidden: []string{
			modulePath + "/errors",
		},
		Exempt: []string{
			modulePath + "/local/errkind",
		},
	},
	{
		Name:          "embedded engine stays free of global error taxonomy",
		PackagePrefix: modulePath + "/engine",
		Forbidden: []string{
			modulePath + "/errors",
		},
	},
	{
		Name:          "utils stays free of global error taxonomy",
		PackagePrefix: modulePath + "/utils",
		Forbidden: []string{
			modulePath + "/errors",
		},
	},
	{
		Name:          "fsmeta executor stays runtime-neutral",
		PackagePrefix: modulePath + "/fsmeta/exec",
		Forbidden: []string{
			modulePath + "/coordinator",
			modulePath + "/raftstore",
			modulePath + "/meta/root",
		},
	},
	{
		Name:          "fsmeta watch router stays store-neutral",
		PackagePrefix: modulePath + "/fsmeta/exec/watch",
		Forbidden: []string{
			modulePath + "/raftstore/store",
		},
	},
	{
		Name:          "meta root does not depend on coordinator service layer",
		PackagePrefix: modulePath + "/meta/root",
		Forbidden: []string{
			modulePath + "/coordinator",
		},
	},
	{
		Name:          "coordinator stays free of raftstore execution packages",
		PackagePrefix: modulePath + "/coordinator",
		Forbidden: []string{
			modulePath + "/raftstore",
		},
	},
	{
		Name:          "coordinator scheduling stays policy-only",
		PackagePrefix: modulePath + "/coordinator/scheduling",
		Forbidden: []string{
			modulePath + "/coordinator/client",
			modulePath + "/coordinator/server",
			modulePath + "/coordinator/storecontrol",
		},
	},
	{
		Name:          "coordinator storecontrol stays out of scheduling and service",
		PackagePrefix: modulePath + "/coordinator/storecontrol",
		Forbidden: []string{
			modulePath + "/coordinator/scheduling",
			modulePath + "/coordinator/server",
		},
	},
}

func ModuleRoot() (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	out, err := cmd.Output()
	if err != nil {
		return "", commandError("go list module root", err)
	}
	root := strings.TrimSpace(string(out))
	if root == "" {
		return "", errors.New("empty module root")
	}
	return root, nil
}

func LoadPackages(root string) ([]GoPackage, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, commandError("go list packages", err)
	}
	dec := json.NewDecoder(bytes.NewReader(out))
	var packages []GoPackage
	for {
		var pkg GoPackage
		err := dec.Decode(&pkg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		packages = append(packages, pkg)
	}
	return packages, nil
}

func CheckImportRules(packages []GoPackage) []Violation {
	var violations []Violation
	for _, rule := range importRules {
		for _, pkg := range packages {
			if !rule.matchesPackage(pkg.ImportPath) || rule.isExempt(pkg.ImportPath) {
				continue
			}
			for _, imp := range pkg.Imports {
				for _, forbidden := range rule.Forbidden {
					if pathMatches(imp, forbidden) {
						violations = append(violations, Violation{
							Rule:    rule.Name,
							Package: pkg.ImportPath,
							Import:  imp,
						})
					}
				}
			}
		}
	}
	return violations
}

func (r ImportRule) matchesPackage(path string) bool {
	if r.PackageExact {
		return path == r.PackagePrefix
	}
	return pathMatches(path, r.PackagePrefix)
}

func (r ImportRule) isExempt(path string) bool {
	for _, exempt := range r.Exempt {
		if pathMatches(path, exempt) {
			return true
		}
	}
	return false
}

func pathMatches(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func commandError(action string, err error) error {
	var exit *exec.ExitError
	if errors.As(err, &exit) && len(exit.Stderr) > 0 {
		return fmt.Errorf("%s: %w: %s", action, err, strings.TrimSpace(string(exit.Stderr)))
	}
	return fmt.Errorf("%s: %w", action, err)
}
