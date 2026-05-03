package architecture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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

type CombinedImportRule struct {
	Name     string
	Required []string
	Allowed  []string
}

type RemovedPathRule struct {
	Name string
	Path string
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
		Name:          "dbcore stays free of distributed assembly",
		PackagePrefix: modulePath + "/dbcore",
		Forbidden: []string{
			modulePath + "/fsmeta",
			modulePath + "/coordinator",
			modulePath + "/meta/root",
			modulePath + "/raftstore",
		},
	},
	{
		Name:          "dbcore stays free of global error taxonomy",
		PackagePrefix: modulePath + "/dbcore",
		Forbidden: []string{
			modulePath + "/errors",
		},
		Exempt: []string{
			modulePath + "/dbcore/errkind",
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
}

var combinedImportRules = []CombinedImportRule{
	{
		Name: "only raftstore fsmeta runtime combines fsmeta exec with coordinator and raftstore clients",
		Required: []string{
			modulePath + "/fsmeta/exec",
			modulePath + "/coordinator/client",
			modulePath + "/raftstore/client",
		},
		Allowed: []string{
			modulePath + "/fsmeta/runtime/raftstore",
		},
	},
}

var removedPathRules = []RemovedPathRule{
	{Name: "raftstore descriptor package stays removed", Path: "raftstore/descriptor"},
	{Name: "coordinator eunomia package stays removed", Path: "coordinator/protocol/eunomia"},
	{Name: "db runtime package stays moved to dbcore", Path: "runtime"},
	{Name: "raftstore mode package stays moved to dbcore/mode", Path: "raftstore/mode"},
	{Name: "raftstore migrate mode alias stays removed", Path: "raftstore/migrate/mode.go"},
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

func CheckCombinedImportRules(packages []GoPackage) []Violation {
	var violations []Violation
	for _, rule := range combinedImportRules {
		for _, pkg := range packages {
			if rule.isAllowed(pkg.ImportPath) || !importsAll(pkg.Imports, rule.Required) {
				continue
			}
			violations = append(violations, Violation{
				Rule:    rule.Name,
				Package: pkg.ImportPath,
				Import:  strings.Join(rule.Required, ", "),
			})
		}
	}
	return violations
}

func CheckRemovedPathRules(root string) []Violation {
	var violations []Violation
	for _, rule := range removedPathRules {
		path := filepath.Join(root, rule.Path)
		if _, err := os.Stat(path); err == nil {
			violations = append(violations, Violation{
				Rule:    rule.Name,
				Package: rule.Path,
			})
		}
	}
	return violations
}

func importsAll(imports, required []string) bool {
	for _, req := range required {
		found := false
		for _, imp := range imports {
			if pathMatches(imp, req) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
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

func (r CombinedImportRule) isAllowed(path string) bool {
	for _, allowed := range r.Allowed {
		if pathMatches(path, allowed) {
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
