package architecture

import (
	"strings"
	"testing"
)

func TestDependencyBoundaries(t *testing.T) {
	root, err := ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	packages, err := LoadPackages(root)
	if err != nil {
		t.Fatal(err)
	}

	var violations []Violation
	violations = append(violations, CheckImportRules(packages)...)
	violations = append(violations, CheckCombinedImportRules(packages)...)
	violations = append(violations, CheckRemovedPathRules(root)...)
	if len(violations) == 0 {
		return
	}

	var b strings.Builder
	b.WriteString("dependency boundary violations:")
	for _, violation := range violations {
		b.WriteString("\n- ")
		b.WriteString(violation.Rule)
		b.WriteString(": ")
		b.WriteString(violation.Package)
		if violation.Import != "" {
			b.WriteString(" imports ")
			b.WriteString(violation.Import)
		}
	}
	t.Fatal(b.String())
}
