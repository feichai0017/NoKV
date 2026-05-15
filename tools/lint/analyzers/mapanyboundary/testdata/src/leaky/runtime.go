package leaky

// Build is not declared as a diagnostics boundary; the internal map[string]any
// usage is therefore a §9 violation.
func Build() string {
	payload := map[string]any{"k": 1} // want `code_contract §9: map\[string\]any.*`
	_ = payload
	return "x"
}

// Snapshot IS declared as a diagnostics boundary (return type map[string]any);
// the body usage is allowed because the function declares itself as the boundary.
func Snapshot() map[string]any {
	return map[string]any{"k": 1}
}

// Reporter is an interface whose method declares a diagnostics boundary;
// declaring map[string]any inside the method type is allowed.
type Reporter interface {
	Stats() map[string]any
}
