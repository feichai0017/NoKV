package ablation

import "fmt"

// Preset names the paper-facing ablation variants. The protocol implementation
// still consumes Config directly, but presets keep benchmark/test call sites
// aligned with the artifact's published comparison points.
type Preset string

const (
	PresetFull                  Preset = "full"
	PresetNoSeal                Preset = "no_seal"
	PresetNoBudget              Preset = "no_budget"
	PresetClientBlind           Preset = "client_blind"
	PresetReplyBlindClientBlind Preset = "reply_blind_client_blind"
	PresetNoReattach            Preset = "no_reattach"
	PresetFailStopOnRootUnreach Preset = "fail_stop_on_root_unreach"
)

// Config captures the first-cut control-plane ablation switches used by the
// paper's fault and benchmark runners.
type Config struct {
	// DisableSeal removes rooted seal emission so the current holder never
	// publishes a predecessor closure point.
	DisableSeal bool
	// DisableBudget removes small rooted refill budgets by switching allocator
	// windows to a large local runway.
	DisableBudget bool
	// DisableClientVerify bypasses client-side witness verification and stale
	// generation rejection.
	DisableClientVerify bool
	// DisableReplyEvidence strips reply-side generation/budget/frontier evidence
	// from detached control-plane answers.
	DisableReplyEvidence bool
	// DisableReattach suppresses explicit rooted reattach recording after close.
	DisableReattach bool
	// FailStopOnRootUnreach rejects metadata answers once rooted state becomes
	// unavailable, instead of serving degraded best-effort answers.
	FailStopOnRootUnreach bool
}

// Config returns the concrete switch set for one named ablation preset.
func (p Preset) Config() (Config, error) {
	switch p {
	case PresetFull:
		return Config{}, nil
	case PresetNoSeal:
		return Config{DisableSeal: true}, nil
	case PresetNoBudget:
		return Config{DisableBudget: true}, nil
	case PresetClientBlind:
		return Config{DisableClientVerify: true}, nil
	case PresetReplyBlindClientBlind:
		return Config{
			DisableReplyEvidence: true,
			DisableClientVerify:  true,
		}, nil
	case PresetNoReattach:
		return Config{DisableReattach: true}, nil
	case PresetFailStopOnRootUnreach:
		return Config{FailStopOnRootUnreach: true}, nil
	default:
		return Config{}, fmt.Errorf("unknown control-plane ablation preset %q", p)
	}
}

// Validate rejects semantically inconsistent switch combinations. The current
// artifact intentionally keeps the switch set small and only rules out
// combinations that cannot describe a meaningful closure path.
func (c Config) Validate() error {
	if c.DisableSeal && c.DisableReattach {
		return fmt.Errorf("invalid ablation config: disable_reattach requires seal path")
	}
	return nil
}
