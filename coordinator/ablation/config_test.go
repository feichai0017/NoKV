package ablation

import "testing"

func TestPresetConfig(t *testing.T) {
	tests := []struct {
		name string
		got  Config
		want Config
	}{
		{name: string(PresetFull), got: mustConfig(t, PresetFull), want: Config{}},
		{name: string(PresetNoSeal), got: mustConfig(t, PresetNoSeal), want: Config{DisableSeal: true}},
		{name: string(PresetNoBudget), got: mustConfig(t, PresetNoBudget), want: Config{DisableBudget: true}},
		{name: string(PresetClientBlind), got: mustConfig(t, PresetClientBlind), want: Config{DisableClientVerify: true}},
		{name: string(PresetReplyBlindClientBlind), got: mustConfig(t, PresetReplyBlindClientBlind), want: Config{DisableReplyEvidence: true, DisableClientVerify: true}},
		{name: string(PresetNoReattach), got: mustConfig(t, PresetNoReattach), want: Config{DisableReattach: true}},
		{name: string(PresetFailStopOnRootUnreach), got: mustConfig(t, PresetFailStopOnRootUnreach), want: Config{FailStopOnRootUnreach: true}},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Fatalf("%s: got %+v want %+v", tc.name, tc.got, tc.want)
		}
	}
}

func TestPresetConfigRejectsUnknownPreset(t *testing.T) {
	_, err := Preset("unknown").Config()
	if err == nil {
		t.Fatal("expected unknown preset to return error")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("baseline config should be valid: %v", err)
	}
	if err := (Config{DisableSeal: true, DisableReattach: true}).Validate(); err == nil {
		t.Fatal("expected invalid disable_seal + disable_reattach combination")
	}
}

func mustConfig(t *testing.T, preset Preset) Config {
	t.Helper()
	cfg, err := preset.Config()
	if err != nil {
		t.Fatalf("%s: %v", preset, err)
	}
	return cfg
}
