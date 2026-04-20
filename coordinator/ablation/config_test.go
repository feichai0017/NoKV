package ablation

import "testing"

func TestPresetConfig(t *testing.T) {
	tests := []struct {
		name string
		got  Config
		want Config
	}{
		{name: string(PresetFull), got: PresetFull.Config(), want: Config{}},
		{name: string(PresetNoSeal), got: PresetNoSeal.Config(), want: Config{DisableSeal: true}},
		{name: string(PresetNoBudget), got: PresetNoBudget.Config(), want: Config{DisableBudget: true}},
		{name: string(PresetClientBlind), got: PresetClientBlind.Config(), want: Config{DisableClientVerify: true}},
		{name: string(PresetReplyBlindClientBlind), got: PresetReplyBlindClientBlind.Config(), want: Config{DisableReplyEvidence: true, DisableClientVerify: true}},
		{name: string(PresetNoReattach), got: PresetNoReattach.Config(), want: Config{DisableReattach: true}},
		{name: string(PresetFailStopOnRootUnreach), got: PresetFailStopOnRootUnreach.Config(), want: Config{FailStopOnRootUnreach: true}},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Fatalf("%s: got %+v want %+v", tc.name, tc.got, tc.want)
		}
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
