// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"gopkg.in/yaml.v3"
)

const DefaultScaleProfile = "median"

var officialProfiles = sync.OnceValue(loadOfficialProfiles)

type officialProfileFile struct {
	Profiles map[string]OfficialProfile `yaml:"profiles"`
}

func ProfileFor(name string) OfficialProfile {
	profiles := officialProfiles()
	if profile, ok := profiles[name]; ok {
		profile.Workload = name
		return profile
	}
	return OfficialProfile{
		Workload:   name,
		Source:     "custom",
		Projection: "native fsmeta workload",
		Scale:      map[string]OfficialScale{DefaultScaleProfile: {}},
	}
}

func ScaleFor(name, scaleProfile string) OfficialScale {
	profile := ProfileFor(name)
	if scaleProfile == "" {
		scaleProfile = DefaultScaleProfile
	}
	if scale, ok := profile.Scale[scaleProfile]; ok {
		return scale
	}
	if scale, ok := profile.Scale[DefaultScaleProfile]; ok {
		return scale
	}
	return OfficialScale{}
}

func OfficialProfilePath() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return filepath.Join("benchmark", "fsmeta", "profiles", "official", "workloads.yaml")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "profiles", "official", "workloads.yaml"))
}

func loadOfficialProfiles() map[string]OfficialProfile {
	data, err := os.ReadFile(OfficialProfilePath())
	if err != nil {
		return defaultOfficialProfiles()
	}
	var file officialProfileFile
	if err := yaml.Unmarshal(data, &file); err != nil || len(file.Profiles) == 0 {
		return defaultOfficialProfiles()
	}
	for name, profile := range file.Profiles {
		profile.Workload = name
		file.Profiles[name] = profile
	}
	return file.Profiles
}

func defaultOfficialProfiles() map[string]OfficialProfile {
	return map[string]OfficialProfile{
		MDTestEasy: {
			Workload:   MDTestEasy,
			Source:     "IO500 mdtest-easy",
			SourceURL:  "https://io500.org/about",
			Shape:      "one private directory per process; zero-byte files; create/stat/delete phases",
			Projection: "fsmeta Create directory/file, Lookup stat, ReadDirPlus namespace scan, Unlink",
			Scale:      map[string]OfficialScale{DefaultScaleProfile: {Clients: 12, Directories: 16, FilesPerDirectory: 256, PageLimit: 256}},
		},
		MDTestHard: {
			Workload:   MDTestHard,
			Source:     "IO500 mdtest-hard",
			SourceURL:  "https://io500.org/about",
			Shape:      "single shared directory; 3901-byte files; create/stat/read/delete phases",
			Projection: "fsmeta shared-directory Create, Lookup stat, ReadDirPlus metadata read, Unlink; file-body read is outside fsmeta",
			Scale:      map[string]OfficialScale{DefaultScaleProfile: {Clients: 12, Directories: 1, FilesPerDirectory: 256, PageLimit: 256}},
		},
		FilebenchVarmail: {
			Workload:   FilebenchVarmail,
			Source:     "Filebench varmail.f",
			SourceURL:  "https://github.com/filebench/filebench/blob/master/workloads/varmail.f",
			Shape:      "mail-spool personality with create, append, fsync, close, open/read, append/fsync, close",
			Projection: "fsmeta Create, UpdateInode, writer session open/heartbeat/close, ReadDirPlus, Unlink",
			Scale:      map[string]OfficialScale{DefaultScaleProfile: {Clients: 12, Users: 16, MessagesPerUser: 128, PageLimit: 128, SessionTTL: "5m"}},
		},
		MimesisNamespace: {
			Workload:   MimesisNamespace,
			Source:     "MimesisBench namespace model",
			SourceURL:  "https://www.usenix.org/conference/icac14/technical-sessions/presentation/abad",
			Shape:      "model-based namespace metadata benchmark for HDFS and big-data storage systems",
			Projection: "fsmeta Create, Rename, UpdateInode, Lookup, ReadDirPlus, Unlink namespace churn",
			Scale:      map[string]OfficialScale{DefaultScaleProfile: {Clients: 12, Directories: 16, FilesPerDirectory: 256, PageLimit: 256}},
		},
		AICheckpointAgent: {
			Workload:   AICheckpointAgent,
			Source:     "MLPerf Storage checkpointing",
			SourceURL:  "https://mlcommons.org/2025/08/storage-2-checkpointing/",
			Shape:      "LLM checkpoint save/load workload with 8B, 70B, 405B, and 1T model scales",
			Projection: "metadata-only checkpoint publish: artifact fan-out, manifest update/rename, watch, snapshot read, snapshot retire",
			Scale:      map[string]OfficialScale{DefaultScaleProfile: {Clients: 12, Workspaces: 4, CheckpointsPerWorkspace: 64, FilesPerCheckpoint: 8, PageLimit: 9, SessionTTL: "5m"}},
		},
	}
}
