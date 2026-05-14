// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package migrate

import workdirmode "github.com/feichai0017/NoKV/local/workdir"

func readState(workDir string) (workdirmode.State, error) {
	return workdirmode.Read(workDir)
}

func readMode(workDir string) (workdirmode.Mode, error) {
	return workdirmode.ReadOnlyMode(workDir)
}

func writeState(workDir string, state workdirmode.State) error {
	return workdirmode.Write(workDir, state)
}
