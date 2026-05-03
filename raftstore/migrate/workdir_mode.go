package migrate

import workdirmode "github.com/feichai0017/NoKV/runtime/mode"

func readState(workDir string) (workdirmode.State, error) {
	return workdirmode.Read(workDir)
}

func readMode(workDir string) (workdirmode.Mode, error) {
	return workdirmode.ReadOnlyMode(workDir)
}

func writeState(workDir string, state workdirmode.State) error {
	return workdirmode.Write(workDir, state)
}
