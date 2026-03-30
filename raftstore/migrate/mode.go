package migrate

import modepkg "github.com/feichai0017/NoKV/raftstore/mode"

type Mode = modepkg.Mode

const (
	ModeStandalone = modepkg.ModeStandalone
	ModePreparing  = modepkg.ModePreparing
	ModeSeeded     = modepkg.ModeSeeded
	ModeCluster    = modepkg.ModeCluster
	ModeFileName   = modepkg.FileName
)

type stateFile = modepkg.State

func readState(workDir string) (stateFile, error) { return modepkg.Read(workDir) }
func readMode(workDir string) (Mode, error)       { return modepkg.ReadOnlyMode(workDir) }
func writeState(workDir string, state stateFile) error {
	return modepkg.Write(workDir, state)
}
