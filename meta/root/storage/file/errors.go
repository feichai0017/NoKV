package file

import "errors"

var (
	errCheckpointMissingState = errors.New("root checkpoint missing state")
)

func IsCheckpointMissingState(err error) bool {
	return errors.Is(err, errCheckpointMissingState)
}
