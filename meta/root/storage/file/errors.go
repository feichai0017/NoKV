package file

import "errors"

var (
	errCheckpointMissingState = errors.New("root checkpoint missing state")
)
