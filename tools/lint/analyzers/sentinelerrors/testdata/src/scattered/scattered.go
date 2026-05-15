package scattered

import "errors"

var ErrFoo = errors.New("foo") // want `code_contract §8: sentinel ErrFoo must be declared in errors.go.*`

// ErrPrefix below is intentionally not an error value: the type does not
// implement error, so the analyzer should ignore it.
type ErrCode int

var ErrCodeBoot ErrCode = 1

func Use() error { return ErrFoo }
