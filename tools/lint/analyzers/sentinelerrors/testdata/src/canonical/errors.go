package canonical

import "errors"

var ErrFoo = errors.New("foo")

func Use() error { return ErrFoo }
