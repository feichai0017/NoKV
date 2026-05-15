package ok

import "errors"

var ErrSentinel = errors.New("sentinel")

func okErrorsIs(err error) bool {
	return errors.Is(err, ErrSentinel)
}

func okStringCompare(s string) bool {
	// Not an error: comparing plain string equality is allowed.
	return s == "hello"
}
