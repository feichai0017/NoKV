package bad

import (
	"errors"
	"strings"
)

var ErrSentinel = errors.New("sentinel")

func badEq(err error) bool {
	return err.Error() == "sentinel" // want `code_contract §8: do not compare err.Error.*`
}

func badContains(err error) bool {
	return strings.Contains(err.Error(), "boom") // want `code_contract §8: strings.Contains against err.Error.*`
}

func badNeq(err error) bool {
	return err.Error() != "" // want `code_contract §8: do not compare err.Error.*`
}
