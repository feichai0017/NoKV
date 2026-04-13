package main

import "errors"

var (
	errConditionNotMet = errors.New("redis condition not met")
	errUnsupported     = errors.New("redis feature unsupported for current backend")
	errQuit            = errors.New("client quit")
	errNotInteger      = errors.New("value is not an integer or out of range")
	errOverflow        = errors.New("increment or decrement would overflow")
	errNotIntegerMsg   = "ERR value is not an integer or out of range"
	errOverflowMsg     = "ERR increment or decrement would overflow"
)
