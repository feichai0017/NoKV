// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func captureExitCode(t *testing.T, fn func()) (code int) {
	t.Helper()
	origExit := exit
	defer func() { exit = origExit }()
	exit = func(code int) {
		panic(code)
	}
	defer func() {
		if r := recover(); r != nil {
			if c, ok := r.(int); ok {
				code = c
				return
			}
			panic(r)
		}
	}()
	fn()
	return code
}
