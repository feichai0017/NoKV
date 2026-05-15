// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lint registers NoKV's repository-specific golangci-lint module
// plugin. The analyzers under analyzers/ encode the mechanically checkable
// rules from docs/guide/development/code_contract.md. Stock golangci-lint
// linters cover style and correctness; this plugin covers repository
// conventions that the contract requires reviewers to enforce by hand.
package lint
