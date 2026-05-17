// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package peras marks Peras visible-before-durable metadata execution as an
// explicit experiment.
//
// Core protocol code lives in exec and runtime. Optional integration with
// stable NoKV packages lives under adapters, so stable code only needs neutral
// extension points and command wiring to attach Peras.
package peras
