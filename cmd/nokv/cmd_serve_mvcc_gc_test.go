// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

func TestServeTSOSourceCachesLastKnownCurrent(t *testing.T) {
	now := time.Unix(100, 0)
	source := newServeTSOSource(&fakeServeTSOClient{
		timestamps: []uint64{200},
		errs:       []error{nil, errors.New("coordinator unavailable"), errors.New("coordinator unavailable")},
	}, time.Second, 10, time.Minute)
	source.now = func() time.Time { return now }

	require.Equal(t, uint64(190), source.SafePoint())
	require.Equal(t, uint64(190), source.SafePoint())

	now = now.Add(2 * time.Minute)
	require.Zero(t, source.SafePoint())
}

func TestServeTSOSourceCacheCanBeDisabled(t *testing.T) {
	source := newServeTSOSource(&fakeServeTSOClient{
		timestamps: []uint64{200},
		errs:       []error{nil, errors.New("coordinator unavailable")},
	}, time.Second, 10, 0)

	require.Equal(t, uint64(190), source.SafePoint())
	require.Zero(t, source.SafePoint())
}

type fakeServeTSOClient struct {
	timestamps []uint64
	errs       []error
	calls      int
}

func (c *fakeServeTSOClient) Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	call := c.calls
	c.calls++
	if call < len(c.errs) && c.errs[call] != nil {
		return nil, c.errs[call]
	}
	var ts uint64
	if call < len(c.timestamps) {
		ts = c.timestamps[call]
	}
	return &coordpb.TsoResponse{Timestamp: ts}, nil
}
