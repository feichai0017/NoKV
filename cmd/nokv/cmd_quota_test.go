package main

import (
	"bytes"
	"context"
	"testing"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeQuotaClient struct {
	published []*coordpb.PublishRootEventRequest
	fences    []*coordpb.QuotaFenceInfo
	closed    bool
}

func (c *fakeQuotaClient) PublishRootEvent(_ context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	c.published = append(c.published, req)
	return &coordpb.PublishRootEventResponse{Accepted: true}, nil
}

func (c *fakeQuotaClient) GetQuotaFence(_ context.Context, req *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error) {
	for _, fence := range c.fences {
		subject := fence.GetSubject()
		if subject.GetMountId() == req.GetSubject().GetMountId() && subject.GetSubtreeRoot() == req.GetSubject().GetSubtreeRoot() {
			return &coordpb.GetQuotaFenceResponse{Fence: fence}, nil
		}
	}
	return &coordpb.GetQuotaFenceResponse{NotFound: true}, nil
}

func (c *fakeQuotaClient) ListQuotaFences(context.Context, *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error) {
	return &coordpb.ListQuotaFencesResponse{Fences: c.fences}, nil
}

func (c *fakeQuotaClient) Close() error {
	c.closed = true
	return nil
}

func TestRunQuotaSetCmdPublishesRootEvent(t *testing.T) {
	client := &fakeQuotaClient{}
	orig := newQuotaCoordinatorClient
	newQuotaCoordinatorClient = func(context.Context, string) (quotaCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newQuotaCoordinatorClient = orig })

	var out bytes.Buffer
	err := runQuotaCmd(&out, []string{
		"set",
		"--coordinator-addr", "127.0.0.1:2379",
		"--mount", "vol",
		"--limit-bytes", "1024",
		"--limit-inodes", "10",
	})
	require.NoError(t, err)
	require.True(t, client.closed)
	require.Len(t, client.published, 1)
	fence := client.published[0].GetEvent().GetQuotaFence()
	require.NotNil(t, fence)
	require.Equal(t, "vol", fence.GetMount())
	require.Equal(t, uint64(1024), fence.GetLimitBytes())
	require.Equal(t, uint64(10), fence.GetLimitInodes())
	require.Equal(t, uint64(1), fence.GetEra())
	require.Contains(t, out.String(), "quota fence set")
}

func TestRunQuotaSetCmdAdvancesExistingEra(t *testing.T) {
	client := &fakeQuotaClient{fences: []*coordpb.QuotaFenceInfo{{
		Subject: &coordpb.QuotaSubject{MountId: "vol"},
		Era:     7,
	}}}
	orig := newQuotaCoordinatorClient
	newQuotaCoordinatorClient = func(context.Context, string) (quotaCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newQuotaCoordinatorClient = orig })

	var out bytes.Buffer
	err := runQuotaCmd(&out, []string{
		"set",
		"--coordinator-addr", "127.0.0.1:2379",
		"--mount", "vol",
		"--limit-inodes", "11",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(8), client.published[0].GetEvent().GetQuotaFence().GetEra())
}

func TestRunQuotaListCmdRendersFences(t *testing.T) {
	client := &fakeQuotaClient{fences: []*coordpb.QuotaFenceInfo{{
		Subject:     &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 7},
		LimitBytes:  1024,
		LimitInodes: 10,
		Era:         2,
	}}}
	orig := newQuotaCoordinatorClient
	newQuotaCoordinatorClient = func(context.Context, string) (quotaCoordinatorClient, error) {
		return client, nil
	}
	t.Cleanup(func() { newQuotaCoordinatorClient = orig })

	var out bytes.Buffer
	err := runQuotaCmd(&out, []string{"list", "--coordinator-addr", "127.0.0.1:2379"})
	require.NoError(t, err)
	require.Contains(t, out.String(), "mount=vol")
	require.Contains(t, out.String(), "subtree_root=7")
	require.Contains(t, out.String(), "limit_inodes=10")
}
