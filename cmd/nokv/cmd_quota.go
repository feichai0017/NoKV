package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

type quotaCoordinatorClient interface {
	PublishRootEvent(context.Context, *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error)
	GetQuotaFence(context.Context, *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error)
	ListQuotaFences(context.Context, *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error)
	Close() error
}

var newQuotaCoordinatorClient = func(ctx context.Context, addr string) (quotaCoordinatorClient, error) {
	return coordclient.NewGRPCClient(ctx, addr)
}

func runQuotaCmd(w io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("quota requires subcommand: set, clear, or list")
	}
	switch args[0] {
	case "set":
		return runQuotaSetCmd(w, args[1:])
	case "clear":
		return runQuotaClearCmd(w, args[1:])
	case "list":
		return runQuotaListCmd(w, args[1:])
	case "help", "-h", "--help":
		printQuotaUsage(w)
		return nil
	default:
		return fmt.Errorf("unknown quota subcommand %q", args[0])
	}
}

func printQuotaUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, `Usage: nokv quota <set|clear|list> [flags]

Subcommands:
  set    Publish QuotaFenceUpdated rooted event
  clear  Clear a quota fence by publishing unlimited limits
  list   List rooted fsmeta quota fences`)
}

type quotaCmdConfig struct {
	coordinator string
	timeout     time.Duration
	mount       string
	subtreeRoot uint64
}

func quotaCommonFlags(fs *flag.FlagSet, cfg *quotaCmdConfig) {
	fs.StringVar(&cfg.coordinator, "coordinator-addr", "", "coordinator gRPC endpoint list")
	fs.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "coordinator RPC timeout")
	fs.StringVar(&cfg.mount, "mount", "", "mount id")
	fs.Uint64Var(&cfg.subtreeRoot, "subtree-root", 0, "optional subtree root inode; 0 means mount-wide")
	fs.SetOutput(io.Discard)
}

func (cfg quotaCmdConfig) context(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := cfg.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (cfg quotaCmdConfig) open(ctx context.Context) (quotaCoordinatorClient, error) {
	if strings.TrimSpace(cfg.coordinator) == "" {
		return nil, fmt.Errorf("--coordinator-addr is required")
	}
	return newQuotaCoordinatorClient(ctx, strings.TrimSpace(cfg.coordinator))
}

func (cfg quotaCmdConfig) subject() (*coordpb.QuotaSubject, error) {
	mountID := strings.TrimSpace(cfg.mount)
	if mountID == "" {
		return nil, fmt.Errorf("--mount is required")
	}
	return &coordpb.QuotaSubject{MountId: mountID, SubtreeRoot: cfg.subtreeRoot}, nil
}

func runQuotaSetCmd(w io.Writer, args []string) error {
	var cfg quotaCmdConfig
	fs := flag.NewFlagSet("quota set", flag.ContinueOnError)
	limitBytes := fs.Uint64("limit-bytes", 0, "byte limit; 0 means unlimited")
	limitInodes := fs.Uint64("limit-inodes", 0, "inode limit; 0 means unlimited")
	era := fs.Uint64("era", 0, "quota fence era; 0 means current+1")
	quotaCommonFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	subject, err := cfg.subject()
	if err != nil {
		return err
	}
	ctx, cancel := cfg.context(context.Background())
	defer cancel()
	cli, err := cfg.open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	nextEra, err := resolveQuotaEra(ctx, cli, subject, *era)
	if err != nil {
		return err
	}
	event := rootevent.QuotaFenceUpdated(subject.GetMountId(), subject.GetSubtreeRoot(), *limitBytes, *limitInodes, nextEra, 0)
	if err := publishQuotaEvent(ctx, cli, event); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "quota fence set mount=%s subtree_root=%d limit_bytes=%d limit_inodes=%d era=%d\n",
		subject.GetMountId(), subject.GetSubtreeRoot(), *limitBytes, *limitInodes, nextEra)
	return nil
}

func runQuotaClearCmd(w io.Writer, args []string) error {
	var cfg quotaCmdConfig
	fs := flag.NewFlagSet("quota clear", flag.ContinueOnError)
	era := fs.Uint64("era", 0, "quota fence era; 0 means current+1")
	quotaCommonFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	subject, err := cfg.subject()
	if err != nil {
		return err
	}
	ctx, cancel := cfg.context(context.Background())
	defer cancel()
	cli, err := cfg.open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	nextEra, err := resolveQuotaEra(ctx, cli, subject, *era)
	if err != nil {
		return err
	}
	event := rootevent.QuotaFenceUpdated(subject.GetMountId(), subject.GetSubtreeRoot(), 0, 0, nextEra, 0)
	if err := publishQuotaEvent(ctx, cli, event); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "quota fence cleared mount=%s subtree_root=%d era=%d\n", subject.GetMountId(), subject.GetSubtreeRoot(), nextEra)
	return nil
}

func runQuotaListCmd(w io.Writer, args []string) error {
	var cfg quotaCmdConfig
	fs := flag.NewFlagSet("quota list", flag.ContinueOnError)
	fs.StringVar(&cfg.coordinator, "coordinator-addr", "", "coordinator gRPC endpoint list")
	fs.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "coordinator RPC timeout")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := cfg.context(context.Background())
	defer cancel()
	cli, err := cfg.open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	resp, err := cli.ListQuotaFences(ctx, &coordpb.ListQuotaFencesRequest{})
	if err != nil {
		return err
	}
	if len(resp.GetFences()) == 0 {
		_, _ = fmt.Fprintln(w, "Quota fences: (none)")
		return nil
	}
	_, _ = fmt.Fprintln(w, "Quota fences:")
	for _, fence := range resp.GetFences() {
		subject := fence.GetSubject()
		_, _ = fmt.Fprintf(w, "  - mount=%s subtree_root=%d limit_bytes=%d limit_inodes=%d era=%d\n",
			subject.GetMountId(), subject.GetSubtreeRoot(), fence.GetLimitBytes(), fence.GetLimitInodes(), fence.GetEra())
	}
	return nil
}

func resolveQuotaEra(ctx context.Context, cli quotaCoordinatorClient, subject *coordpb.QuotaSubject, explicit uint64) (uint64, error) {
	if explicit > 0 {
		return explicit, nil
	}
	resp, err := cli.GetQuotaFence(ctx, &coordpb.GetQuotaFenceRequest{Subject: subject})
	if err != nil {
		return 0, err
	}
	if resp == nil || resp.GetNotFound() || resp.GetFence() == nil {
		return 1, nil
	}
	return resp.GetFence().GetEra() + 1, nil
}

func publishQuotaEvent(ctx context.Context, cli quotaCoordinatorClient, event rootevent.Event) error {
	resp, err := cli.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{Event: metawire.RootEventToProto(event)})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return fmt.Errorf("quota root event was not accepted")
	}
	return nil
}
