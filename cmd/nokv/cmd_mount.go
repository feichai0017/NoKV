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

type mountCoordinatorClient interface {
	PublishRootEvent(context.Context, *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error)
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
	Close() error
}

var newMountCoordinatorClient = func(ctx context.Context, addr string) (mountCoordinatorClient, error) {
	return coordclient.NewGRPCClient(ctx, addr)
}

func runMountCmd(w io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("mount requires subcommand: register, retire, or list")
	}
	switch args[0] {
	case "register":
		return runMountRegisterCmd(w, args[1:])
	case "retire":
		return runMountRetireCmd(w, args[1:])
	case "list":
		return runMountListCmd(w, args[1:])
	case "help", "-h", "--help":
		printMountUsage(w)
		return nil
	default:
		return fmt.Errorf("unknown mount subcommand %q", args[0])
	}
}

func printMountUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, `Usage: nokv mount <register|retire|list> [flags]

Subcommands:
  register  Publish MountRegistered rooted event
  retire    Publish MountRetired rooted event
  list      List rooted fsmeta mounts`)
}

type mountCmdConfig struct {
	coordinator string
	timeout     time.Duration
}

func mountCommonFlags(fs *flag.FlagSet, cfg *mountCmdConfig) {
	fs.StringVar(&cfg.coordinator, "coordinator-addr", "", "coordinator gRPC endpoint list")
	fs.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "coordinator RPC timeout")
	fs.SetOutput(io.Discard)
}

func (cfg mountCmdConfig) context(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := cfg.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (cfg mountCmdConfig) open(ctx context.Context) (mountCoordinatorClient, error) {
	if strings.TrimSpace(cfg.coordinator) == "" {
		return nil, fmt.Errorf("--coordinator-addr is required")
	}
	return newMountCoordinatorClient(ctx, strings.TrimSpace(cfg.coordinator))
}

func runMountRegisterCmd(w io.Writer, args []string) error {
	var cfg mountCmdConfig
	fs := flag.NewFlagSet("mount register", flag.ContinueOnError)
	mount := fs.String("mount", "", "mount id")
	rootInode := fs.Uint64("root-inode", 1, "root inode id")
	schemaVersion := fs.Uint("schema-version", 1, "fsmeta schema version")
	mountCommonFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	mountID := strings.TrimSpace(*mount)
	if mountID == "" {
		return fmt.Errorf("--mount is required")
	}
	if *rootInode == 0 {
		return fmt.Errorf("--root-inode must be > 0")
	}
	if *schemaVersion == 0 {
		return fmt.Errorf("--schema-version must be > 0")
	}
	ctx, cancel := cfg.context(context.Background())
	defer cancel()
	cli, err := cfg.open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	resp, err := cli.GetMount(ctx, &coordpb.GetMountRequest{MountId: mountID})
	if err != nil {
		return err
	}
	if resp != nil && !resp.GetNotFound() {
		info := resp.GetMount()
		if info == nil {
			return fmt.Errorf("mount %q returned empty rooted record", mountID)
		}
		if info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED {
			return fmt.Errorf("mount %q is retired", mountID)
		}
		if info.GetRootInode() != *rootInode || info.GetSchemaVersion() != uint32(*schemaVersion) {
			return fmt.Errorf("mount %q conflicts with rooted truth", mountID)
		}
		_, _ = fmt.Fprintf(w, "mount %q already registered root_inode=%d schema_version=%d\n", mountID, *rootInode, *schemaVersion)
		return nil
	}
	if err := publishMountEvent(ctx, cli, rootevent.MountRegistered(mountID, *rootInode, uint32(*schemaVersion))); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "mount %q registered root_inode=%d schema_version=%d\n", mountID, *rootInode, *schemaVersion)
	return nil
}

func runMountRetireCmd(w io.Writer, args []string) error {
	var cfg mountCmdConfig
	fs := flag.NewFlagSet("mount retire", flag.ContinueOnError)
	mount := fs.String("mount", "", "mount id")
	mountCommonFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	mountID := strings.TrimSpace(*mount)
	if mountID == "" {
		return fmt.Errorf("--mount is required")
	}
	ctx, cancel := cfg.context(context.Background())
	defer cancel()
	cli, err := cfg.open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	if err := publishMountEvent(ctx, cli, rootevent.MountRetired(mountID)); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "mount %q retired\n", mountID)
	return nil
}

func runMountListCmd(w io.Writer, args []string) error {
	var cfg mountCmdConfig
	fs := flag.NewFlagSet("mount list", flag.ContinueOnError)
	mountCommonFlags(fs, &cfg)
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

	resp, err := cli.ListMounts(ctx, &coordpb.ListMountsRequest{})
	if err != nil {
		return err
	}
	if len(resp.GetMounts()) == 0 {
		_, _ = fmt.Fprintln(w, "Mounts: (none)")
		return nil
	}
	_, _ = fmt.Fprintln(w, "Mounts:")
	for _, mount := range resp.GetMounts() {
		_, _ = fmt.Fprintf(w, "  - id=%s root_inode=%d schema_version=%d state=%s\n",
			mount.GetMountId(), mount.GetRootInode(), mount.GetSchemaVersion(), mount.GetState().String())
	}
	return nil
}

func publishMountEvent(ctx context.Context, cli mountCoordinatorClient, event rootevent.Event) error {
	resp, err := cli.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{Event: metawire.RootEventToProto(event)})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return fmt.Errorf("mount root event was not accepted")
	}
	return nil
}
