// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type mountRegistrationCoordinator interface {
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
	AllocID(context.Context, *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error)
	PublishRootEvent(context.Context, *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error)
}

type fsmetaMountRegisterOptions struct {
	CoordinatorAddr string
	MountID         string
	MountKeyID      uint64
	RootInode       uint64
	SchemaVersion   uint32
	Timeout         time.Duration
}

type fsmetaMountRegistration struct {
	MountID       string
	MountKeyID    uint64
	RootInode     uint64
	SchemaVersion uint32
	AlreadyExists bool
}

func runFSMetaMountRegisterCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("fsmeta-mount-register", flag.ContinueOnError)
	coordinatorAddr := fs.String("coordinator-addr", "127.0.0.1:2379", "coordinator gRPC endpoint")
	mountID := fs.String("mount", "", "fsmeta mount id to register")
	mountKeyID := fs.Uint64("mount-key-id", 0, "rooted mount key id; 0 allocates one from coordinator")
	rootInode := fs.Uint64("root-inode", uint64(model.RootInode), "mount root inode")
	schemaVersion := fs.Uint("schema-version", 1, "fsmeta schema version")
	timeout := fs.Duration("timeout", 30*time.Second, "registration timeout")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *schemaVersion == 0 || *schemaVersion > uint(^uint32(0)) {
		return fmt.Errorf("invalid --schema-version %d", *schemaVersion)
	}
	opts := fsmetaMountRegisterOptions{
		CoordinatorAddr: strings.TrimSpace(*coordinatorAddr),
		MountID:         strings.TrimSpace(*mountID),
		MountKeyID:      *mountKeyID,
		RootInode:       *rootInode,
		SchemaVersion:   uint32(*schemaVersion),
		Timeout:         *timeout,
	}
	if err := validateFSMetaMountRegisterOptions(opts); err != nil {
		return err
	}
	if opts.Timeout <= 0 {
		return fmt.Errorf("fsmeta mount registration requires positive --timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	coordRPC, err := coordclient.NewGRPCClient(ctx, opts.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial coordinator for fsmeta mount registration: %w", err)
	}
	defer func() { _ = coordRPC.Close() }()
	registration, err := registerFSMetaMount(ctx, coordRPC, opts)
	if err != nil {
		return err
	}
	if registration.AlreadyExists {
		_, _ = fmt.Fprintf(w, "FSMeta mount already registered: mount=%s mount_key_id=%d root_inode=%d schema_version=%d\n", registration.MountID, registration.MountKeyID, registration.RootInode, registration.SchemaVersion)
		return nil
	}
	_, _ = fmt.Fprintf(w, "FSMeta mount registered: mount=%s mount_key_id=%d root_inode=%d schema_version=%d\n", registration.MountID, registration.MountKeyID, registration.RootInode, registration.SchemaVersion)
	return nil
}

func registerFSMetaMount(ctx context.Context, coord mountRegistrationCoordinator, opts fsmetaMountRegisterOptions) (fsmetaMountRegistration, error) {
	if coord == nil {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount registration requires coordinator client")
	}
	if err := validateFSMetaMountRegisterOptions(opts); err != nil {
		return fsmetaMountRegistration{}, err
	}
	existing, err := lookupFSMetaMount(ctx, coord, opts.MountID)
	if err != nil {
		return fsmetaMountRegistration{}, err
	}
	if existing != nil {
		return existingFSMetaMountRegistration(existing, opts)
	}
	mountKeyID := opts.MountKeyID
	if mountKeyID == 0 {
		alloc, err := coord.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
		if err != nil {
			return fsmetaMountRegistration{}, fmt.Errorf("allocate fsmeta mount key id: %w", err)
		}
		if alloc == nil || alloc.GetFirstId() == 0 || alloc.GetCount() != 1 {
			return fsmetaMountRegistration{}, fmt.Errorf("coordinator returned invalid mount key id allocation")
		}
		mountKeyID = alloc.GetFirstId()
	}
	resp, err := coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.MountRegistered(opts.MountID, mountKeyID, opts.RootInode, opts.SchemaVersion)),
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			existing, lookupErr := lookupFSMetaMount(ctx, coord, opts.MountID)
			if lookupErr == nil && existing != nil {
				return existingFSMetaMountRegistration(existing, fsmetaMountRegisterOptions{
					MountID:       opts.MountID,
					MountKeyID:    mountKeyID,
					RootInode:     opts.RootInode,
					SchemaVersion: opts.SchemaVersion,
				})
			}
		}
		return fsmetaMountRegistration{}, fmt.Errorf("register fsmeta mount: %w", err)
	}
	if resp == nil || !resp.GetAccepted() {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount registration was not accepted")
	}
	return fsmetaMountRegistration{
		MountID:       opts.MountID,
		MountKeyID:    mountKeyID,
		RootInode:     opts.RootInode,
		SchemaVersion: opts.SchemaVersion,
	}, nil
}

func validateFSMetaMountRegisterOptions(opts fsmetaMountRegisterOptions) error {
	if strings.TrimSpace(opts.CoordinatorAddr) == "" {
		return fmt.Errorf("fsmeta mount registration requires --coordinator-addr")
	}
	if strings.TrimSpace(opts.MountID) == "" {
		return fmt.Errorf("fsmeta mount registration requires --mount")
	}
	if opts.RootInode == 0 {
		return fmt.Errorf("fsmeta mount registration requires non-zero --root-inode")
	}
	if opts.SchemaVersion == 0 {
		return fmt.Errorf("fsmeta mount registration requires non-zero --schema-version")
	}
	return nil
}

func lookupFSMetaMount(ctx context.Context, coord mountRegistrationCoordinator, mountID string) (*coordpb.MountInfo, error) {
	resp, err := coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: mountID})
	if err != nil {
		return nil, fmt.Errorf("get fsmeta mount %q: %w", mountID, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("get fsmeta mount %q returned nil response", mountID)
	}
	if resp.GetNotFound() {
		return nil, nil
	}
	if resp.GetMount() == nil {
		return nil, fmt.Errorf("get fsmeta mount %q returned no mount", mountID)
	}
	return resp.GetMount(), nil
}

func existingFSMetaMountRegistration(existing *coordpb.MountInfo, opts fsmetaMountRegisterOptions) (fsmetaMountRegistration, error) {
	if existing == nil {
		return fsmetaMountRegistration{}, fmt.Errorf("existing fsmeta mount is nil")
	}
	if existing.GetMountId() != opts.MountID {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount lookup returned %q, not requested %q", existing.GetMountId(), opts.MountID)
	}
	if existing.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount %q is retired", opts.MountID)
	}
	if opts.MountKeyID != 0 && existing.GetMountKeyId() != opts.MountKeyID {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount %q has mount_key_id=%d, not requested %d", opts.MountID, existing.GetMountKeyId(), opts.MountKeyID)
	}
	if existing.GetRootInode() != 0 && existing.GetRootInode() != opts.RootInode {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount %q has root_inode=%d, not requested %d", opts.MountID, existing.GetRootInode(), opts.RootInode)
	}
	if existing.GetSchemaVersion() != 0 && existing.GetSchemaVersion() != opts.SchemaVersion {
		return fsmetaMountRegistration{}, fmt.Errorf("fsmeta mount %q has schema_version=%d, not requested %d", opts.MountID, existing.GetSchemaVersion(), opts.SchemaVersion)
	}
	return fsmetaMountRegistration{
		MountID:       existing.GetMountId(),
		MountKeyID:    existing.GetMountKeyId(),
		RootInode:     existing.GetRootInode(),
		SchemaVersion: existing.GetSchemaVersion(),
		AlreadyExists: true,
	}, nil
}
