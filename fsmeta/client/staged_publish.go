package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
)

const stagedPublishCleanupTimeout = 5 * time.Second

// StagedPublishClient is the narrow namespace surface needed for staged
// publish. GRPCClient satisfies it.
type StagedPublishClient interface {
	Create(context.Context, fsmeta.CreateRequest) (fsmeta.CreateResult, error)
	RenameSubtree(context.Context, fsmeta.RenameSubtreeRequest) error
	Unlink(context.Context, fsmeta.UnlinkRequest) error
}

// StagedPublishRequest describes one namespace-only stage -> commit publish.
//
// The helper creates (StageParent, StageName), lets the caller prepare any
// external body/object state, then atomically renames the staged entry to
// (FinalParent, FinalName). NoKV does not write or validate the body itself;
// callers typically store body references inside Inode.OpaqueAttrs.
type StagedPublishRequest struct {
	Mount       fsmeta.MountID
	StageParent fsmeta.InodeID
	StageName   string
	FinalParent fsmeta.InodeID
	FinalName   string
	Attrs       fsmeta.CreateAttrs
}

// PrepareStagedFunc is called after the staged namespace entry is created and
// before the final rename. The callback is where a caller writes external body
// data, uploads an object, or validates a content-addressed reference.
type PrepareStagedFunc func(context.Context, fsmeta.CreateResult) error

// PublishStagedNamespaceEntry implements the stage -> commit namespace pattern.
//
// Failure semantics:
//   - create failure: no callback is called;
//   - prepare failure: the staged entry is best-effort unlinked before
//     returning the original error, joined with cleanup failure if any;
//   - final rename failure: the staged entry is intentionally left in place so
//     the caller can inspect or retry without losing prepared external state.
func PublishStagedNamespaceEntry(ctx context.Context, cli StagedPublishClient, req StagedPublishRequest, prepare PrepareStagedFunc) error {
	if cli == nil {
		return errStagedPublishClientRequired
	}
	create := req.createRequest()
	if _, err := fsmeta.PlanCreate(create, fsmeta.RootInode); err != nil {
		return err
	}
	rename := req.renameRequest()
	if _, err := fsmeta.PlanRenameSubtree(rename); err != nil {
		return err
	}

	result, err := cli.Create(ctx, create)
	if err != nil {
		return err
	}
	if prepare != nil {
		if err := prepare(ctx, result); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stagedPublishCleanupTimeout)
			defer cancel()
			if cleanupErr := cli.Unlink(cleanupCtx, fsmeta.UnlinkRequest{
				Mount:  req.Mount,
				Parent: req.StageParent,
				Name:   req.StageName,
			}); cleanupErr != nil {
				return errors.Join(err, fmt.Errorf("cleanup staged namespace entry: %w", cleanupErr))
			}
			return err
		}
	}
	return cli.RenameSubtree(ctx, rename)
}

func (r StagedPublishRequest) createRequest() fsmeta.CreateRequest {
	return fsmeta.CreateRequest{
		Mount:  r.Mount,
		Parent: r.StageParent,
		Name:   r.StageName,
		Attrs:  r.Attrs,
	}
}

func (r StagedPublishRequest) renameRequest() fsmeta.RenameSubtreeRequest {
	return fsmeta.RenameSubtreeRequest{
		Mount:      r.Mount,
		FromParent: r.StageParent,
		FromName:   r.StageName,
		ToParent:   r.FinalParent,
		ToName:     r.FinalName,
	}
}
