package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
)

func (e *Executor) requireActiveMount(ctx context.Context, mount fsmeta.MountID) error {
	_, err := e.resolveActiveMount(ctx, mount)
	return err
}

func (e *Executor) resolveActiveMount(ctx context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	record, err := e.resolveKnownMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.Retired {
		return MountAdmission{}, fsmeta.ErrMountRetired
	}
	return record, nil
}

func (e *Executor) resolveKnownMount(ctx context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	if e == nil || e.mounts == nil {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	record, err := e.mounts.ResolveMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.MountID == "" {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	if record.MountKeyID == 0 {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	return record, nil
}
