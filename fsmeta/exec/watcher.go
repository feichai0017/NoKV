package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
)

// watcher wraps the in-process router with mount admission and pulls watch
// metrics together with remote-source metrics under a single Stats() surface
// for expvar exposure.
type watcher struct {
	*fsmetawatch.Router
	source *fsmetawatch.RemoteSource
	mounts MountResolver
}

func (w watcher) Subscribe(ctx context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	if req.Mount != "" && w.mounts != nil {
		record, err := w.mounts.ResolveMount(ctx, req.Mount)
		if err != nil {
			return nil, err
		}
		if record.MountID == "" {
			return nil, fsmeta.ErrMountNotRegistered
		}
		if record.Retired {
			return nil, fsmeta.ErrMountRetired
		}
	}
	if w.Router == nil {
		return nil, fsmeta.ErrInvalidRequest
	}
	return w.Router.Subscribe(ctx, req)
}

func (w watcher) Stats() map[string]any {
	out := map[string]any{}
	if w.Router != nil {
		for k, v := range w.Router.Stats() {
			out[k] = v
		}
	}
	if w.source != nil {
		for k, v := range w.source.Stats() {
			out[k] = v
		}
	}
	return out
}
