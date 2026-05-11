package perasauth

import (
	"context"
	"log"
	"sync"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
)

const defaultAuthorityFeedInterval = time.Second

// RootAuthoritySource is the narrow coordinator surface needed by storage
// witnesses to follow rooted Peras authority grants.
type RootAuthoritySource interface {
	ListPerasAuthorityGrants(context.Context, *coordpb.ListPerasAuthorityGrantsRequest) (*coordpb.ListPerasAuthorityGrantsResponse, error)
	WatchRootEvents(context.Context, *coordpb.WatchRootEventsRequest, ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error)
}

// RootAuthorityFeed keeps an ActiveAuthorities table current from meta/root
// events. It owns one background stream and is intentionally limited to Peras
// grants; fsmeta's broader lifecycle monitor handles mounts, quotas, and
// subtrees.
type RootAuthorityFeed struct {
	source   RootAuthoritySource
	table    *ActiveAuthorities
	interval time.Duration
	cancel   context.CancelFunc
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func StartRootAuthorityFeed(ctx context.Context, source RootAuthoritySource, table *ActiveAuthorities, interval time.Duration) *RootAuthorityFeed {
	if source == nil || table == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	if interval <= 0 {
		interval = defaultAuthorityFeedInterval
	}
	f := &RootAuthorityFeed{
		source:   source,
		table:    table,
		interval: interval,
		cancel:   cancel,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go f.run(runCtx)
	return f
}

func (f *RootAuthorityFeed) run(ctx context.Context) {
	defer close(f.done)
	var wg sync.WaitGroup
	wg.Go(func() {
		f.poll(ctx)
	})
	defer wg.Wait()

	var after rootstorage.TailToken
	for {
		if err := f.watch(ctx, &after); err != nil && ctx.Err() == nil {
			log.Printf("peras authority feed watch: %v", err)
		}
		if !f.wait(ctx, f.interval) {
			return
		}
	}
}

func (f *RootAuthorityFeed) poll(ctx context.Context) {
	for {
		if err := f.bootstrap(ctx); err != nil && ctx.Err() == nil {
			log.Printf("peras authority feed bootstrap: %v", err)
		}
		if !f.wait(ctx, f.interval) {
			return
		}
	}
}

func (f *RootAuthorityFeed) bootstrap(ctx context.Context) error {
	resp, err := f.source.ListPerasAuthorityGrants(ctx, &coordpb.ListPerasAuthorityGrantsRequest{})
	if err != nil {
		return err
	}
	grants := make([]AuthorityGrant, 0, len(resp.GetGrants()))
	for _, pbGrant := range resp.GetGrants() {
		grant := metawire.RootPerasAuthorityGrantFromProto(pbGrant)
		if grant.Valid() {
			grants = append(grants, grant)
		}
	}
	return f.table.Replace(grants)
}

// Refresh synchronously installs the latest root authority snapshot. Foreground
// paths use this as a cache-miss repair before rejecting an otherwise valid
// holder request due to watch or polling lag.
func (f *RootAuthorityFeed) Refresh(ctx context.Context) error {
	if f == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return f.bootstrap(ctx)
}

func (f *RootAuthorityFeed) watch(ctx context.Context, after *rootstorage.TailToken) error {
	var token rootstorage.TailToken
	if after != nil {
		token = *after
	}
	stream, err := f.source.WatchRootEvents(ctx, &coordpb.WatchRootEventsRequest{
		After: metawire.RootTailTokenToProto(token),
	})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if resp.GetBootstrapRequired() {
			if err := f.bootstrap(ctx); err != nil {
				return err
			}
		}
		token = metawire.RootTailTokenFromProto(resp.GetToken())
		if after != nil {
			*after = token
		}
		for _, record := range resp.GetEvents() {
			f.applyRootEvent(metawire.RootEventFromProto(record.GetEvent()))
		}
	}
}

func (f *RootAuthorityFeed) applyRootEvent(event rootevent.Event) {
	switch event.Kind {
	case rootevent.KindPerasAuthorityGranted, rootevent.KindPerasAuthorityRetired:
		if err := f.table.ApplyRootEvent(event); err != nil {
			log.Printf("peras authority feed apply event kind=%d: %v", event.Kind, err)
		}
	}
}

func (f *RootAuthorityFeed) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-f.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (f *RootAuthorityFeed) Close() error {
	if f == nil {
		return nil
	}
	f.once.Do(func() {
		if f.cancel != nil {
			f.cancel()
		}
		close(f.stop)
	})
	<-f.done
	return nil
}
