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

const defaultMirrorInterval = time.Second

// MirrorSource is the narrow coordinator surface needed by storage witnesses
// to follow rooted Peras authority grants.
type MirrorSource interface {
	ListPerasAuthorityGrants(context.Context, *coordpb.ListPerasAuthorityGrantsRequest) (*coordpb.ListPerasAuthorityGrantsResponse, error)
	WatchRootEvents(context.Context, *coordpb.WatchRootEventsRequest, ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error)
}

// Mirror keeps an ActiveAuthorities table current from meta/root events.
// It owns one background stream and is intentionally limited to Peras grants;
// fsmeta's broader lifecycle monitor handles mounts, quotas, and subtrees.
type Mirror struct {
	source   MirrorSource
	table    *ActiveAuthorities
	interval time.Duration
	cancel   context.CancelFunc
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func StartMirror(ctx context.Context, source MirrorSource, table *ActiveAuthorities, interval time.Duration) *Mirror {
	if source == nil || table == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	if interval <= 0 {
		interval = defaultMirrorInterval
	}
	m := &Mirror{
		source:   source,
		table:    table,
		interval: interval,
		cancel:   cancel,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go m.run(runCtx)
	return m
}

func (m *Mirror) run(ctx context.Context) {
	defer close(m.done)
	var wg sync.WaitGroup
	wg.Go(func() {
		m.poll(ctx)
	})
	defer wg.Wait()

	var after rootstorage.TailToken
	for {
		if err := m.watch(ctx, &after); err != nil && ctx.Err() == nil {
			log.Printf("peras authority mirror watch: %v", err)
		}
		if !m.wait(ctx, m.interval) {
			return
		}
	}
}

func (m *Mirror) poll(ctx context.Context) {
	for {
		if err := m.bootstrap(ctx); err != nil && ctx.Err() == nil {
			log.Printf("peras authority mirror bootstrap: %v", err)
		}
		if !m.wait(ctx, m.interval) {
			return
		}
	}
}

func (m *Mirror) bootstrap(ctx context.Context) error {
	resp, err := m.source.ListPerasAuthorityGrants(ctx, &coordpb.ListPerasAuthorityGrantsRequest{})
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
	return m.table.Replace(grants)
}

func (m *Mirror) watch(ctx context.Context, after *rootstorage.TailToken) error {
	var token rootstorage.TailToken
	if after != nil {
		token = *after
	}
	stream, err := m.source.WatchRootEvents(ctx, &coordpb.WatchRootEventsRequest{
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
			if err := m.bootstrap(ctx); err != nil {
				return err
			}
		}
		token = metawire.RootTailTokenFromProto(resp.GetToken())
		if after != nil {
			*after = token
		}
		for _, record := range resp.GetEvents() {
			m.applyRootEvent(metawire.RootEventFromProto(record.GetEvent()))
		}
	}
}

func (m *Mirror) applyRootEvent(event rootevent.Event) {
	switch event.Kind {
	case rootevent.KindPerasAuthorityGranted, rootevent.KindPerasAuthorityRetired:
		if err := m.table.ApplyRootEvent(event); err != nil {
			log.Printf("peras authority mirror apply event kind=%d: %v", event.Kind, err)
		}
	}
}

func (m *Mirror) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-m.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (m *Mirror) Close() error {
	if m == nil {
		return nil
	}
	m.once.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
		close(m.stop)
	})
	<-m.done
	return nil
}
