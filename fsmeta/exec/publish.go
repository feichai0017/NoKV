package exec

import (
	"context"
	"errors"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// rootPublisher implements both fsmeta.SnapshotPublisher and the
// SubtreeHandoffPublisher interface using the coordinator PublishRootEvent RPC.
type rootPublisher struct {
	coord *coordclient.GRPCClient
}

func (p rootPublisher) PublishSnapshotSubtree(ctx context.Context, t fsmeta.SnapshotSubtreeToken) error {
	return p.send(ctx, rootevent.SnapshotEpochPublished(string(t.Mount), uint64(t.RootInode), t.ReadVersion))
}

func (p rootPublisher) RetireSnapshotSubtree(ctx context.Context, t fsmeta.SnapshotSubtreeToken) error {
	return p.send(ctx, rootevent.SnapshotEpochRetired(string(t.Mount), uint64(t.RootInode), t.ReadVersion))
}

func (p rootPublisher) StartSubtreeHandoff(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	return p.send(ctx, rootevent.SubtreeHandoffStarted(string(mount), uint64(root), frontier))
}

func (p rootPublisher) CompleteSubtreeHandoff(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	return p.send(ctx, rootevent.SubtreeHandoffCompleted(string(mount), uint64(root), frontier))
}

func (p rootPublisher) send(ctx context.Context, event rootevent.Event) error {
	if p.coord == nil {
		return errors.New("root publisher is not configured")
	}
	resp, err := p.coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return errors.New("root event was not accepted")
	}
	return nil
}
