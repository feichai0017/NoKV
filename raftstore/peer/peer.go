package peer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/utils"
	raftpb "go.etcd.io/raft/v3/raftpb"
	proto "google.golang.org/protobuf/proto"
)

// ApplyFunc consumes committed raft log entries and applies them to the user
// state machine (LSM, MVCC, etc).
type ApplyFunc func(entries []myraft.Entry) error

// AdminApplyFunc consumes admin commands (split, merge, etc.).
type AdminApplyFunc func(cmd *pb.AdminCommand) error

// Peer wraps a RawNode with simple storage and apply plumbing.
type Peer struct {
	mu               sync.Mutex
	readyMu          sync.Mutex
	id               uint64
	node             *myraft.RawNode
	storage          engine.PeerStorage
	transport        transport.Transport
	apply            ApplyFunc
	adminApply       AdminApplyFunc
	raftLog          *raftLogTracker
	snapshotQueue    *snapshotResendQueue
	logRetainEntries uint64
	confChangeHook   ConfChangeHandler
	applyMark        *utils.WaterMark
	applyCloser      *utils.Closer
	applyLimit       uint64
	stopCtx          context.Context
	stopCancel       context.CancelFunc
	region           *manifest.RegionMeta
	readSeq          atomic.Uint64
	readMu           sync.Mutex
	pendingReads     map[string]chan uint64
}

const defaultMaxInFlightApply = 8192

var errPeerStopped = errors.New("raftstore: peer stopped")

const adminCommandPrefix byte = 0xAD

func isAdminEntry(data []byte) bool {
	return len(data) > 0 && data[0] == adminCommandPrefix
}

func decodeAdminCommand(data []byte) (*pb.AdminCommand, error) {
	if len(data) <= 1 {
		return nil, fmt.Errorf("raftstore: admin command payload too short")
	}
	var cmd pb.AdminCommand
	if err := proto.Unmarshal(data[1:], &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// NewPeer constructs a peer using the provided configuration. The caller must
// register the peer with the transport before invoking Bootstrap.
func NewPeer(cfg *Config) (*Peer, error) {
	if cfg == nil {
		return nil, errors.New("raftstore: config is nil")
	}
	if cfg.Transport == nil {
		return nil, errors.New("raftstore: transport must be provided")
	}
	if cfg.Apply == nil {
		return nil, errors.New("raftstore: apply function must be provided")
	}
	storage, err := ResolveStorage(cfg)
	if err != nil {
		return nil, err
	}
	raftCfg := cfg.RaftConfig
	if raftCfg.ID == 0 {
		return nil, errors.New("raftstore: raft config must specify ID")
	}
	raftCfg.Storage = storage
	node, err := myraft.NewRawNode(&raftCfg)
	if err != nil {
		return nil, err
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	peer := &Peer{
		id:               raftCfg.ID,
		node:             node,
		storage:          storage,
		transport:        cfg.Transport,
		apply:            cfg.Apply,
		adminApply:       cfg.AdminApply,
		confChangeHook:   cfg.ConfChange,
		raftLog:          newRaftLogTracker(cfg.Manifest, cfg.WAL, nonZeroGroupID(cfg.GroupID)),
		snapshotQueue:    newSnapshotResendQueue(),
		logRetainEntries: cfg.LogRetainEntries,
		applyCloser:      utils.NewCloserInitial(1),
		stopCtx:          stopCtx,
		stopCancel:       stopCancel,
		region:           manifest.CloneRegionMetaPtr(cfg.Region),
		pendingReads:     make(map[string]chan uint64),
	}
	if peer.logRetainEntries == 0 {
		peer.logRetainEntries = defaultLogRetainEntries
	}
	peer.applyMark = &utils.WaterMark{Name: fmt.Sprintf("raft.%d.apply", peer.id)}
	peer.applyMark.Init(peer.applyCloser)
	if cfg.MaxInFlightApply > 0 {
		peer.applyLimit = cfg.MaxInFlightApply
	} else {
		peer.applyLimit = defaultMaxInFlightApply
	}
	return peer, nil
}

// ID returns the peer ID.
func (p *Peer) ID() uint64 {
	return p.id
}

// RegionMeta returns a clone of the region metadata associated with this
// peer. It mirrors TinyKV's approach of surfacing region layout through the
// store for schedulers and debugging endpoints.
func (p *Peer) RegionMeta() *manifest.RegionMeta {
	if p == nil || p.region == nil {
		return nil
	}
	return manifest.CloneRegionMetaPtr(p.region)
}

// SetRegionMeta replaces the in-memory region metadata with the provided
// snapshot. It mirrors TinyKV's behaviour where raftstore updates peer state
// based on scheduler decisions (splits, epoch bumps, membership changes).
func (p *Peer) SetRegionMeta(meta manifest.RegionMeta) {
	if p == nil {
		return
	}
	cp := manifest.CloneRegionMetaPtr(&meta)
	p.mu.Lock()
	p.region = cp
	p.mu.Unlock()
}

// Bootstrap injects the initial configuration into the node. It must be called
// after the peer has been registered with the transport.
func (p *Peer) Bootstrap(peers []myraft.Peer) error {
	if len(peers) == 0 {
		return nil
	}
	last, err := p.storage.LastIndex()
	if err == nil && last > 0 {
		return nil
	}
	if hs, cs, err := p.storage.InitialState(); err == nil {
		if !myraft.IsEmptyHardState(hs) || len(cs.Voters) > 0 || len(cs.Learners) > 0 {
			return nil
		}
	}
	p.mu.Lock()
	err = p.node.Bootstrap(peers)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Tick increments the logical clock, driving elections and heartbeats.
func (p *Peer) Tick() error {
	p.mu.Lock()
	p.node.Tick()
	p.mu.Unlock()
	p.resendPendingSnapshots()
	return p.processReady()
}

// Step forwards a received raft message to the underlying node.
func (p *Peer) Step(msg myraft.Message) error {
	if msg.Type == myraft.MsgSnapshotStatus && !msg.Reject {
		if q := p.snapshotQueue; q != nil {
			q.drop(msg.From)
		}
	}
	p.mu.Lock()
	err := p.node.Step(msg)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Propose submits application data to the raft log.
func (p *Peer) Propose(data []byte) error {
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	err := p.node.Propose(data)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// ProposeCommand encodes the provided raft command request and submits it to
// the raft log.
func (p *Peer) ProposeCommand(req *pb.RaftCmdRequest) error {
	payload, err := command.Encode(req)
	if err != nil {
		return err
	}
	return p.Propose(payload)
}

// ProposeAdmin submits an admin command encoded as pb.AdminCommand payload.
func (p *Peer) ProposeAdmin(cmdData []byte) error {
	if len(cmdData) == 0 {
		return fmt.Errorf("raftstore: empty admin command")
	}
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	payload := append([]byte{adminCommandPrefix}, cmdData...)
	p.mu.Lock()
	err := p.node.Propose(payload)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// ProposeConfChange submits a configuration change entry to the raft log.
func (p *Peer) ProposeConfChange(cc raftpb.ConfChangeV2) error {
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	err := p.node.ProposeConfChange(cc)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// TransferLeader requests leadership transfer to the provided peer ID.
func (p *Peer) TransferLeader(target uint64) error {
	if target == 0 {
		return fmt.Errorf("raftstore: transfer target must be non-zero")
	}
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	p.node.TransferLeader(target)
	p.mu.Unlock()
	return p.processReady()
}

// Campaign transitions this peer into candidate state.
func (p *Peer) Campaign() error {
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	err := p.node.Campaign()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Flush forces processing of any pending Ready state.
func (p *Peer) Flush() error {
	return p.processReady()
}

// Status returns the raft status snapshot.
func (p *Peer) Status() myraft.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.node.Status()
}

func (p *Peer) processReady() error {
	p.readyMu.Lock()
	for {
		p.mu.Lock()
		hasReady := p.node.HasReady()
		var rd myraft.Ready
		if hasReady {
			rd = p.node.Ready()
		}
		p.mu.Unlock()

		if !hasReady {
			p.readyMu.Unlock()
			return nil
		}

		msgs := rd.Messages
		if err := p.handleReady(rd); err != nil {
			p.readyMu.Unlock()
			return err
		}

		p.mu.Lock()
		p.node.Advance(rd)
		p.mu.Unlock()

		p.readyMu.Unlock()
		p.sendMessages(msgs)
		p.readyMu.Lock()
	}
}

func (p *Peer) handleReady(rd myraft.Ready) error {
	if info := p.raftLog; info != nil {
		info.setInjected(failpoints.ShouldFailBeforeStorage())
	}

	if !myraft.IsEmptyHardState(rd.HardState) {
		if err := p.raftLog.injectFailure("before_hard_state"); err != nil {
			return err
		}
		if err := p.storage.SetHardState(rd.HardState); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			info.capturePointer(manifest.RaftLogPointer{
				GroupID:      info.groupID,
				AppliedIndex: rd.HardState.Commit,
				AppliedTerm:  rd.HardState.Term,
			})
		}
	}
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if err := p.raftLog.injectFailure("before_snapshot"); err != nil {
			return err
		}
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			meta := rd.Snapshot.Metadata
			info.capturePointer(manifest.RaftLogPointer{
				GroupID:       info.groupID,
				SnapshotIndex: meta.Index,
				SnapshotTerm:  meta.Term,
			})
		}
		if meta := rd.Snapshot.Metadata; meta.Index > 0 {
			p.markSnapshotApplied(meta.Index)
		}
	}
	if len(rd.Entries) > 0 {
		if err := p.raftLog.injectFailure("before_entries"); err != nil {
			return err
		}
		if err := p.storage.Append(rd.Entries); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			last := rd.Entries[len(rd.Entries)-1]
			info.capturePointer(manifest.RaftLogPointer{
				GroupID:      info.groupID,
				AppliedIndex: last.Index,
				AppliedTerm:  last.Term,
			})
		}
	}
	if len(rd.ReadStates) > 0 {
		p.handleReadStates(rd.ReadStates)
	}
	if len(rd.CommittedEntries) > 0 {
		p.beginApply(rd.CommittedEntries)
		var toApply []myraft.Entry
		for _, entry := range rd.CommittedEntries {
			switch entry.Type {
			case myraft.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				ccV2 := cc.AsV2()
				p.node.ApplyConfChange(ccV2)
				if err := p.handleConfChange(ccV2, entry); err != nil {
					return err
				}
			case myraft.EntryConfChangeV2:
				var cc raftpb.ConfChangeV2
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				p.node.ApplyConfChange(cc)
				if err := p.handleConfChange(cc, entry); err != nil {
					return err
				}
			default:
				if len(entry.Data) == 0 {
					continue
				}
				if isAdminEntry(entry.Data) {
					cmd, err := decodeAdminCommand(entry.Data)
					if err != nil {
						return err
					}
					if err := p.applyAdminCommand(cmd); err != nil {
						return err
					}
					continue
				}
				toApply = append(toApply, entry)
			}
		}
		if len(toApply) > 0 && p.apply != nil {
			if err := p.apply(toApply); err != nil {
				p.finishApply(rd.CommittedEntries)
				return err
			}
		}
		p.finishApply(rd.CommittedEntries)
		lastApplied := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		if err := p.maybeCompact(lastApplied); err != nil {
			return err
		}
	}
	return nil
}

func (p *Peer) sendMessages(msgs []myraft.Message) {
	if p == nil || len(msgs) == 0 || p.transport == nil {
		return
	}
	for _, msg := range msgs {
		if msg.Type == myraft.MsgSnapshot {
			if q := p.snapshotQueue; q != nil {
				q.record(msg)
			}
		}
		p.transport.Send(msg)
	}
}

func (p *Peer) handleConfChange(cc raftpb.ConfChangeV2, entry raftpb.Entry) error {
	if p == nil || p.confChangeHook == nil {
		return nil
	}
	event := ConfChangeEvent{
		Peer:       p,
		RegionMeta: p.RegionMeta(),
		ConfChange: cc,
		Index:      entry.Index,
		Term:       entry.Term,
	}
	return p.confChangeHook(event)
}

func (p *Peer) maybeCompact(applied uint64) error {
	if applied == 0 {
		return nil
	}
	ws, ok := p.storage.(*engine.WALStorage)
	if !ok {
		return nil
	}
	return ws.MaybeCompact(applied, p.logRetainEntries)
}

func (p *Peer) applyAdminCommand(cmd *pb.AdminCommand) error {
	if p == nil || cmd == nil {
		return nil
	}
	if p.adminApply == nil {
		return nil
	}
	return p.adminApply(cmd)
}

// WaitApplied blocks until the provided raft log index has been fully applied.
func (p *Peer) WaitApplied(ctx context.Context, index uint64) error {
	if p == nil || p.applyMark == nil || index == 0 {
		return nil
	}
	return p.applyMark.WaitForMark(ctx, index)
}

// Close releases resources associated with the peer, including background
// watermark processors.
func (p *Peer) Close() error {
	if p == nil {
		return nil
	}
	p.stopCancel()
	if p.applyCloser != nil {
		p.applyCloser.SignalAndWait()
	}
	p.readMu.Lock()
	for key, ch := range p.pendingReads {
		if ch != nil {
			close(ch)
		}
		delete(p.pendingReads, key)
	}
	p.readMu.Unlock()
	return nil
}

func (p *Peer) waitForApplyBacklog() error {
	if p == nil || p.applyMark == nil {
		return nil
	}
	limit := p.applyLimit
	if limit == 0 {
		return nil
	}
	for {
		done := p.applyMark.DoneUntil()
		last := p.applyMark.LastIndex()
		if last <= done || last-done < limit {
			return nil
		}
		target := done + 1
		if err := p.applyMark.WaitForMark(p.stopCtx, target); err != nil {
			if errors.Is(err, context.Canceled) {
				return errPeerStopped
			}
			return err
		}
	}
}

func (p *Peer) beginApply(entries []myraft.Entry) {
	if p == nil || p.applyMark == nil || len(entries) == 0 {
		return
	}
	indices := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Index == 0 {
			continue
		}
		indices = append(indices, entry.Index)
	}
	if len(indices) == 0 {
		return
	}
	if len(indices) == 1 {
		p.applyMark.Begin(indices[0])
		return
	}
	p.applyMark.BeginMany(indices)
}

func (p *Peer) finishApply(entries []myraft.Entry) {
	if p == nil || p.applyMark == nil || len(entries) == 0 {
		return
	}
	indices := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Index == 0 {
			continue
		}
		indices = append(indices, entry.Index)
	}
	if len(indices) == 0 {
		return
	}
	if len(indices) == 1 {
		p.applyMark.Done(indices[0])
		return
	}
	p.applyMark.DoneMany(indices)
}

// LinearizableRead performs a raft ReadIndex round-trip to ensure the peer
// still holds leadership and returns the corresponding log index. Callers
// should subsequently wait for that index to be applied before reading state.
func (p *Peer) LinearizableRead(ctx context.Context) (uint64, error) {
	if p == nil {
		return 0, fmt.Errorf("raftstore: peer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-p.stopCtx.Done():
		return 0, errPeerStopped
	default:
	}
	key, ch := p.startReadIndex()
	if err := p.Flush(); err != nil {
		p.cancelReadIndex(key)
		return 0, err
	}
	select {
	case idx, ok := <-ch:
		if !ok {
			return 0, errPeerStopped
		}
		return idx, nil
	case <-ctx.Done():
		p.cancelReadIndex(key)
		return 0, ctx.Err()
	case <-p.stopCtx.Done():
		p.cancelReadIndex(key)
		return 0, errPeerStopped
	}
}

func (p *Peer) startReadIndex() (string, chan uint64) {
	reqCtx := make([]byte, 16)
	binary.BigEndian.PutUint64(reqCtx[:8], p.id)
	seq := p.readSeq.Add(1)
	binary.BigEndian.PutUint64(reqCtx[8:], seq)
	key := string(reqCtx)
	ch := make(chan uint64, 1)

	p.readMu.Lock()
	p.pendingReads[key] = ch
	p.readMu.Unlock()

	p.mu.Lock()
	p.node.ReadIndex(reqCtx)
	p.mu.Unlock()
	return key, ch
}

func (p *Peer) cancelReadIndex(key string) {
	p.readMu.Lock()
	ch, ok := p.pendingReads[key]
	if ok {
		delete(p.pendingReads, key)
	}
	p.readMu.Unlock()
	if ok && ch != nil {
		close(ch)
	}
}

func (p *Peer) handleReadStates(states []myraft.ReadState) {
	if len(states) == 0 {
		return
	}
	for _, state := range states {
		if len(state.RequestCtx) == 0 {
			continue
		}
		key := string(state.RequestCtx)
		p.readMu.Lock()
		ch, ok := p.pendingReads[key]
		if ok {
			delete(p.pendingReads, key)
		}
		p.readMu.Unlock()
		if ok && ch != nil {
			ch <- state.Index
			close(ch)
		}
	}
}

func (p *Peer) markSnapshotApplied(index uint64) {
	if p == nil || p.applyMark == nil || index == 0 {
		return
	}
	// Snapshot application makes all indices <= snapshot index finished.
	p.applyMark.SetDoneUntil(index)
}

// PopPendingSnapshot returns the most recent snapshot recorded during Ready
// handling, clearing the queue. It returns false when no snapshot is pending.
func (p *Peer) PopPendingSnapshot() (myraft.Snapshot, bool) {
	if p == nil || p.snapshotQueue == nil {
		return myraft.Snapshot{}, false
	}
	msg, ok := p.snapshotQueue.first()
	if !ok || msg.Snapshot == nil || myraft.IsEmptySnap(*msg.Snapshot) {
		return myraft.Snapshot{}, false
	}
	p.snapshotQueue.drop(msg.To)
	return *msg.Snapshot, true
}

// PendingSnapshot returns the snapshot retained for resend without removing it
// from the queue.
func (p *Peer) PendingSnapshot() (myraft.Snapshot, bool) {
	if p == nil || p.snapshotQueue == nil {
		return myraft.Snapshot{}, false
	}
	msg, ok := p.snapshotQueue.first()
	if !ok || msg.Snapshot == nil {
		return myraft.Snapshot{}, false
	}
	return *msg.Snapshot, true
}

// ResendSnapshot attempts to resend the last snapshot destined for the provided
// peer ID. It returns true when a snapshot message was re-enqueued.
func (p *Peer) ResendSnapshot(to uint64) bool {
	if p == nil || p.transport == nil || p.snapshotQueue == nil || to == 0 {
		return false
	}
	msg, ok := p.snapshotQueue.pendingFor(to)
	if !ok {
		return false
	}
	p.transport.Send(msg)
	return true
}

func (p *Peer) resendPendingSnapshots() {
	if p == nil || p.transport == nil || p.snapshotQueue == nil {
		return
	}
	p.snapshotQueue.forEach(func(msg myraft.Message) {
		p.transport.Send(msg)
	})
}
