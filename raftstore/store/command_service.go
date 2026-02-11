package store

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

func (s *Store) validateCommand(req *pb.RaftCmdRequest) (*peer.Peer, manifest.RegionMeta, *pb.RaftCmdResponse, error) {
	if s == nil {
		return nil, manifest.RegionMeta{}, nil, fmt.Errorf("raftstore: store is nil")
	}
	if req == nil {
		return nil, manifest.RegionMeta{}, nil, fmt.Errorf("raftstore: command is nil")
	}
	if req.Header == nil {
		req.Header = &pb.CmdHeader{}
	}
	regionID := req.Header.GetRegionId()
	if regionID == 0 {
		return nil, manifest.RegionMeta{}, nil, fmt.Errorf("raftstore: region id missing")
	}
	meta, ok := s.RegionMetaByID(regionID)
	if !ok {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: epochNotMatchError(nil)}
		return nil, manifest.RegionMeta{}, resp, nil
	}
	if err := validateRegionEpoch(req.Header.GetRegionEpoch(), meta); err != nil {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: err}
		return nil, meta, resp, nil
	}
	if err := validateRequestKeys(meta, req); err != nil {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: err}
		return nil, meta, resp, nil
	}
	peer := s.regions.peer(regionID)
	if peer == nil {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: epochNotMatchError(&meta)}
		return nil, meta, resp, nil
	}
	status := peer.Status()
	if status.RaftState != myraft.StateLeader {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: notLeaderError(meta, status.Lead)}
		return nil, meta, resp, nil
	}
	req.Header.PeerId = peer.ID()
	return peer, meta, nil, nil
}

// ProposeCommand submits a raft command to the leader hosting the target
// region. When the store is not leader or the request header is invalid the
// returned response includes an appropriate RegionError.
func (s *Store) ProposeCommand(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	peer, _, resp, err := s.validateCommand(req)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	if req.Header.RequestId == 0 {
		req.Header.RequestId = s.command.nextProposalID()
	}
	id := req.Header.RequestId
	prop, err := s.command.registerProposal(id)
	if err != nil {
		return nil, err
	}
	if prop == nil {
		return nil, fmt.Errorf("raftstore: command pipeline unavailable")
	}
	if err := s.router.SendCommand(peer.ID(), req); err != nil {
		s.command.removeProposal(id)
		return nil, err
	}
	timer := time.NewTimer(s.commandTimeout)
	defer timer.Stop()
	select {
	case result := <-prop.ch:
		if result.err != nil {
			return nil, result.err
		}
		if result.resp == nil {
			return &pb.RaftCmdResponse{Header: req.Header}, nil
		}
		return result.resp, nil
	case <-timer.C:
		s.command.removeProposal(id)
		return nil, fmt.Errorf("raftstore: command %d timed out", id)
	}
}

// ReadCommand executes the provided read-only raft command locally on the
// leader. The command must only include read operations (Get/Scan). The method
// returns a RegionError when the store is not leader for the target region.
func (s *Store) ReadCommand(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	peer, meta, regionResp, err := s.validateCommand(req)
	if err != nil {
		return nil, err
	}
	if regionResp != nil {
		return regionResp, nil
	}
	if len(req.GetRequests()) == 0 {
		return nil, fmt.Errorf("raftstore: read command missing requests")
	}
	if !isReadOnlyRequest(req) {
		return nil, fmt.Errorf("raftstore: read command must be read-only")
	}
	if s.commandApplier == nil {
		return nil, fmt.Errorf("raftstore: command apply without handler")
	}
	if req.Header == nil {
		req.Header = &pb.CmdHeader{}
	}
	if s.command != nil && req.Header.GetRequestId() == 0 {
		req.Header.RequestId = s.command.nextProposalID()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	index, err := peer.LinearizableRead(ctx)
	if err != nil {
		return nil, err
	}
	if err := peer.WaitApplied(ctx, index); err != nil {
		return nil, err
	}
	out, err := s.commandApplier(req)
	if err != nil {
		return nil, err
	}
	if out != nil {
		trimScanResponse(meta, req, out)
	}
	return out, nil
}

func isReadOnlyRequest(req *pb.RaftCmdRequest) bool {
	if req == nil {
		return false
	}
	for _, r := range req.GetRequests() {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case pb.CmdType_CMD_GET, pb.CmdType_CMD_SCAN:
			continue
		default:
			return false
		}
	}
	return true
}

func validateRegionEpoch(reqEpoch *pb.RegionEpoch, meta manifest.RegionMeta) *pb.RegionError {
	if reqEpoch == nil {
		return epochNotMatchError(&meta)
	}
	if reqEpoch.GetConfVer() != meta.Epoch.ConfVersion || reqEpoch.GetVersion() != meta.Epoch.Version {
		return epochNotMatchError(&meta)
	}
	return nil
}

func validateRequestKeys(meta manifest.RegionMeta, req *pb.RaftCmdRequest) *pb.RegionError {
	if req == nil {
		return nil
	}
	for _, r := range req.GetRequests() {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case pb.CmdType_CMD_GET:
			key := r.GetGet().GetKey()
			if len(key) > 0 && !keyInRange(meta, key) {
				return epochNotMatchError(&meta)
			}
		case pb.CmdType_CMD_SCAN:
			start := r.GetScan().GetStartKey()
			if len(start) > 0 && !keyInRange(meta, start) {
				return epochNotMatchError(&meta)
			}
		case pb.CmdType_CMD_PREWRITE:
			for _, mut := range r.GetPrewrite().GetMutations() {
				if mut == nil {
					continue
				}
				key := mut.GetKey()
				if len(key) > 0 && !keyInRange(meta, key) {
					return epochNotMatchError(&meta)
				}
			}
		case pb.CmdType_CMD_COMMIT:
			for _, key := range r.GetCommit().GetKeys() {
				if len(key) > 0 && !keyInRange(meta, key) {
					return epochNotMatchError(&meta)
				}
			}
		case pb.CmdType_CMD_BATCH_ROLLBACK:
			for _, key := range r.GetBatchRollback().GetKeys() {
				if len(key) > 0 && !keyInRange(meta, key) {
					return epochNotMatchError(&meta)
				}
			}
		case pb.CmdType_CMD_RESOLVE_LOCK:
			for _, key := range r.GetResolveLock().GetKeys() {
				if len(key) > 0 && !keyInRange(meta, key) {
					return epochNotMatchError(&meta)
				}
			}
		case pb.CmdType_CMD_CHECK_TXN_STATUS:
			key := r.GetCheckTxnStatus().GetPrimaryKey()
			if len(key) > 0 && !keyInRange(meta, key) {
				return epochNotMatchError(&meta)
			}
		default:
			return epochNotMatchError(&meta)
		}
	}
	return nil
}

func keyInRange(meta manifest.RegionMeta, key []byte) bool {
	if len(key) == 0 {
		return true
	}
	if len(meta.StartKey) > 0 && bytes.Compare(key, meta.StartKey) < 0 {
		return false
	}
	if len(meta.EndKey) > 0 && bytes.Compare(key, meta.EndKey) >= 0 {
		return false
	}
	return true
}

func trimScanResponse(meta manifest.RegionMeta, req *pb.RaftCmdRequest, resp *pb.RaftCmdResponse) {
	if req == nil || resp == nil {
		return
	}
	if len(resp.Responses) == 0 {
		return
	}
	requests := req.GetRequests()
	for i, r := range requests {
		if r == nil || r.GetCmdType() != pb.CmdType_CMD_SCAN {
			continue
		}
		if i >= len(resp.Responses) {
			return
		}
		out := resp.Responses[i]
		if out == nil {
			continue
		}
		scan := out.GetScan()
		if scan == nil || len(scan.Kvs) == 0 {
			continue
		}
		kept := scan.Kvs[:0]
		for _, kv := range scan.Kvs {
			if kv == nil {
				continue
			}
			if keyInRange(meta, kv.Key) {
				kept = append(kept, kv)
			}
		}
		scan.Kvs = kept
	}
}

func epochNotMatchError(meta *manifest.RegionMeta) *pb.RegionError {
	var current *pb.RegionEpoch
	var regions []*pb.RegionMeta
	if meta != nil {
		current = &pb.RegionEpoch{
			ConfVer: meta.Epoch.ConfVersion,
			Version: meta.Epoch.Version,
		}
		regions = append(regions, regionMetaToPB(*meta))
	}
	return &pb.RegionError{
		EpochNotMatch: &pb.EpochNotMatch{
			CurrentEpoch: current,
			Regions:      regions,
		},
	}
}

func notLeaderError(meta manifest.RegionMeta, leaderPeerID uint64) *pb.RegionError {
	var leader *pb.RegionPeer
	if leaderPeerID != 0 {
		for _, p := range meta.Peers {
			if p.PeerID == leaderPeerID {
				leader = &pb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID}
				break
			}
		}
	}
	return &pb.RegionError{
		NotLeader: &pb.NotLeader{
			RegionId: meta.ID,
			Leader:   leader,
		},
	}
}
