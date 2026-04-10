package store

import (
	"bytes"
	"context"
	"fmt"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

func (s *Store) validateCommandClass(class AdmissionClass, req *raftcmdpb.RaftCmdRequest) (*peer.Peer, localmeta.RegionMeta, *raftcmdpb.RaftCmdResponse, error) {
	if s == nil {
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{Class: class, Reason: AdmissionReasonInvalid, Detail: "store is nil"})
		}
		return nil, localmeta.RegionMeta{}, nil, fmt.Errorf("raftstore: store is nil")
	}
	if req == nil {
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{Class: class, Reason: AdmissionReasonInvalid, Detail: "command is nil"})
		}
		return nil, localmeta.RegionMeta{}, nil, fmt.Errorf("raftstore: command is nil")
	}
	if req.Header == nil {
		req.Header = &raftcmdpb.CmdHeader{}
	}
	regionID := req.Header.GetRegionId()
	if regionID == 0 {
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    AdmissionReasonInvalid,
				RequestID: req.Header.GetRequestId(),
				Detail:    "region id missing",
			})
		}
		return nil, localmeta.RegionMeta{}, nil, fmt.Errorf("raftstore: region id missing")
	}
	if requestStoreID := req.Header.GetStoreId(); requestStoreID != 0 && s.storeID != 0 && requestStoreID != s.storeID {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.Header, RegionError: storeNotMatchError(requestStoreID, s.storeID)}
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    AdmissionReasonStoreNotMatch,
				RegionID:  regionID,
				RequestID: req.Header.GetRequestId(),
				Detail:    fmt.Sprintf("request store %d != local store %d", requestStoreID, s.storeID),
			})
		}
		return nil, localmeta.RegionMeta{}, resp, nil
	}
	meta, ok := s.RegionMetaByID(regionID)
	if !ok {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.Header, RegionError: regionNotFoundError(regionID)}
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    AdmissionReasonNotHosted,
				RegionID:  regionID,
				RequestID: req.Header.GetRequestId(),
				Detail:    "region metadata not found",
			})
		}
		return nil, localmeta.RegionMeta{}, resp, nil
	}
	if err := validateRegionEpoch(req.Header.GetRegionEpoch(), meta); err != nil {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.Header, RegionError: err}
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    AdmissionReasonEpochMismatch,
				RegionID:  regionID,
				RequestID: req.Header.GetRequestId(),
				Detail:    "region epoch mismatch",
			})
		}
		return nil, meta, resp, nil
	}
	if err, reason := validateRequestKeys(meta, req); err != nil {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.Header, RegionError: err}
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    reason,
				RegionID:  regionID,
				RequestID: req.Header.GetRequestId(),
				Detail:    "request keys failed local validation",
			})
		}
		return nil, meta, resp, nil
	}
	peer := s.regionMgr().peer(regionID)
	if peer == nil {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.Header, RegionError: regionNotFoundError(regionID)}
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    AdmissionReasonNotHosted,
				RegionID:  regionID,
				RequestID: req.Header.GetRequestId(),
				Detail:    "region peer not hosted",
			})
		}
		return nil, meta, resp, nil
	}
	status := peer.Status()
	if status.RaftState != myraft.StateLeader {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.Header, RegionError: notLeaderError(meta, status.Lead)}
		if class != AdmissionClassUnknown {
			s.recordAdmission(Admission{
				Class:     class,
				Reason:    AdmissionReasonNotLeader,
				RegionID:  regionID,
				PeerID:    peer.ID(),
				RequestID: req.Header.GetRequestId(),
				Detail:    "local peer is not leader",
			})
		}
		return nil, meta, resp, nil
	}
	req.Header.PeerId = peer.ID()
	return peer, meta, nil, nil
}

// ProposeCommand submits a raft command to the leader hosting the target
// region. When the store is not leader or the request header is invalid the
// returned response includes an appropriate RegionError.
func (s *Store) ProposeCommand(ctx context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	peer, _, resp, err := s.validateCommandClass(AdmissionClassWrite, req)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	if req.Header.RequestId == 0 {
		req.Header.RequestId = s.cmds.pipe.nextProposalID()
	}
	if ctx == nil {
		ctx = s.runtimeContext()
	}
	if err := ctx.Err(); err != nil {
		s.recordAdmission(Admission{
			Class:     AdmissionClassWrite,
			Reason:    classifyContextAdmission(err),
			RegionID:  req.Header.GetRegionId(),
			PeerID:    peer.ID(),
			RequestID: req.Header.GetRequestId(),
			Detail:    "write request context already done before local admission",
		})
		return nil, fmt.Errorf("raftstore: command %d context unavailable: %w", req.Header.GetRequestId(), err)
	}
	id := req.Header.RequestId
	prop, err := s.cmds.pipe.registerProposal(id)
	if err != nil {
		s.recordAdmission(Admission{
			Class:     AdmissionClassWrite,
			Reason:    AdmissionReasonInvalid,
			RegionID:  req.Header.GetRegionId(),
			PeerID:    peer.ID(),
			RequestID: id,
			Detail:    err.Error(),
		})
		return nil, err
	}
	if prop == nil {
		s.recordAdmission(Admission{
			Class:     AdmissionClassWrite,
			Reason:    AdmissionReasonInvalid,
			RegionID:  req.Header.GetRegionId(),
			PeerID:    peer.ID(),
			RequestID: id,
			Detail:    "command pipeline unavailable",
		})
		return nil, fmt.Errorf("raftstore: command pipeline unavailable")
	}
	if err := s.router.SendCommand(peer.ID(), req); err != nil {
		s.cmds.pipe.removeProposal(id)
		s.recordAdmission(Admission{
			Class:     AdmissionClassWrite,
			Reason:    AdmissionReasonUnknown,
			RegionID:  req.Header.GetRegionId(),
			PeerID:    peer.ID(),
			RequestID: id,
			Detail:    err.Error(),
		})
		return nil, err
	}
	s.recordAdmission(Admission{
		Class:     AdmissionClassWrite,
		Reason:    AdmissionReasonAccepted,
		Accepted:  true,
		RegionID:  req.Header.GetRegionId(),
		PeerID:    peer.ID(),
		RequestID: id,
	})
	timeout := time.Duration(0)
	if s != nil && s.cmds != nil {
		timeout = s.cmds.timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	select {
	case result := <-prop.ch:
		if result.err != nil {
			return nil, result.err
		}
		if result.resp == nil {
			return &raftcmdpb.RaftCmdResponse{Header: req.Header}, nil
		}
		return result.resp, nil
	case <-ctx.Done():
		s.cmds.pipe.removeProposal(id)
		return nil, fmt.Errorf("raftstore: command %d failed while waiting: %w", id, ctx.Err())
	}
}

// ReadCommand executes the provided read-only raft command locally on the
// leader. The command must only include read operations (Get/Scan). The method
// returns a RegionError when the store is not leader for the target region.
func (s *Store) ReadCommand(ctx context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	peer, meta, regionResp, err := s.validateCommandClass(AdmissionClassRead, req)
	if err != nil {
		return nil, err
	}
	if regionResp != nil {
		return regionResp, nil
	}
	if len(req.GetRequests()) == 0 {
		s.recordAdmission(Admission{Class: AdmissionClassRead, Reason: AdmissionReasonInvalid, RegionID: req.Header.GetRegionId(), Detail: "read command missing requests"})
		return nil, fmt.Errorf("raftstore: read command missing requests")
	}
	if !isReadOnlyRequest(req) {
		s.recordAdmission(Admission{Class: AdmissionClassRead, Reason: AdmissionReasonInvalid, RegionID: req.Header.GetRegionId(), Detail: "read command must be read-only"})
		return nil, fmt.Errorf("raftstore: read command must be read-only")
	}
	if s == nil || s.cmds == nil || s.cmds.apply == nil {
		s.recordAdmission(Admission{Class: AdmissionClassRead, Reason: AdmissionReasonInvalid, RegionID: req.Header.GetRegionId(), Detail: "command apply without handler"})
		return nil, fmt.Errorf("raftstore: command apply without handler")
	}
	if req.Header == nil {
		req.Header = &raftcmdpb.CmdHeader{}
	}
	if s.cmds != nil && s.cmds.pipe != nil && req.Header.GetRequestId() == 0 {
		req.Header.RequestId = s.cmds.pipe.nextProposalID()
	}
	timeout := time.Duration(0)
	if s != nil && s.cmds != nil {
		timeout = s.cmds.timeout
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if ctx == nil {
		ctx = s.runtimeContext()
	}
	if err := ctx.Err(); err != nil {
		s.recordAdmission(Admission{
			Class:     AdmissionClassRead,
			Reason:    classifyContextAdmission(err),
			RegionID:  req.Header.GetRegionId(),
			PeerID:    peer.ID(),
			RequestID: req.Header.GetRequestId(),
			Detail:    "read request context already done before local admission",
		})
		return nil, fmt.Errorf("raftstore: read command %d context unavailable: %w", req.Header.GetRequestId(), err)
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	s.recordAdmission(Admission{
		Class:     AdmissionClassRead,
		Reason:    AdmissionReasonAccepted,
		Accepted:  true,
		RegionID:  req.Header.GetRegionId(),
		PeerID:    peer.ID(),
		RequestID: req.Header.GetRequestId(),
	})
	index, err := peer.LinearizableRead(ctx)
	if err != nil {
		return nil, err
	}
	if err := peer.WaitApplied(ctx, index); err != nil {
		return nil, err
	}
	out, err := s.cmds.apply(req)
	if err != nil {
		return nil, err
	}
	if out != nil {
		trimScanResponse(meta, req, out)
	}
	return out, nil
}

func isReadOnlyRequest(req *raftcmdpb.RaftCmdRequest) bool {
	if req == nil {
		return false
	}
	for _, r := range req.GetRequests() {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case raftcmdpb.CmdType_CMD_GET, raftcmdpb.CmdType_CMD_SCAN:
			continue
		default:
			return false
		}
	}
	return true
}

func validateRegionEpoch(reqEpoch *metapb.RegionEpoch, meta localmeta.RegionMeta) *errorpb.RegionError {
	if reqEpoch == nil {
		return epochNotMatchError(&meta)
	}
	if reqEpoch.GetConfVersion() != meta.Epoch.ConfVersion || reqEpoch.GetVersion() != meta.Epoch.Version {
		return epochNotMatchError(&meta)
	}
	return nil
}

func validateRequestKeys(meta localmeta.RegionMeta, req *raftcmdpb.RaftCmdRequest) (*errorpb.RegionError, AdmissionReason) {
	if req == nil {
		return nil, AdmissionReasonUnknown
	}
	for _, r := range req.GetRequests() {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case raftcmdpb.CmdType_CMD_GET:
			key := r.GetGet().GetKey()
			if len(key) > 0 && !keyInRange(meta, key) {
				return keyNotInRegionError(meta, key), AdmissionReasonKeyNotInRegion
			}
		case raftcmdpb.CmdType_CMD_SCAN:
			start := r.GetScan().GetStartKey()
			if len(start) > 0 && !keyInRange(meta, start) {
				return keyNotInRegionError(meta, start), AdmissionReasonKeyNotInRegion
			}
		case raftcmdpb.CmdType_CMD_PREWRITE:
			for _, mut := range r.GetPrewrite().GetMutations() {
				if mut == nil {
					continue
				}
				key := mut.GetKey()
				if len(key) > 0 && !keyInRange(meta, key) {
					return keyNotInRegionError(meta, key), AdmissionReasonKeyNotInRegion
				}
			}
		case raftcmdpb.CmdType_CMD_COMMIT:
			for _, key := range r.GetCommit().GetKeys() {
				if len(key) > 0 && !keyInRange(meta, key) {
					return keyNotInRegionError(meta, key), AdmissionReasonKeyNotInRegion
				}
			}
		case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
			for _, key := range r.GetBatchRollback().GetKeys() {
				if len(key) > 0 && !keyInRange(meta, key) {
					return keyNotInRegionError(meta, key), AdmissionReasonKeyNotInRegion
				}
			}
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			for _, key := range r.GetResolveLock().GetKeys() {
				if len(key) > 0 && !keyInRange(meta, key) {
					return keyNotInRegionError(meta, key), AdmissionReasonKeyNotInRegion
				}
			}
		case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
			key := r.GetCheckTxnStatus().GetPrimaryKey()
			if len(key) > 0 && !keyInRange(meta, key) {
				return keyNotInRegionError(meta, key), AdmissionReasonKeyNotInRegion
			}
		default:
			return epochNotMatchError(&meta), AdmissionReasonInvalid
		}
	}
	return nil, AdmissionReasonUnknown
}

func keyInRange(meta localmeta.RegionMeta, key []byte) bool {
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

func trimScanResponse(meta localmeta.RegionMeta, req *raftcmdpb.RaftCmdRequest, resp *raftcmdpb.RaftCmdResponse) {
	if req == nil || resp == nil {
		return
	}
	if len(resp.Responses) == 0 {
		return
	}
	requests := req.GetRequests()
	for i, r := range requests {
		if r == nil || r.GetCmdType() != raftcmdpb.CmdType_CMD_SCAN {
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

func epochNotMatchError(meta *localmeta.RegionMeta) *errorpb.RegionError {
	var current *metapb.RegionEpoch
	var regions []*metapb.RegionDescriptor
	if meta != nil {
		current = &metapb.RegionEpoch{
			ConfVersion: meta.Epoch.ConfVersion,
			Version:     meta.Epoch.Version,
		}
		regions = append(regions, metacodec.LocalRegionMetaToDescriptorProto(*meta))
	}
	return &errorpb.RegionError{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentEpoch: current,
			Regions:      regions,
		},
	}
}

func notLeaderError(meta localmeta.RegionMeta, leaderPeerID uint64) *errorpb.RegionError {
	var leader *metapb.RegionPeer
	if leaderPeerID != 0 {
		for _, p := range meta.Peers {
			if p.PeerID == leaderPeerID {
				leader = &metapb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID}
				break
			}
		}
	}
	return &errorpb.RegionError{
		NotLeader: &errorpb.NotLeader{
			RegionId: meta.ID,
			Leader:   leader,
		},
	}
}

func storeNotMatchError(requestStoreID, actualStoreID uint64) *errorpb.RegionError {
	return &errorpb.RegionError{
		StoreNotMatch: &errorpb.StoreNotMatch{
			RequestStoreId: requestStoreID,
			ActualStoreId:  actualStoreID,
		},
	}
}

func regionNotFoundError(regionID uint64) *errorpb.RegionError {
	return &errorpb.RegionError{
		RegionNotFound: &errorpb.RegionNotFound{RegionId: regionID},
	}
}

func keyNotInRegionError(meta localmeta.RegionMeta, key []byte) *errorpb.RegionError {
	return &errorpb.RegionError{
		KeyNotInRegion: &errorpb.KeyNotInRegion{
			Key:      append([]byte(nil), key...),
			RegionId: meta.ID,
			StartKey: append([]byte(nil), meta.StartKey...),
			EndKey:   append([]byte(nil), meta.EndKey...),
		},
	}
}
