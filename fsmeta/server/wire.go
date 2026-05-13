package server

import (
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
)

func createRequestFromProto(req *fsmetapb.CreateRequest) fsmeta.CreateRequest {
	return fsmeta.CreateRequest{
		Mount:  fsmeta.MountID(req.GetMount()),
		Parent: fsmeta.InodeID(req.GetParent()),
		Name:   req.GetName(),
		Attrs:  createAttrsFromProto(req.GetAttrs()),
	}
}

func createAttrsFromProto(pb *fsmetapb.CreateInodeAttrs) fsmeta.CreateAttrs {
	if pb == nil {
		return fsmeta.CreateAttrs{}
	}
	return fsmeta.CreateAttrs{
		Type:          inodeTypeFromProto(pb.GetType()),
		Size:          pb.GetSize(),
		Mode:          pb.GetMode(),
		CreatedUnixNs: pb.GetCreatedUnixNs(),
		UpdatedUnixNs: pb.GetUpdatedUnixNs(),
		OpaqueAttrs:   append([]byte(nil), pb.GetOpaqueAttrs()...),
	}
}

func updateInodeRequestFromProto(req *fsmetapb.UpdateInodeRequest) fsmeta.UpdateInodeRequest {
	if req == nil {
		return fsmeta.UpdateInodeRequest{}
	}
	return fsmeta.UpdateInodeRequest{
		Mount:            fsmeta.MountID(req.GetMount()),
		Parent:           fsmeta.InodeID(req.GetParent()),
		Inode:            fsmeta.InodeID(req.GetInode()),
		Name:             req.GetName(),
		SetSize:          req.GetSetSize(),
		Size:             req.GetSize(),
		SetMode:          req.GetSetMode(),
		Mode:             req.GetMode(),
		SetUpdatedUnixNs: req.GetSetUpdatedUnixNs(),
		UpdatedUnixNs:    req.GetUpdatedUnixNs(),
		SetOpaqueAttrs:   req.GetSetOpaqueAttrs(),
		OpaqueAttrs:      append([]byte(nil), req.GetOpaqueAttrs()...),
	}
}

func lookupRequestFromProto(req *fsmetapb.LookupRequest) fsmeta.LookupRequest {
	return fsmeta.LookupRequest{
		Mount:  fsmeta.MountID(req.GetMount()),
		Parent: fsmeta.InodeID(req.GetParent()),
		Name:   req.GetName(),
	}
}

func readDirRequestFromProto(req *fsmetapb.ReadDirRequest) fsmeta.ReadDirRequest {
	return fsmeta.ReadDirRequest{
		Mount:           fsmeta.MountID(req.GetMount()),
		Parent:          fsmeta.InodeID(req.GetParent()),
		StartAfter:      req.GetStartAfter(),
		Limit:           req.GetLimit(),
		SnapshotVersion: req.GetSnapshotVersion(),
	}
}

func getReadVersionRequestFromProto(req *fsmetapb.GetReadVersionRequest) fsmeta.ReadVersionRequest {
	if req == nil {
		return fsmeta.ReadVersionRequest{}
	}
	return fsmeta.ReadVersionRequest{Mount: fsmeta.MountID(req.GetMount())}
}

func snapshotSubtreeRequestFromProto(req *fsmetapb.SnapshotSubtreeRequest) fsmeta.SnapshotSubtreeRequest {
	if req == nil {
		return fsmeta.SnapshotSubtreeRequest{}
	}
	return fsmeta.SnapshotSubtreeRequest{
		Mount:     fsmeta.MountID(req.GetMount()),
		RootInode: fsmeta.InodeID(req.GetRootInode()),
	}
}

func snapshotSubtreeResponseToProto(token fsmeta.SnapshotSubtreeToken) *fsmetapb.SnapshotSubtreeResponse {
	return &fsmetapb.SnapshotSubtreeResponse{
		Mount:       string(token.Mount),
		RootInode:   uint64(token.RootInode),
		ReadVersion: token.ReadVersion,
	}
}

func retireSnapshotSubtreeRequestFromProto(req *fsmetapb.RetireSnapshotSubtreeRequest) fsmeta.SnapshotSubtreeToken {
	if req == nil {
		return fsmeta.SnapshotSubtreeToken{}
	}
	return fsmeta.SnapshotSubtreeToken{
		Mount:       fsmeta.MountID(req.GetMount()),
		RootInode:   fsmeta.InodeID(req.GetRootInode()),
		ReadVersion: req.GetReadVersion(),
	}
}

func quotaUsageRequestFromProto(req *fsmetapb.QuotaUsageRequest) fsmeta.QuotaUsageRequest {
	if req == nil {
		return fsmeta.QuotaUsageRequest{}
	}
	return fsmeta.QuotaUsageRequest{
		Mount: fsmeta.MountID(req.GetMount()),
		Scope: fsmeta.InodeID(req.GetScope()),
	}
}

func quotaUsageResponseToProto(record fsmeta.UsageRecord) *fsmetapb.QuotaUsageResponse {
	return &fsmetapb.QuotaUsageResponse{
		Bytes:  record.Bytes,
		Inodes: record.Inodes,
	}
}

func renameRequestFromProto(req *fsmetapb.RenameRequest) fsmeta.RenameRequest {
	return fsmeta.RenameRequest{
		Mount:      fsmeta.MountID(req.GetMount()),
		FromParent: fsmeta.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   fsmeta.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func renameSubtreeRequestFromProto(req *fsmetapb.RenameSubtreeRequest) fsmeta.RenameSubtreeRequest {
	return fsmeta.RenameSubtreeRequest{
		Mount:      fsmeta.MountID(req.GetMount()),
		FromParent: fsmeta.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   fsmeta.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func linkRequestFromProto(req *fsmetapb.LinkRequest) fsmeta.LinkRequest {
	return fsmeta.LinkRequest{
		Mount:      fsmeta.MountID(req.GetMount()),
		FromParent: fsmeta.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   fsmeta.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func unlinkRequestFromProto(req *fsmetapb.UnlinkRequest) fsmeta.UnlinkRequest {
	return fsmeta.UnlinkRequest{
		Mount:  fsmeta.MountID(req.GetMount()),
		Parent: fsmeta.InodeID(req.GetParent()),
		Name:   req.GetName(),
	}
}

func openWriteSessionRequestFromProto(req *fsmetapb.OpenWriteSessionRequest) fsmeta.OpenWriteSessionRequest {
	if req == nil {
		return fsmeta.OpenWriteSessionRequest{}
	}
	return fsmeta.OpenWriteSessionRequest{
		Mount:   fsmeta.MountID(req.GetMount()),
		Inode:   fsmeta.InodeID(req.GetInode()),
		Session: fsmeta.SessionID(req.GetSession()),
		TTL:     time.Duration(req.GetTtlNs()),
	}
}

func heartbeatWriteSessionRequestFromProto(req *fsmetapb.HeartbeatWriteSessionRequest) fsmeta.HeartbeatWriteSessionRequest {
	if req == nil {
		return fsmeta.HeartbeatWriteSessionRequest{}
	}
	return fsmeta.HeartbeatWriteSessionRequest{
		Mount:   fsmeta.MountID(req.GetMount()),
		Inode:   fsmeta.InodeID(req.GetInode()),
		Session: fsmeta.SessionID(req.GetSession()),
		TTL:     time.Duration(req.GetTtlNs()),
	}
}

func closeWriteSessionRequestFromProto(req *fsmetapb.CloseWriteSessionRequest) fsmeta.CloseWriteSessionRequest {
	if req == nil {
		return fsmeta.CloseWriteSessionRequest{}
	}
	return fsmeta.CloseWriteSessionRequest{
		Mount:   fsmeta.MountID(req.GetMount()),
		Inode:   fsmeta.InodeID(req.GetInode()),
		Session: fsmeta.SessionID(req.GetSession()),
	}
}

func expireWriteSessionsRequestFromProto(req *fsmetapb.ExpireWriteSessionsRequest) fsmeta.ExpireWriteSessionsRequest {
	if req == nil {
		return fsmeta.ExpireWriteSessionsRequest{}
	}
	return fsmeta.ExpireWriteSessionsRequest{
		Mount: fsmeta.MountID(req.GetMount()),
		Limit: req.GetLimit(),
	}
}

func dentryToProto(record fsmeta.DentryRecord) *fsmetapb.DentryRecord {
	return &fsmetapb.DentryRecord{
		Parent: uint64(record.Parent),
		Name:   record.Name,
		Inode:  uint64(record.Inode),
		Type:   inodeTypeToProto(record.Type),
	}
}

func inodeToProto(record fsmeta.InodeRecord) *fsmetapb.InodeRecord {
	return &fsmetapb.InodeRecord{
		Inode:         uint64(record.Inode),
		Type:          inodeTypeToProto(record.Type),
		Size:          record.Size,
		Mode:          record.Mode,
		LinkCount:     record.LinkCount,
		CreatedUnixNs: record.CreatedUnixNs,
		UpdatedUnixNs: record.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), record.OpaqueAttrs...),
	}
}

func sessionToProto(record fsmeta.SessionRecord) *fsmetapb.SessionRecord {
	return &fsmetapb.SessionRecord{
		Session:       string(record.Session),
		Inode:         uint64(record.Inode),
		ExpiresUnixNs: record.ExpiresUnixNs,
	}
}

func pairToProto(pair fsmeta.DentryAttrPair) *fsmetapb.DentryAttrPair {
	return &fsmetapb.DentryAttrPair{
		Dentry: dentryToProto(pair.Dentry),
		Inode:  inodeToProto(pair.Inode),
	}
}

func watchRequestFromProto(req *fsmetapb.WatchSubtreeRequest) fsmeta.WatchRequest {
	if req == nil {
		return fsmeta.WatchRequest{}
	}
	return fsmeta.WatchRequest{
		Mount:              fsmeta.MountID(req.GetMount()),
		RootInode:          fsmeta.InodeID(req.GetRootInode()),
		KeyPrefix:          append([]byte(nil), req.GetKeyPrefix()...),
		DescendRecursively: req.GetDescendRecursively(),
		ResumeCursor:       watchCursorFromProto(req.GetResumeCursor()),
		BackPressureWindow: req.GetBackPressureWindow(),
	}
}

func watchCursorFromProto(cursor *fsmetapb.WatchCursor) fsmeta.WatchCursor {
	if cursor == nil {
		return fsmeta.WatchCursor{}
	}
	return fsmeta.WatchCursor{
		RegionID: cursor.GetRegionId(),
		Term:     cursor.GetTerm(),
		Index:    cursor.GetIndex(),
	}
}

func watchCursorToProto(cursor fsmeta.WatchCursor) *fsmetapb.WatchCursor {
	return &fsmetapb.WatchCursor{
		RegionId: cursor.RegionID,
		Term:     cursor.Term,
		Index:    cursor.Index,
	}
}

func watchEventToProto(evt fsmeta.WatchEvent) *fsmetapb.WatchEvent {
	return &fsmetapb.WatchEvent{
		RaftCursor:    watchCursorToProto(evt.Cursor),
		CommitVersion: evt.CommitVersion,
		Source:        watchEventSourceToProto(evt.Source),
		Key:           append([]byte(nil), evt.Key...),
	}
}

func watchEventSourceToProto(source fsmeta.WatchEventSource) fsmetapb.WatchEventSource {
	switch source {
	case fsmeta.WatchEventSourceCommit:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_COMMIT
	case fsmeta.WatchEventSourceResolveLock:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_RESOLVE_LOCK
	case fsmeta.WatchEventSourcePerasVisible:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_PERAS_VISIBLE
	default:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_UNSPECIFIED
	}
}

func inodeTypeFromProto(typ fsmetapb.InodeType) fsmeta.InodeType {
	switch typ {
	case fsmetapb.InodeType_INODE_TYPE_FILE:
		return fsmeta.InodeTypeFile
	case fsmetapb.InodeType_INODE_TYPE_DIRECTORY:
		return fsmeta.InodeTypeDirectory
	default:
		return ""
	}
}

func inodeTypeToProto(typ fsmeta.InodeType) fsmetapb.InodeType {
	switch typ {
	case fsmeta.InodeTypeFile:
		return fsmetapb.InodeType_INODE_TYPE_FILE
	case fsmeta.InodeTypeDirectory:
		return fsmetapb.InodeType_INODE_TYPE_DIRECTORY
	default:
		return fsmetapb.InodeType_INODE_TYPE_UNSPECIFIED
	}
}
