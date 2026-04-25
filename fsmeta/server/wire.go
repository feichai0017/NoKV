package server

import (
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
)

func createRequestFromProto(req *fsmetapb.CreateRequest) (fsmeta.CreateRequest, fsmeta.InodeRecord) {
	inode := inodeFromProto(req.GetInode())
	return fsmeta.CreateRequest{
		Mount:  fsmeta.MountID(req.GetMount()),
		Parent: fsmeta.InodeID(req.GetParent()),
		Name:   req.GetName(),
		Inode:  inode.Inode,
	}, inode
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

func renameRequestFromProto(req *fsmetapb.RenameRequest) fsmeta.RenameRequest {
	return fsmeta.RenameRequest{
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

func inodeFromProto(pb *fsmetapb.InodeRecord) fsmeta.InodeRecord {
	if pb == nil {
		return fsmeta.InodeRecord{}
	}
	return fsmeta.InodeRecord{
		Inode:         fsmeta.InodeID(pb.GetInode()),
		Type:          inodeTypeFromProto(pb.GetType()),
		Size:          pb.GetSize(),
		Mode:          pb.GetMode(),
		LinkCount:     pb.GetLinkCount(),
		CreatedUnixNs: pb.GetCreatedUnixNs(),
		UpdatedUnixNs: pb.GetUpdatedUnixNs(),
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
