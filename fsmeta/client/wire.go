package client

import (
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
)

func createRequestToProto(req fsmeta.CreateRequest, inode fsmeta.InodeRecord) *fsmetapb.CreateRequest {
	inode.Inode = req.Inode
	return &fsmetapb.CreateRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
		Inode:  inodeToProto(inode),
	}
}

func lookupRequestToProto(req fsmeta.LookupRequest) *fsmetapb.LookupRequest {
	return &fsmetapb.LookupRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
	}
}

func readDirRequestToProto(req fsmeta.ReadDirRequest) *fsmetapb.ReadDirRequest {
	return &fsmetapb.ReadDirRequest{
		Mount:      string(req.Mount),
		Parent:     uint64(req.Parent),
		StartAfter: req.StartAfter,
		Limit:      req.Limit,
	}
}

func renameRequestToProto(req fsmeta.RenameRequest) *fsmetapb.RenameRequest {
	return &fsmetapb.RenameRequest{
		Mount:      string(req.Mount),
		FromParent: uint64(req.FromParent),
		FromName:   req.FromName,
		ToParent:   uint64(req.ToParent),
		ToName:     req.ToName,
	}
}

func unlinkRequestToProto(req fsmeta.UnlinkRequest) *fsmetapb.UnlinkRequest {
	return &fsmetapb.UnlinkRequest{
		Mount:  string(req.Mount),
		Parent: uint64(req.Parent),
		Name:   req.Name,
	}
}

func dentryFromProto(pb *fsmetapb.DentryRecord) fsmeta.DentryRecord {
	if pb == nil {
		return fsmeta.DentryRecord{}
	}
	return fsmeta.DentryRecord{
		Parent: fsmeta.InodeID(pb.GetParent()),
		Name:   pb.GetName(),
		Inode:  fsmeta.InodeID(pb.GetInode()),
		Type:   inodeTypeFromProto(pb.GetType()),
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

func pairFromProto(pb *fsmetapb.DentryAttrPair) fsmeta.DentryAttrPair {
	if pb == nil {
		return fsmeta.DentryAttrPair{}
	}
	return fsmeta.DentryAttrPair{
		Dentry: dentryFromProto(pb.GetDentry()),
		Inode:  inodeFromProto(pb.GetInode()),
	}
}

func watchRequestToProto(req fsmeta.WatchRequest) *fsmetapb.WatchSubtreeRequest {
	return &fsmetapb.WatchSubtreeRequest{
		Mount:              string(req.Mount),
		RootInode:          uint64(req.RootInode),
		KeyPrefix:          append([]byte(nil), req.KeyPrefix...),
		DescendRecursively: req.DescendRecursively,
		ResumeCursor:       watchCursorToProto(req.ResumeCursor),
		BackPressureWindow: req.BackPressureWindow,
	}
}

func watchCursorToProto(cursor fsmeta.WatchCursor) *fsmetapb.WatchCursor {
	return &fsmetapb.WatchCursor{
		RegionId: cursor.RegionID,
		Term:     cursor.Term,
		Index:    cursor.Index,
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

func watchEventFromProto(pb *fsmetapb.WatchEvent) fsmeta.WatchEvent {
	if pb == nil {
		return fsmeta.WatchEvent{}
	}
	return fsmeta.WatchEvent{
		Cursor:        watchCursorFromProto(pb.GetRaftCursor()),
		CommitVersion: pb.GetCommitVersion(),
		Source:        watchEventSourceFromProto(pb.GetSource()),
		Key:           append([]byte(nil), pb.GetKey()...),
	}
}

func watchEventSourceFromProto(source fsmetapb.WatchEventSource) fsmeta.WatchEventSource {
	switch source {
	case fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_COMMIT:
		return fsmeta.WatchEventSourceCommit
	case fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_RESOLVE_LOCK:
		return fsmeta.WatchEventSourceResolveLock
	default:
		return 0
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
