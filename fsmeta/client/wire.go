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
