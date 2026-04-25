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
		Mount:      fsmeta.MountID(req.GetMount()),
		Parent:     fsmeta.InodeID(req.GetParent()),
		StartAfter: req.GetStartAfter(),
		Limit:      req.GetLimit(),
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
