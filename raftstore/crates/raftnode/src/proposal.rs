use prost::Message;

use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_proto::nokv::raft::v1 as raftpb;

use crate::{Error, RegionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub region_id: RegionId,
    pub payload: ProposalPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalPayload {
    MetadataCommand(Vec<u8>),
    RegionDescriptor(Vec<u8>),
    AdminCommand(Vec<u8>),
}

impl Proposal {
    pub fn from_region_descriptor(descriptor: &metapb::RegionDescriptor) -> Result<Self, Error> {
        if descriptor.region_id == 0 {
            return Err(Error::InvalidRegionDescriptor(
                "region descriptor id is required".to_owned(),
            ));
        }
        let mut payload = Vec::with_capacity(descriptor.encoded_len());
        descriptor.encode(&mut payload)?;
        Ok(Self {
            region_id: descriptor.region_id,
            payload: ProposalPayload::RegionDescriptor(payload),
        })
    }

    pub fn from_metadata_command(req: &metadatapb::MetadataCommitRequest) -> Result<Self, Error> {
        let region_id = req
            .context
            .as_ref()
            .map(|context| context.region_id)
            .ok_or(Error::MissingRegionHeader)?;
        if region_id == 0 {
            return Err(Error::MissingRegionHeader);
        }
        let mut payload = Vec::with_capacity(req.encoded_len());
        req.encode(&mut payload)?;
        Ok(Self {
            region_id,
            payload: ProposalPayload::MetadataCommand(payload),
        })
    }

    pub fn from_admin_command(
        region_id: RegionId,
        command: &raftpb::AdminCommand,
    ) -> Result<Self, Error> {
        if region_id == 0 {
            return Err(Error::MissingRegionHeader);
        }
        let mut payload = Vec::with_capacity(command.encoded_len());
        command.encode(&mut payload)?;
        Ok(Self {
            region_id,
            payload: ProposalPayload::AdminCommand(payload),
        })
    }

    pub fn decode_region_descriptor(&self) -> Result<metapb::RegionDescriptor, Error> {
        let ProposalPayload::RegionDescriptor(payload) = &self.payload else {
            return Err(Error::InvalidLogPayload(
                "raft command proposal cannot decode as region descriptor".to_owned(),
            ));
        };
        let descriptor = metapb::RegionDescriptor::decode(payload.as_slice())?;
        if descriptor.region_id != self.region_id {
            return Err(Error::RegionMismatch {
                proposal_region_id: self.region_id,
                command_region_id: descriptor.region_id,
            });
        }
        Ok(descriptor)
    }

    pub fn decode_metadata_command(&self) -> Result<metadatapb::MetadataCommitRequest, Error> {
        let ProposalPayload::MetadataCommand(payload) = &self.payload else {
            return Err(Error::InvalidLogPayload(
                "non-metadata-command proposal cannot decode as metadata command".to_owned(),
            ));
        };
        let req = metadatapb::MetadataCommitRequest::decode(payload.as_slice())?;
        let region_id = req
            .context
            .as_ref()
            .map(|context| context.region_id)
            .ok_or(Error::MissingRegionHeader)?;
        if region_id != self.region_id {
            return Err(Error::RegionMismatch {
                proposal_region_id: self.region_id,
                command_region_id: region_id,
            });
        }
        Ok(req)
    }

    pub fn decode_admin_command(&self) -> Result<raftpb::AdminCommand, Error> {
        let ProposalPayload::AdminCommand(payload) = &self.payload else {
            return Err(Error::InvalidLogPayload(
                "non-admin-command proposal cannot decode as admin command".to_owned(),
            ));
        };
        Ok(raftpb::AdminCommand::decode(payload.as_slice())?)
    }

    pub(crate) fn payload_kind(&self) -> ProposalPayloadKind {
        match &self.payload {
            ProposalPayload::MetadataCommand(_) => ProposalPayloadKind::MetadataCommand,
            ProposalPayload::RegionDescriptor(_) => ProposalPayloadKind::RegionDescriptor,
            ProposalPayload::AdminCommand(_) => ProposalPayloadKind::AdminCommand,
        }
    }

    pub(crate) fn payload_bytes(&self) -> &[u8] {
        match &self.payload {
            ProposalPayload::MetadataCommand(payload)
            | ProposalPayload::RegionDescriptor(payload)
            | ProposalPayload::AdminCommand(payload) => payload,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProposalPayloadKind {
    MetadataCommand,
    RegionDescriptor,
    AdminCommand,
}
