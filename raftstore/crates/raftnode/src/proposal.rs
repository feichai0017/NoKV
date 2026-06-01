use prost::Message;

use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::{Error, RegionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub region_id: RegionId,
    pub payload: ProposalPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalPayload {
    MetadataCommand(Vec<u8>),
    MetadataCommandBatch(Vec<u8>),
    RegionDescriptor(Vec<u8>),
}

#[derive(Clone, PartialEq, Message)]
struct PersistedMetadataCommandBatch {
    #[prost(bytes = "vec", repeated, tag = "1")]
    commands: Vec<Vec<u8>>,
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

    pub fn from_metadata_command_batch(
        reqs: &[metadatapb::MetadataCommitRequest],
    ) -> Result<Self, Error> {
        let Some(first) = reqs.first() else {
            return Err(Error::InvalidLogPayload(
                "metadata command batch is empty".to_owned(),
            ));
        };
        let region_id = first
            .context
            .as_ref()
            .map(|context| context.region_id)
            .ok_or(Error::MissingRegionHeader)?;
        if region_id == 0 {
            return Err(Error::MissingRegionHeader);
        }
        let mut commands = Vec::with_capacity(reqs.len());
        for req in reqs {
            let command_region_id = req
                .context
                .as_ref()
                .map(|context| context.region_id)
                .ok_or(Error::MissingRegionHeader)?;
            if command_region_id != region_id {
                return Err(Error::RegionMismatch {
                    proposal_region_id: region_id,
                    command_region_id,
                });
            }
            let mut payload = Vec::with_capacity(req.encoded_len());
            req.encode(&mut payload)?;
            commands.push(payload);
        }
        let persisted = PersistedMetadataCommandBatch { commands };
        let mut payload = Vec::with_capacity(persisted.encoded_len());
        persisted.encode(&mut payload)?;
        Ok(Self {
            region_id,
            payload: ProposalPayload::MetadataCommandBatch(payload),
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

    pub fn decode_metadata_command_batch(
        &self,
    ) -> Result<Vec<metadatapb::MetadataCommitRequest>, Error> {
        let ProposalPayload::MetadataCommandBatch(payload) = &self.payload else {
            return Err(Error::InvalidLogPayload(
                "non-metadata-command-batch proposal cannot decode as metadata command batch"
                    .to_owned(),
            ));
        };
        let persisted = PersistedMetadataCommandBatch::decode(payload.as_slice())?;
        if persisted.commands.is_empty() {
            return Err(Error::InvalidLogPayload(
                "metadata command batch is empty".to_owned(),
            ));
        }
        persisted
            .commands
            .into_iter()
            .map(|payload| {
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
            })
            .collect()
    }

    pub(crate) fn payload_kind(&self) -> ProposalPayloadKind {
        match &self.payload {
            ProposalPayload::MetadataCommand(_) => ProposalPayloadKind::MetadataCommand,
            ProposalPayload::MetadataCommandBatch(_) => ProposalPayloadKind::MetadataCommandBatch,
            ProposalPayload::RegionDescriptor(_) => ProposalPayloadKind::RegionDescriptor,
        }
    }

    pub(crate) fn payload_bytes(&self) -> &[u8] {
        match &self.payload {
            ProposalPayload::MetadataCommand(payload)
            | ProposalPayload::MetadataCommandBatch(payload)
            | ProposalPayload::RegionDescriptor(payload) => payload,
        }
    }

    pub(crate) fn metadata_command_count(&self) -> u64 {
        match &self.payload {
            ProposalPayload::MetadataCommand(_) => 1,
            ProposalPayload::MetadataCommandBatch(payload) => {
                PersistedMetadataCommandBatch::decode(payload.as_slice())
                    .map(|batch| batch.commands.len() as u64)
                    .unwrap_or(0)
            }
            ProposalPayload::RegionDescriptor(_) => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProposalPayloadKind {
    MetadataCommand,
    MetadataCommandBatch,
    RegionDescriptor,
}
