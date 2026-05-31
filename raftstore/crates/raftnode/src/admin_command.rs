use nokv_proto::nokv::meta::v1 as metapb;

#[derive(Clone, PartialEq, prost::Message)]
pub struct SplitCommand {
    #[prost(uint64, tag = "1")]
    pub parent_region_id: u64,
    #[prost(bytes, tag = "2")]
    pub split_key: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub child: Option<metapb::RegionDescriptor>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MergeCommand {
    #[prost(uint64, tag = "1")]
    pub target_region_id: u64,
    #[prost(uint64, tag = "2")]
    pub source_region_id: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct AdminCommand {
    #[prost(enumeration = "AdminCommandType", tag = "1")]
    pub r#type: i32,
    #[prost(message, optional, tag = "2")]
    pub split: Option<SplitCommand>,
    #[prost(message, optional, tag = "3")]
    pub merge: Option<MergeCommand>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum AdminCommandType {
    Unknown = 0,
    Split = 1,
    Merge = 2,
}
