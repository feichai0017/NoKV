use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;

use nokv_raftstore_server::PeerEndpointCatalog;

pub(crate) fn advertised_addr_from_env(
    bind_addr: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    let addr =
        std::env::var("NOKV_RAFTSTORE_ADVERTISE_ADDR").unwrap_or_else(|_| bind_addr.to_string());
    let addr = addr.trim();
    if addr.is_empty() {
        return Err("NOKV_RAFTSTORE_ADVERTISE_ADDR must not be empty".into());
    }
    Ok(addr.to_owned())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ServerArgs {
    pub(crate) metrics_addr: Option<SocketAddr>,
}

impl ServerArgs {
    pub(crate) fn parse<I>(args: I) -> Result<Self, Box<dyn std::error::Error>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut metrics_addr = None;
        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            if let Some(value) = arg.strip_prefix("--metrics-addr=") {
                metrics_addr = Some(value.parse::<SocketAddr>()?);
                continue;
            }
            if arg == "--metrics-addr" {
                let value = iter.next().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "--metrics-addr requires a listen address",
                    )
                })?;
                metrics_addr = Some(value.parse::<SocketAddr>()?);
                continue;
            }
        }
        Ok(Self { metrics_addr })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegionKeyRange {
    pub(crate) start_key: Vec<u8>,
    pub(crate) end_key: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RegionRangeCatalog {
    pub(crate) ranges: HashMap<u64, RegionKeyRange>,
}

impl RegionRangeCatalog {
    pub(crate) fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let Ok(raw) = std::env::var("NOKV_RAFTSTORE_REGION_RANGES") else {
            return Ok(Self::default());
        };
        Self::parse(&raw)
    }

    pub(crate) fn parse(raw: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut ranges = HashMap::new();
        for item in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let (region_id, range) = item.split_once('=').ok_or_else(|| {
                format!("invalid NOKV_RAFTSTORE_REGION_RANGES entry {item:?}: expected region_id=start_hex:end_hex")
            })?;
            let region_id = parse_required_nonzero_u64(
                "NOKV_RAFTSTORE_REGION_RANGES region_id",
                Some(region_id.to_owned()),
                0,
            )?;
            let range = parse_region_key_range(range)?;
            if ranges.insert(region_id, range).is_some() {
                return Err(format!(
                    "duplicate region_id {region_id} in NOKV_RAFTSTORE_REGION_RANGES"
                )
                .into());
            }
        }
        Ok(Self { ranges })
    }

    pub(crate) fn get(&self, region_id: u64) -> Option<&RegionKeyRange> {
        self.ranges.get(&region_id)
    }
}

pub(crate) fn parse_region_key_range(
    raw: &str,
) -> Result<RegionKeyRange, Box<dyn std::error::Error>> {
    let (start, end) = raw
        .split_once(':')
        .ok_or_else(|| format!("invalid region range {raw:?}: expected start_hex:end_hex"))?;
    let range = RegionKeyRange {
        start_key: decode_hex_key(start)?,
        end_key: decode_hex_key(end)?,
    };
    if !range.end_key.is_empty() && range.start_key >= range.end_key {
        return Err(
            format!("invalid region range {raw:?}: start key must be less than end key").into(),
        );
    }
    Ok(range)
}

pub(crate) fn decode_hex_key(raw: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    if raw.len() % 2 != 0 {
        return Err(format!("hex key {raw:?} must have an even number of digits").into());
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.as_bytes().chunks_exact(2) {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

pub(crate) fn hex_digit(byte: u8) -> Result<u8, Box<dyn std::error::Error>> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit {:?}", byte as char).into()),
    }
}

pub(crate) fn validate_startup_region_ranges(
    identities: &[ServerIdentity],
    ranges: &RegionRangeCatalog,
) -> Result<(), Box<dyn std::error::Error>> {
    if identities.len() <= 1 {
        return Ok(());
    }
    let mut explicit = Vec::new();
    for identity in identities {
        if !identity.bootstrap {
            continue;
        }
        let range = ranges.get(identity.region_id).ok_or_else(|| {
            format!(
                "multi-region bootstrap requires NOKV_RAFTSTORE_REGION_RANGES for region {}",
                identity.region_id
            )
        })?;
        explicit.push((identity.region_id, range));
    }
    for left in 0..explicit.len() {
        for right in (left + 1)..explicit.len() {
            let (left_id, left_range) = explicit[left];
            let (right_id, right_range) = explicit[right];
            if region_ranges_overlap(left_range, right_range) {
                return Err(format!(
                    "region ranges overlap in NOKV_RAFTSTORE_REGION_RANGES: region {left_id} overlaps region {right_id}"
                )
                .into());
            }
        }
    }
    Ok(())
}

pub(crate) fn region_ranges_overlap(left: &RegionKeyRange, right: &RegionKeyRange) -> bool {
    range_start_before_end(&left.start_key, &right.end_key)
        && range_start_before_end(&right.start_key, &left.end_key)
}

pub(crate) fn range_start_before_end(start: &[u8], end: &[u8]) -> bool {
    end.is_empty() || start < end
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ServerIdentity {
    pub(crate) region_id: u64,
    pub(crate) store_id: u64,
    pub(crate) peer_id: u64,
    pub(crate) bootstrap: bool,
}

impl Default for ServerIdentity {
    fn default() -> Self {
        Self {
            region_id: 1,
            store_id: 1,
            peer_id: 1,
            bootstrap: true,
        }
    }
}

impl ServerIdentity {
    pub(crate) fn from_env_list() -> Result<Vec<Self>, Box<dyn std::error::Error>> {
        if let Ok(raw) = std::env::var("NOKV_RAFTSTORE_REGIONS") {
            return Self::from_region_list(&raw);
        }
        Ok(vec![Self::from_env()?])
    }

    pub(crate) fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_values(
            std::env::var("NOKV_RAFTSTORE_REGION_ID").ok(),
            std::env::var("NOKV_RAFTSTORE_STORE_ID").ok(),
            std::env::var("NOKV_RAFTSTORE_PEER_ID").ok(),
            std::env::var("NOKV_RAFTSTORE_BOOTSTRAP").ok(),
        )
    }

    pub(crate) fn from_values(
        region_id: Option<String>,
        store_id: Option<String>,
        peer_id: Option<String>,
        bootstrap: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let default = Self::default();
        Ok(Self {
            region_id: parse_required_nonzero_u64(
                "NOKV_RAFTSTORE_REGION_ID",
                region_id,
                default.region_id,
            )?,
            store_id: parse_required_nonzero_u64(
                "NOKV_RAFTSTORE_STORE_ID",
                store_id,
                default.store_id,
            )?,
            peer_id: parse_required_nonzero_u64(
                "NOKV_RAFTSTORE_PEER_ID",
                peer_id,
                default.peer_id,
            )?,
            bootstrap: parse_bootstrap_flag(bootstrap, default.bootstrap)?,
        })
    }

    pub(crate) fn from_region_list(raw: &str) -> Result<Vec<Self>, Box<dyn std::error::Error>> {
        let mut identities = Vec::new();
        for item in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let fields = item.split(':').collect::<Vec<_>>();
            if fields.len() != 4 {
                return Err(format!(
                    "invalid NOKV_RAFTSTORE_REGIONS entry {item:?}: expected region_id:store_id:peer_id:bootstrap"
                )
                .into());
            }
            identities.push(Self::from_values(
                Some(fields[0].to_owned()),
                Some(fields[1].to_owned()),
                Some(fields[2].to_owned()),
                Some(fields[3].to_owned()),
            )?);
        }
        validate_server_identities(&identities)?;
        Ok(identities)
    }
}

pub(crate) fn validate_server_identities(
    identities: &[ServerIdentity],
) -> Result<(), Box<dyn std::error::Error>> {
    if identities.is_empty() {
        return Err("NOKV_RAFTSTORE_REGIONS must contain at least one region".into());
    }
    let store_id = identities[0].store_id;
    let mut region_ids = BTreeSet::new();
    let mut peer_ids = BTreeSet::new();
    for identity in identities {
        if identity.store_id != store_id {
            return Err(format!(
                "NOKV_RAFTSTORE_REGIONS must use one store_id per process: got {} and {}",
                store_id, identity.store_id
            )
            .into());
        }
        if !region_ids.insert(identity.region_id) {
            return Err(format!(
                "duplicate region_id {} in NOKV_RAFTSTORE_REGIONS",
                identity.region_id
            )
            .into());
        }
        if !peer_ids.insert(identity.peer_id) {
            return Err(format!(
                "duplicate peer_id {} in NOKV_RAFTSTORE_REGIONS",
                identity.peer_id
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn parse_required_nonzero_u64(
    name: &str,
    value: Option<String>,
    default: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err(format!("{name} must be non-zero").into());
    }
    Ok(parsed)
}

pub(crate) fn parse_bootstrap_flag(
    value: Option<String>,
    default: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err("NOKV_RAFTSTORE_BOOTSTRAP must be true or false".into()),
    }
}

pub(crate) fn peer_endpoint_catalog_from_env(
) -> Result<PeerEndpointCatalog, Box<dyn std::error::Error>> {
    let catalog = PeerEndpointCatalog::require_configured();
    let Ok(raw) = std::env::var("NOKV_RAFTSTORE_PEER_ENDPOINTS") else {
        return Ok(catalog);
    };
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (peer_id, endpoint) = item.split_once('=').ok_or_else(|| {
            format!(
                "invalid NOKV_RAFTSTORE_PEER_ENDPOINTS entry {item:?}: expected peer_id=endpoint"
            )
        })?;
        catalog.insert_peer(peer_id.parse()?, endpoint.to_owned())?;
    }
    Ok(catalog)
}
