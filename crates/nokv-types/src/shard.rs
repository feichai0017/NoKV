//! Subtree / path-prefix shard routing — the partitioning function shared
//! *identically* by client, server, and control plane.
//!
//! A shard owns a registered path subtree (`mount:prefix`). Routing is by the
//! LONGEST matching prefix; a default/root shard ([`DEFAULT_SHARD_INDEX`]) owns
//! `/` and anything not under a more-specific registered prefix. Inode routing
//! is direct: the owning shard index lives in the high bits of every inode id
//! (see [`InodeId::shard_index`]).

use std::fmt;

use crate::{InodeId, MountId};

/// The default/root shard index. Owns `/` and anything not under a more-specific
/// registered prefix. Its inode ids are the legacy single-shard ids, unchanged.
pub const DEFAULT_SHARD_INDEX: u16 = 0;

/// A registered shard subtree boundary: a mount plus an absolute path prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardPrefix {
    pub mount: MountId,
    /// Absolute, normalized prefix path: starts with `/`, no trailing `/`
    /// except the root `/`.
    pub path: String,
}

/// Error parsing a `mount-<n>:<path>` shard-prefix string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardPrefixParseError(pub String);

impl fmt::Display for ShardPrefixParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ShardPrefixParseError {}

impl ShardPrefix {
    pub fn new(mount: MountId, path: impl Into<String>) -> Self {
        Self {
            mount,
            path: normalize_prefix_path(path.into()),
        }
    }

    /// Parse `mount-<n>:<path>`, e.g. `mount-1:/dataset/imagenet`. This is the
    /// same string shape as the control-plane `ShardId`, so a shard's identity
    /// round-trips between the control plane and the routing function.
    pub fn parse(value: &str) -> Result<Self, ShardPrefixParseError> {
        let (head, path) = value.split_once(':').ok_or_else(|| {
            ShardPrefixParseError(format!("missing ':' in shard prefix {value:?}"))
        })?;
        let digits = head.strip_prefix("mount-").ok_or_else(|| {
            ShardPrefixParseError(format!("shard prefix must start with 'mount-': {value:?}"))
        })?;
        let mount_raw: u64 = digits.parse().map_err(|_| {
            ShardPrefixParseError(format!("invalid mount id in shard prefix {value:?}"))
        })?;
        let mount =
            MountId::new(mount_raw).map_err(|err| ShardPrefixParseError(err.to_string()))?;
        if !path.starts_with('/') {
            return Err(ShardPrefixParseError(format!(
                "shard prefix path must be absolute: {value:?}"
            )));
        }
        Ok(Self {
            mount,
            path: normalize_prefix_path(path.to_owned()),
        })
    }
}

impl fmt::Display for ShardPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mount-{}:{}", self.mount.get(), self.path)
    }
}

/// One registered subtree shard: the prefix it owns and its shard index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardRoute {
    pub shard_index: u16,
    pub prefix: ShardPrefix,
}

/// The partitioning function: maps a `(mount, path)` or an inode to a shard
/// index by longest-prefix match, with a default/root-shard fallback. Holds no
/// endpoint or storage state — callers map the returned index to their own
/// `ShardSlot` (server) or owner endpoint (client).
#[derive(Clone, Debug, Default)]
pub struct ShardMap {
    /// Registered subtree shards (not the default), kept sorted by descending
    /// prefix length so the first match is the longest.
    entries: Vec<ShardRoute>,
}

impl ShardMap {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn from_routes(mut entries: Vec<ShardRoute>) -> Self {
        sort_longest_first(&mut entries);
        Self { entries }
    }

    /// Register (or replace) a subtree shard.
    pub fn insert(&mut self, route: ShardRoute) {
        self.entries
            .retain(|existing| existing.prefix != route.prefix);
        self.entries.push(route);
        sort_longest_first(&mut self.entries);
    }

    pub fn routes(&self) -> &[ShardRoute] {
        &self.entries
    }

    /// Longest-prefix match for an absolute, normalized `path` under `mount`.
    /// Returns [`DEFAULT_SHARD_INDEX`] when nothing more specific matches.
    pub fn route(&self, mount: MountId, path: &str) -> u16 {
        for entry in &self.entries {
            if entry.prefix.mount == mount && path_has_prefix(path, &entry.prefix.path) {
                return entry.shard_index;
            }
        }
        DEFAULT_SHARD_INDEX
    }

    /// Route by inode — the owning shard index is encoded in the id itself, so
    /// bare-inode operations route with no lookup.
    pub fn route_inode(&self, inode: InodeId) -> u16 {
        inode.shard_index()
    }
}

fn sort_longest_first(entries: &mut [ShardRoute]) {
    entries.sort_by(|a, b| {
        b.prefix
            .path
            .len()
            .cmp(&a.prefix.path.len())
            .then_with(|| a.prefix.path.cmp(&b.prefix.path))
    });
}

/// Component-boundary-safe prefix test: `/dataset` matches `/dataset` and
/// `/dataset/x`, but NOT `/datasetx`.
fn path_has_prefix(path: &str, prefix: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    if !path.starts_with(prefix) {
        return false;
    }
    path.len() == prefix.len() || path.as_bytes()[prefix.len()] == b'/'
}

/// Normalize a prefix path: ensure a leading `/`, strip a trailing `/` (except
/// the root).
fn normalize_prefix_path(mut path: String) -> String {
    if !path.starts_with('/') {
        path.insert(0, '/');
    }
    while path.len() > 1 && path.ends_with('/') {
        path.pop();
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount(n: u64) -> MountId {
        MountId::new(n).unwrap()
    }

    #[test]
    fn inode_compose_round_trips_including_root() {
        let root = InodeId::root();
        assert_eq!(root.shard_index(), 0);
        assert_eq!(root.local(), InodeId::ROOT_RAW);

        // Shard 0 is the identity on the local number (legacy ids unchanged).
        let zero = InodeId::compose(0, 42).unwrap();
        assert_eq!(zero.get(), 42);
        assert_eq!(zero.shard_index(), 0);
        assert_eq!(zero.local(), 42);

        // A non-zero shard tags the high bits and stays globally unique.
        let tagged = InodeId::compose(7, 42).unwrap();
        assert_eq!(tagged.shard_index(), 7);
        assert_eq!(tagged.local(), 42);
        assert_ne!(tagged.get(), zero.get());

        let max = InodeId::compose(InodeId::MAX_SHARD_INDEX, InodeId::MAX_LOCAL).unwrap();
        assert_eq!(max.shard_index(), InodeId::MAX_SHARD_INDEX);
        assert_eq!(max.local(), InodeId::MAX_LOCAL);
    }

    #[test]
    fn inode_compose_rejects_out_of_range_local() {
        assert!(InodeId::compose(3, 0).is_err());
        assert!(InodeId::compose(3, InodeId::MAX_LOCAL + 1).is_err());
    }

    #[test]
    fn distinct_shards_never_collide() {
        let a = InodeId::compose(1, 1).unwrap();
        let b = InodeId::compose(2, 1).unwrap();
        assert_ne!(a.get(), b.get());
        assert_eq!(a.shard_index(), 1);
        assert_eq!(b.shard_index(), 2);
    }

    #[test]
    fn shard_prefix_parses_and_round_trips() {
        let parsed = ShardPrefix::parse("mount-1:/dataset/imagenet").unwrap();
        assert_eq!(parsed.mount, mount(1));
        assert_eq!(parsed.path, "/dataset/imagenet");
        assert_eq!(parsed.to_string(), "mount-1:/dataset/imagenet");

        // Trailing slash is normalized away; root is preserved.
        assert_eq!(ShardPrefix::parse("mount-2:/runs/").unwrap().path, "/runs");
        assert_eq!(ShardPrefix::parse("mount-1:/").unwrap().path, "/");
    }

    #[test]
    fn shard_prefix_parse_rejects_malformed() {
        assert!(ShardPrefix::parse("no-colon").is_err());
        assert!(ShardPrefix::parse("shard-1:/x").is_err());
        assert!(ShardPrefix::parse("mount-0:/x").is_err());
        assert!(ShardPrefix::parse("mount-1:relative").is_err());
    }

    #[test]
    fn route_uses_longest_prefix_with_component_boundaries() {
        let map = ShardMap::from_routes(vec![
            ShardRoute {
                shard_index: 1,
                prefix: ShardPrefix::new(mount(1), "/dataset"),
            },
            ShardRoute {
                shard_index: 2,
                prefix: ShardPrefix::new(mount(1), "/dataset/imagenet"),
            },
        ]);

        // Longest prefix wins.
        assert_eq!(map.route(mount(1), "/dataset/imagenet/train/0.tar"), 2);
        assert_eq!(map.route(mount(1), "/dataset/coco/0.tar"), 1);
        assert_eq!(map.route(mount(1), "/dataset"), 1);

        // Component boundary: "/datasetx" must NOT match "/dataset".
        assert_eq!(map.route(mount(1), "/datasetx"), DEFAULT_SHARD_INDEX);

        // Anything unmatched falls to the default/root shard.
        assert_eq!(map.route(mount(1), "/runs/r1"), DEFAULT_SHARD_INDEX);
        assert_eq!(map.route(mount(1), "/"), DEFAULT_SHARD_INDEX);
    }

    #[test]
    fn route_isolates_mounts() {
        let map = ShardMap::from_routes(vec![ShardRoute {
            shard_index: 5,
            prefix: ShardPrefix::new(mount(1), "/dataset"),
        }]);
        assert_eq!(map.route(mount(1), "/dataset/x"), 5);
        // Same path under a different mount does not match.
        assert_eq!(map.route(mount(2), "/dataset/x"), DEFAULT_SHARD_INDEX);
    }

    #[test]
    fn insert_replaces_same_prefix_and_keeps_longest_first() {
        let mut map = ShardMap::new();
        map.insert(ShardRoute {
            shard_index: 1,
            prefix: ShardPrefix::new(mount(1), "/a"),
        });
        map.insert(ShardRoute {
            shard_index: 9,
            prefix: ShardPrefix::new(mount(1), "/a"),
        });
        assert_eq!(map.routes().len(), 1);
        assert_eq!(map.route(mount(1), "/a/b"), 9);
    }

    #[test]
    fn route_inode_reads_the_shard_index_directly() {
        let map = ShardMap::new();
        let inode = InodeId::compose(11, 3).unwrap();
        assert_eq!(map.route_inode(inode), 11);
        assert_eq!(map.route_inode(InodeId::root()), DEFAULT_SHARD_INDEX);
    }
}
