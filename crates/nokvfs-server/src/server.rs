use std::error::Error;
use std::fmt;
use std::io;
use std::net::TcpListener;
use std::sync::Arc;

use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
};
use nokvfs_object::{ObjectError, S3ObjectStore};

use crate::http;
use crate::options::ServerOptions;

const DEFAULT_ROOT_MODE: u32 = 0o755;

pub struct Server {
    service: Arc<NoKvFs<HoltMetadataStore, S3ObjectStore>>,
    object_gc: ObjectGcWorker,
    history_gc: HistoryGcWorker,
}

#[derive(Debug)]
pub enum ServerError {
    Io(io::Error),
    Metadata(MetadError),
    Object(ObjectError),
}

pub fn run(options: ServerOptions) -> Result<(), ServerError> {
    let bind = options.bind;
    let server = Server::open(options)?;
    let listener = TcpListener::bind(bind).map_err(ServerError::Io)?;
    server.serve(listener)
}

impl Server {
    pub fn open(options: ServerOptions) -> Result<Self, ServerError> {
        let metadata =
            HoltMetadataStore::open_file(&options.meta_path).map_err(MetadError::from)?;
        let objects = options.object.open()?;
        let service = Arc::new(NoKvFs::open_existing(options.mount, metadata, objects)?);
        service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
        let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
        let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
        Ok(Self {
            service,
            object_gc,
            history_gc,
        })
    }

    pub fn serve(&self, listener: TcpListener) -> Result<(), ServerError> {
        for stream in listener.incoming() {
            let stream = stream.map_err(ServerError::Io)?;
            if let Err(err) = http::handle_stream(self, stream) {
                eprintln!("nokvfs-server control connection failed: {err}");
            }
        }
        Ok(())
    }

    pub fn stats_json(&self) -> String {
        let objects = self.service.object_stats();
        let object_gc = self.object_gc.state();
        let history_gc = self.history_gc.state();
        format!(
            "{{\"ready\":true,\"block_cache_enabled\":{},\"object_puts\":{},\"object_gets\":{},\"cache_hits\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"object_gc\":{},\"history_gc\":{}}}\n",
            self.service.block_cache_enabled(),
            objects.object_puts,
            objects.object_gets,
            objects.cache_hits,
            objects.manifest_chunks,
            objects.manifest_blocks,
            object_gc_json(&object_gc),
            history_gc_json(&history_gc),
        )
    }

    pub fn run_manual_gc(&self) -> Result<String, ServerError> {
        let object = self.service.cleanup_pending_objects(usize::MAX)?;
        let history = self.service.cleanup_history(usize::MAX)?;
        Ok(format!(
            "{{\"object_gc\":{{\"scanned\":{},\"blocked_by_snapshots\":{},\"attempted\":{},\"deleted\":{},\"missing\":{},\"records_removed\":{}}},\"history_gc\":{{\"scanned\":{},\"removed\":{},\"retained_by_snapshots\":{}}}}}\n",
            object.scanned,
            object.blocked_by_snapshots,
            object.attempted,
            object.deleted,
            object.missing,
            object.records_removed,
            history.scanned,
            history.removed,
            history.retained_by_snapshots,
        ))
    }
}

fn object_gc_json(state: &ObjectGcWorkerState) -> String {
    format!(
        "{{\"iterations\":{},\"last_error\":{}}}",
        state.iterations,
        json_string_or_null(state.last_error.as_deref())
    )
}

fn history_gc_json(state: &HistoryGcWorkerState) -> String {
    format!(
        "{{\"iterations\":{},\"last_error\":{}}}",
        state.iterations,
        json_string_or_null(state.last_error.as_deref())
    )
}

fn json_string_or_null(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", escape_json_string(value)),
        None => "null".to_owned(),
    }
}

fn escape_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

impl From<MetadError> for ServerError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ObjectError> for ServerError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
        }
    }
}

impl Error for ServerError {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::time::Duration;

    use nokvfs_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokvfs_object::{ObjectStoreConfig, S3ObjectStoreOptions};
    use nokvfs_types::MountId;
    use tempfile::tempdir;

    pub(crate) fn test_server() -> Server {
        let dir = tempdir().unwrap();
        let options = ServerOptions {
            bind: crate::options::DEFAULT_SERVER_BIND,
            mount: MountId::new(1).unwrap(),
            meta_path: dir.path().to_path_buf(),
            object: ObjectStoreConfig::s3(S3ObjectStoreOptions {
                bucket: "test".to_owned(),
                root: "/".to_owned(),
                region: "auto".to_owned(),
                endpoint: Some("http://127.0.0.1:1".to_owned()),
                access_key_id: Some("test".to_owned()),
                secret_access_key: Some("test".to_owned()),
                session_token: None,
                virtual_host_style: false,
                skip_signature: true,
            }),
            uid: 1000,
            gid: 1000,
            object_gc: ObjectGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
            history_gc: HistoryGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
        };
        Server::open(options).unwrap()
    }

    #[test]
    fn manual_gc_reports_empty_outcomes() {
        let server = test_server();
        let body = server.run_manual_gc().unwrap();
        assert!(body.contains("\"object_gc\""));
        assert!(body.contains("\"history_gc\""));
    }
}
