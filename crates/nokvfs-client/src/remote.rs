use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use nokvfs_meta::{DentryWithAttr, RenameReplaceResult};
use nokvfs_protocol::{
    MetadataRpcEnvelope, MetadataRpcRequest, MetadataRpcResult, WireBodyDescriptor,
    WireDentryRecord, WireDentryWithAttr, WireInodeAttr, WireSnapshotPin,
};
use nokvfs_types::{
    BodyDescriptor, DentryName, DentryRecord, FileType, InodeAttr, InodeId, SnapshotPin,
};

use crate::{display_name, is_root_path, parse_absolute_path, ClientError};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteMetadataClientOptions {
    pub address: SocketAddr,
    pub timeout: Duration,
}

pub struct RemoteMetadataClient {
    options: RemoteMetadataClientOptions,
}

impl RemoteMetadataClientOptions {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }
}

impl RemoteMetadataClient {
    pub fn new(options: RemoteMetadataClientOptions) -> Self {
        Self { options }
    }

    pub fn connect(address: SocketAddr) -> Self {
        Self::new(RemoteMetadataClientOptions::new(address))
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::BootstrapRoot { mode, uid, gid })? {
            MetadataRpcResult::InodeAttr { .. } => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::CreateDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::CreateFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        if is_root_path(path)? {
            return Ok(None);
        }
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::LookupPlus {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let parent = self.resolve_directory(path)?;
        match self.call(MetadataRpcRequest::ReadDirPlus {
            parent: parent.get(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::RemoveFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::RemoveEmptyDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(source)?;
        let (new_parent, new_name) = self.resolve_parent(destination)?;
        match self.call(MetadataRpcRequest::Rename {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, ClientError> {
        let (parent, name) = self.resolve_parent(source)?;
        let (new_parent, new_name) = self.resolve_parent(destination)?;
        match self.call(MetadataRpcRequest::RenameReplace {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot(&self, path: &str) -> Result<SnapshotPin, ClientError> {
        let root = self.resolve_directory(path)?;
        match self.call(MetadataRpcRequest::SnapshotSubtree { root: root.get() })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, ClientError> {
        match self.call(MetadataRpcRequest::RetireSnapshot { snapshot_id })? {
            MetadataRpcResult::RetiredSnapshot { retired } => Ok(retired),
            other => Err(unexpected_result(other)),
        }
    }

    fn resolve_parent(&self, path: &str) -> Result<(InodeId, DentryName), ClientError> {
        let mut components = parse_absolute_path(path)?;
        let name = components.pop().ok_or(ClientError::RootHasNoParent)?;
        let parent = self.resolve_components_as_directory(&components)?;
        Ok((parent, name))
    }

    fn resolve_directory(&self, path: &str) -> Result<InodeId, ClientError> {
        let components = parse_absolute_path(path)?;
        self.resolve_components_as_directory(&components)
    }

    fn resolve_components_as_directory(
        &self,
        components: &[DentryName],
    ) -> Result<InodeId, ClientError> {
        let mut current = InodeId::root();
        for name in components {
            let label = display_name(name);
            let entry = match self.call(MetadataRpcRequest::LookupPlus {
                parent: current.get(),
                name: rpc_name(name)?,
            })? {
                MetadataRpcResult::Dentry { entry } => {
                    entry.map(|entry| wire_dentry(*entry)).transpose()?
                }
                other => return Err(unexpected_result(other)),
            }
            .ok_or_else(|| ClientError::NotFound(label.clone()))?;
            if entry.attr.file_type != FileType::Directory {
                return Err(ClientError::NotDirectory(label));
            }
            current = entry.attr.inode;
        }
        Ok(current)
    }

    fn call(&self, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ClientError> {
        let body =
            serde_json::to_vec(&request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        let mut stream = TcpStream::connect(self.options.address)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .set_read_timeout(Some(self.options.timeout))
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .set_write_timeout(Some(self.options.timeout))
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let header = format!(
            "POST /rpc HTTP/1.1\r\nhost: {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            self.options.address,
            body.len()
        );
        let mut request = Vec::with_capacity(header.len() + body.len());
        request.extend_from_slice(header.as_bytes());
        request.extend_from_slice(&body);
        stream
            .write_all(&request)
            .map_err(|err| ClientError::Io(err.to_string()))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let body = http_response_body(&response)?;
        let envelope: MetadataRpcEnvelope =
            serde_json::from_slice(body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        if !envelope.ok {
            return Err(ClientError::Remote(
                envelope
                    .error
                    .unwrap_or_else(|| "unknown remote error".to_owned()),
            ));
        }
        envelope
            .result
            .ok_or_else(|| ClientError::Protocol("metadata rpc response missing result".to_owned()))
    }
}

fn http_response_body(response: &[u8]) -> Result<&[u8], ClientError> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| ClientError::Protocol("metadata rpc response missing headers".to_owned()))?;
    let header = String::from_utf8_lossy(&response[..header_end]);
    let status = header.lines().next().unwrap_or_default();
    if !status.contains(" 200 ") {
        return Err(ClientError::Protocol(format!(
            "metadata rpc returned non-success status {status}"
        )));
    }
    Ok(&response[header_end + 4..])
}

fn rpc_name(name: &DentryName) -> Result<String, ClientError> {
    String::from_utf8(name.as_bytes().to_vec())
        .map_err(|_| ClientError::InvalidName("remote rpc requires utf-8 names".to_owned()))
}

fn wire_dentry(entry: WireDentryWithAttr) -> Result<DentryWithAttr, ClientError> {
    Ok(DentryWithAttr {
        dentry: wire_dentry_record(entry.dentry)?,
        attr: wire_inode_attr(entry.attr)?,
        body: entry.body.map(wire_body).transpose()?,
    })
}

fn wire_dentry_record(record: WireDentryRecord) -> Result<DentryRecord, ClientError> {
    Ok(DentryRecord {
        parent: inode_id(record.parent)?,
        name: DentryName::new(hex_decode(&record.name_hex)?)
            .map_err(|err| ClientError::InvalidName(err.to_string()))?,
        child: inode_id(record.child)?,
        child_type: wire_file_type(&record.child_type)?,
        attr_generation: record.attr_generation,
    })
}

fn wire_inode_attr(attr: WireInodeAttr) -> Result<InodeAttr, ClientError> {
    Ok(InodeAttr {
        inode: inode_id(attr.inode)?,
        file_type: wire_file_type(&attr.file_type)?,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        generation: attr.generation,
        mtime_ms: attr.mtime_ms,
        ctime_ms: attr.ctime_ms,
    })
}

fn wire_body(body: WireBodyDescriptor) -> Result<BodyDescriptor, ClientError> {
    Ok(BodyDescriptor {
        producer: body.producer,
        digest_uri: body.digest_uri,
        size: body.size,
        content_type: body.content_type,
        manifest_id: body.manifest_id,
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    })
}

fn wire_snapshot(snapshot: WireSnapshotPin) -> Result<SnapshotPin, ClientError> {
    Ok(SnapshotPin {
        snapshot_id: snapshot.snapshot_id,
        root: inode_id(snapshot.root)?,
        read_version: snapshot.read_version,
        created_version: snapshot.created_version,
    })
}

fn inode_id(raw: u64) -> Result<InodeId, ClientError> {
    InodeId::new(raw).map_err(|err| ClientError::Protocol(err.to_string()))
}

fn wire_file_type(raw: &str) -> Result<FileType, ClientError> {
    match raw {
        "file" => Ok(FileType::File),
        "directory" => Ok(FileType::Directory),
        "symlink" => Ok(FileType::Symlink),
        other => Err(ClientError::Protocol(format!("unknown file type {other}"))),
    }
}

fn hex_decode(raw: &str) -> Result<Vec<u8>, ClientError> {
    if !raw.len().is_multiple_of(2) {
        return Err(ClientError::Protocol(
            "hex string has odd length".to_owned(),
        ));
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.as_bytes().chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_digit(byte: u8) -> Result<u8, ClientError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ClientError::Protocol("invalid hex digit".to_owned())),
    }
}

fn unexpected_result(result: MetadataRpcResult) -> ClientError {
    ClientError::Protocol(format!("unexpected metadata rpc result {result:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    fn serve_one(body: &'static str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let request = String::from_utf8_lossy(&request);
            assert!(request.starts_with("POST /rpc HTTP/1.1"));
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        addr
    }

    fn read_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buf = [0_u8; 4096];
        loop {
            let read = stream.read(&mut buf).unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
            if let Some((body_start, content_len)) = request_body_bounds(&request) {
                if request.len() >= body_start + content_len {
                    break;
                }
            }
        }
        request
    }

    fn request_body_bounds(request: &[u8]) -> Option<(usize, usize)> {
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")?;
        let header = String::from_utf8_lossy(&request[..header_end]);
        let mut content_len = 0_usize;
        for line in header.lines().skip(1) {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            if key.eq_ignore_ascii_case("content-length") {
                content_len = value.trim().parse().unwrap();
            }
        }
        Some((header_end + 4, content_len))
    }

    #[test]
    fn remote_mkdir_sends_metadata_rpc() {
        let addr = serve_one(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_utf8":"runs","name_hex":"72756e73","child":2,"child_type":"directory","attr_generation":1},"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"size":0,"generation":1,"mtime_ms":1,"ctime_ms":1},"body":null}}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let entry = client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
        assert_eq!(entry.attr.inode.get(), 2);
        assert_eq!(entry.dentry.name.as_bytes(), b"runs");
    }

    #[test]
    fn remote_error_maps_to_client_error() {
        let addr = serve_one(r#"{"ok":false,"error":"metadata command predicate failed"}"#);
        let client = RemoteMetadataClient::connect(addr);
        let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
        assert!(
            matches!(
                err,
                ClientError::Remote(ref err) if err.contains("predicate failed")
            ),
            "unexpected error: {err:?}"
        );
    }
}
