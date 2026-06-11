use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::ClientError;

pub(super) const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC3\n";
const FRAME_HEADER_BYTES: usize = 16;
const MAX_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct PipelinedConnection {
    writer: Mutex<TcpStream>,
    pending: Arc<Mutex<HashMap<u64, mpsc::Sender<PendingFrame>>>>,
}

enum PendingFrame {
    Payload(Vec<u8>),
    Failed(String),
}

impl PipelinedConnection {
    pub(super) fn connect(address: SocketAddr) -> Result<Self, ClientError> {
        let mut stream =
            TcpStream::connect(address).map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .write_all(FRAMED_RPC_MAGIC)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let reader = stream
            .try_clone()
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let connection = Self {
            writer: Mutex::new(stream),
            pending: Arc::new(Mutex::new(HashMap::new())),
        };
        connection.spawn_reader(reader);
        Ok(connection)
    }

    fn spawn_reader(&self, mut reader: TcpStream) {
        let pending = Arc::clone(&self.pending);
        thread::spawn(move || loop {
            match read_frame(&mut reader) {
                Ok((request_id, _flags, payload)) => {
                    let waiter = pending
                        .lock()
                        .expect("metadata rpc pending")
                        .remove(&request_id);
                    if let Some(waiter) = waiter {
                        let _ = waiter.send(PendingFrame::Payload(payload));
                    }
                }
                Err(err) => {
                    let mut pending = pending.lock().expect("metadata rpc pending");
                    let waiters = pending
                        .drain()
                        .map(|(_, waiter)| waiter)
                        .collect::<Vec<_>>();
                    drop(pending);
                    for waiter in waiters {
                        let _ = waiter.send(PendingFrame::Failed(err.to_string()));
                    }
                    break;
                }
            }
        });
    }

    pub(super) fn call(
        &self,
        request_id: u64,
        body: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, ClientError> {
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .expect("metadata rpc pending")
            .insert(request_id, tx);
        let write_result = {
            let mut writer = self.writer.lock().expect("metadata rpc writer");
            write_frame(&mut writer, request_id, 0, body)
        };
        if let Err(err) = write_result {
            self.pending
                .lock()
                .expect("metadata rpc pending")
                .remove(&request_id);
            return Err(err);
        }
        match rx.recv_timeout(timeout) {
            Ok(PendingFrame::Payload(payload)) => Ok(payload),
            Ok(PendingFrame::Failed(err)) => Err(ClientError::Io(err)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.pending
                    .lock()
                    .expect("metadata rpc pending")
                    .remove(&request_id);
                Err(ClientError::Io(
                    "metadata rpc response timed out".to_owned(),
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(ClientError::Io("metadata rpc connection closed".to_owned()))
            }
        }
    }
}

pub(super) fn write_frame(
    stream: &mut TcpStream,
    request_id: u64,
    flags: u32,
    body: &[u8],
) -> Result<(), ClientError> {
    let len = u32::try_from(body.len())
        .map_err(|_| ClientError::Protocol("metadata rpc request exceeds u32".to_owned()))?;
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[0..8].copy_from_slice(&request_id.to_be_bytes());
    header[8..12].copy_from_slice(&flags.to_be_bytes());
    header[12..16].copy_from_slice(&len.to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(body))
        .map_err(|err| ClientError::Io(err.to_string()))
}

pub(super) fn read_frame(stream: &mut TcpStream) -> Result<(u64, u32, Vec<u8>), ClientError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    stream.read_exact(&mut header).map_err(rpc_read_error)?;
    let request_id = u64::from_be_bytes(header[0..8].try_into().expect("request id header"));
    let flags = u32::from_be_bytes(header[8..12].try_into().expect("flags header"));
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_RPC_RESPONSE_BYTES {
        return Err(ClientError::Protocol(
            "metadata rpc response exceeds size limit".to_owned(),
        ));
    }
    let mut body = vec![0_u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    Ok((request_id, flags, body))
}

fn rpc_read_error(err: io::Error) -> ClientError {
    ClientError::Io(err.to_string())
}
