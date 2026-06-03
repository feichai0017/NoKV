use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::rpc;
use crate::server::{Server, ServerError};

const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct HttpRequest {
    line: String,
    body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    status: &'static str,
    content_type: &'static str,
    body: String,
    keep_alive: bool,
}

pub(crate) fn handle_stream(server: &Server, mut stream: TcpStream) -> Result<(), ServerError> {
    stream
        .set_read_timeout(Some(CONTROL_IO_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .set_write_timeout(Some(CONTROL_IO_TIMEOUT))
        .map_err(ServerError::Io)?;
    loop {
        let request = match read_request(&mut stream) {
            Ok(request) => request,
            Err(ServerError::Io(err)) if is_idle_timeout(&err) => return Ok(()),
            Err(err) => return Err(err),
        };
        if request.line.is_empty() && request.body.is_empty() {
            return Ok(());
        }
        let response = handle_request(server, &request);
        write_response(&mut stream, &response).map_err(ServerError::Io)?;
        if !response.keep_alive {
            return Ok(());
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, ServerError> {
    let mut bytes = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buf).map_err(ServerError::Io)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buf[..read]);
        if bytes.len() > MAX_CONTROL_REQUEST_BYTES {
            return Ok(HttpRequest {
                line: "GET /request-too-large HTTP/1.1".to_owned(),
                body: Vec::new(),
            });
        }
        if let Some((header_end, content_len)) = header_end_and_content_len(&bytes) {
            let body_start = header_end + 4;
            let expected = body_start.saturating_add(content_len);
            while bytes.len() < expected {
                let read = stream.read(&mut buf).map_err(ServerError::Io)?;
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..read]);
                if bytes.len() > MAX_CONTROL_REQUEST_BYTES {
                    return Ok(HttpRequest {
                        line: "GET /request-too-large HTTP/1.1".to_owned(),
                        body: Vec::new(),
                    });
                }
            }
            let header = String::from_utf8_lossy(&bytes[..header_end]);
            let line = header.lines().next().unwrap_or_default().to_owned();
            let body_end = bytes.len().min(expected);
            return Ok(HttpRequest {
                line,
                body: bytes[body_start..body_end].to_vec(),
            });
        }
    }

    let line = String::from_utf8_lossy(&bytes)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    Ok(HttpRequest {
        line,
        body: Vec::new(),
    })
}

fn header_end_and_content_len(bytes: &[u8]) -> Option<(usize, usize)> {
    let header_end = bytes.windows(4).position(|window| window == b"\r\n\r\n")?;
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let mut content_len = 0_usize;
    for line in header.lines().skip(1) {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("content-length") {
            content_len = value.trim().parse().unwrap_or(0);
        }
    }
    Some((header_end, content_len))
}

fn handle_request(server: &Server, request: &HttpRequest) -> HttpResponse {
    handle_parts(server, &request.line, &request.body)
}

pub(crate) fn handle_parts(server: &Server, line: &str, body: &[u8]) -> HttpResponse {
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    match (method, path) {
        ("GET", "/healthz") => HttpResponse::text("200 OK", "ok\n"),
        ("GET", "/readyz") => HttpResponse::text("200 OK", "ready\n"),
        ("GET", "/stats") => HttpResponse::json("200 OK", server.stats_json()),
        ("POST", "/rpc") => HttpResponse::json_keep_alive("200 OK", rpc::handle_rpc(server, body)),
        ("GET", "/gc") | ("POST", "/gc") => match server.run_manual_gc() {
            Ok(body) => HttpResponse::json("200 OK", body),
            Err(err) => HttpResponse::text("500 Internal Server Error", format!("{err}\n")),
        },
        (_, "/request-too-large") => {
            HttpResponse::text("413 Payload Too Large", "request too large\n")
        }
        _ => HttpResponse::text("404 Not Found", "not found\n"),
    }
}

impl HttpResponse {
    fn text(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
            keep_alive: false,
        }
    }

    fn json(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: body.into(),
            keep_alive: false,
        }
    }

    fn json_keep_alive(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: body.into(),
            keep_alive: true,
        }
    }
}

fn write_response(out: &mut impl Write, response: &HttpResponse) -> io::Result<()> {
    let connection = if response.keep_alive {
        "keep-alive"
    } else {
        "close"
    };
    write!(
        out,
        "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: {}\r\n\r\n{}",
        response.status,
        response.content_type,
        response.body.len(),
        connection,
        response.body
    )
}

fn is_idle_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_server;

    #[test]
    fn health_endpoint_is_plain_text() {
        let server = test_server();
        let response = handle_parts(&server, "GET /healthz HTTP/1.1", &[]);
        assert_eq!(response.status, "200 OK");
        assert_eq!(response.content_type, "text/plain; charset=utf-8");
        assert_eq!(response.body, "ok\n");
    }

    #[test]
    fn stats_endpoint_reports_object_counters() {
        let server = test_server();
        let response = handle_parts(&server, "GET /stats HTTP/1.1", &[]);
        assert_eq!(response.status, "200 OK");
        assert!(response.body.contains("\"object_puts\":0"));
        assert!(response.body.contains("\"block_cache_enabled\":true"));
    }

    #[test]
    fn unknown_endpoint_returns_404() {
        let server = test_server();
        let response = handle_parts(&server, "GET /missing HTTP/1.1", &[]);
        assert_eq!(response.status, "404 Not Found");
    }

    #[test]
    fn rpc_endpoint_executes_metadata_request() {
        let server = test_server();
        let response = handle_parts(
            &server,
            "POST /rpc HTTP/1.1",
            br#"{"op":"create_dir","parent":1,"name":"runs","mode":493,"uid":1000,"gid":1000}"#,
        );
        assert_eq!(response.status, "200 OK");
        assert!(response.body.contains("\"ok\":true"));
        assert!(response.body.contains("\"name_utf8\":\"runs\""));
    }
}
