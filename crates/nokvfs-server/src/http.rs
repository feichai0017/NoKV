use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::server::{Server, ServerError};

const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    status: &'static str,
    content_type: &'static str,
    body: String,
}

pub(crate) fn handle_stream(server: &Server, mut stream: TcpStream) -> Result<(), ServerError> {
    stream
        .set_read_timeout(Some(CONTROL_IO_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .set_write_timeout(Some(CONTROL_IO_TIMEOUT))
        .map_err(ServerError::Io)?;
    let mut buf = [0_u8; 4096];
    let read = stream.read(&mut buf).map_err(ServerError::Io)?;
    let request = String::from_utf8_lossy(&buf[..read]);
    let response = handle_request_line(server, request.lines().next().unwrap_or_default());
    write_response(&mut stream, &response).map_err(ServerError::Io)
}

pub(crate) fn handle_request_line(server: &Server, line: &str) -> HttpResponse {
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    match (method, path) {
        ("GET", "/healthz") => HttpResponse::text("200 OK", "ok\n"),
        ("GET", "/readyz") => HttpResponse::text("200 OK", "ready\n"),
        ("GET", "/stats") => HttpResponse::json("200 OK", server.stats_json()),
        ("GET", "/gc") | ("POST", "/gc") => match server.run_manual_gc() {
            Ok(body) => HttpResponse::json("200 OK", body),
            Err(err) => HttpResponse::text("500 Internal Server Error", format!("{err}\n")),
        },
        _ => HttpResponse::text("404 Not Found", "not found\n"),
    }
}

impl HttpResponse {
    fn text(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
        }
    }

    fn json(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: body.into(),
        }
    }
}

fn write_response(out: &mut impl Write, response: &HttpResponse) -> io::Result<()> {
    write!(
        out,
        "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response.status,
        response.content_type,
        response.body.len(),
        response.body
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_server;

    #[test]
    fn health_endpoint_is_plain_text() {
        let server = test_server();
        let response = handle_request_line(&server, "GET /healthz HTTP/1.1");
        assert_eq!(response.status, "200 OK");
        assert_eq!(response.content_type, "text/plain; charset=utf-8");
        assert_eq!(response.body, "ok\n");
    }

    #[test]
    fn stats_endpoint_reports_object_counters() {
        let server = test_server();
        let response = handle_request_line(&server, "GET /stats HTTP/1.1");
        assert_eq!(response.status, "200 OK");
        assert!(response.body.contains("\"object_puts\":0"));
        assert!(response.body.contains("\"block_cache_enabled\":true"));
    }

    #[test]
    fn unknown_endpoint_returns_404() {
        let server = test_server();
        let response = handle_request_line(&server, "GET /missing HTTP/1.1");
        assert_eq!(response.status, "404 Not Found");
    }
}
