//! A minimal HTTP/1.1 server with one thread per connection.
//!
//! A scan edge is a request that streams for seconds, so a server must never make one
//! connection wait for another. `tiny_http` queued new connections behind open ones: when a
//! reducer's 33 connections arrived at once, some waited until an earlier edge's connection
//! closed, about 5 s after that edge ended.
//!
//! It speaks what the reducer's one client (its Durable Object) sends: request bodies with a
//! length or chunked, keep-alive, and replies that are short text or a chunked stream.

use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

const MAX_HEAD_BYTES: usize = 64 * 1024;
const MAX_HEADERS: usize = 32;
const STREAM_CHUNK: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Put,
    Post,
    Other,
}

pub struct Request<'a> {
    pub method: Method,
    /// Path and query, as sent.
    pub target: String,
    pub body: &'a mut dyn Read,
}

pub enum Reply {
    Text(u16, String),
    /// Sent chunked as it is read; the reply ends when the reader does.
    Stream(Box<dyn Read + Send>),
}

/// Serve `handler` on `addr`, each connection on its own thread.
pub fn serve<F>(addr: &str, handler: F) -> io::Result<()>
where
    F: Fn(Request<'_>) -> Reply + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr)?;
    let handler = Arc::new(handler);
    for stream in listener.incoming().flatten() {
        let handler = Arc::clone(&handler);
        thread::spawn(move || {
            let _ = connection(stream, &*handler);
        });
    }
    Ok(())
}

/// Requests on one keep-alive connection, in order.
fn connection(stream: TcpStream, handler: &dyn Fn(Request<'_>) -> Reply) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut writer = BufWriter::new(stream.try_clone()?);
    let mut reader = BufReader::with_capacity(STREAM_CHUNK, stream);
    while let Some(head) = read_head(&mut reader)? {
        let head = Head::parse(&head)?;
        if head.expect_continue {
            writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
            writer.flush()?;
        }
        let reply = {
            let mut body: Box<dyn Read + '_> = if head.chunked {
                Box::new(Chunked {
                    inner: &mut reader,
                    left: 0,
                    done: false,
                })
            } else {
                Box::new((&mut reader).take(head.length))
            };
            let reply = handler(Request {
                method: head.method,
                target: head.target,
                body: &mut *body,
            });
            // The next request starts after this body, whatever the handler read.
            io::copy(&mut body, &mut io::sink())?;
            reply
        };
        write_reply(&mut writer, reply)?;
        if head.close {
            break;
        }
    }
    Ok(())
}

/// What the reducer needs from a request head.
struct Head {
    method: Method,
    target: String,
    length: u64,
    chunked: bool,
    close: bool,
    expect_continue: bool,
}

impl Head {
    fn parse(bytes: &[u8]) -> io::Result<Self> {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut request = httparse::Request::new(&mut headers);
        match request.parse(bytes).map_err(|e| invalid(&e.to_string()))? {
            httparse::Status::Complete(_) => {}
            httparse::Status::Partial => return Err(invalid("incomplete request head")),
        }
        let header = |name: &str| {
            request
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case(name))
                .map(|h| String::from_utf8_lossy(h.value).trim().to_ascii_lowercase())
        };
        Ok(Self {
            method: match request.method {
                Some("GET") => Method::Get,
                Some("PUT") => Method::Put,
                Some("POST") => Method::Post,
                _ => Method::Other,
            },
            target: request.path.unwrap_or("/").to_string(),
            length: header("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            chunked: header("transfer-encoding").is_some_and(|v| v.contains("chunked")),
            close: header("connection").is_some_and(|v| v == "close"),
            expect_continue: header("expect").is_some_and(|v| v == "100-continue"),
        })
    }
}

/// The request head through its blank line, or `None` if the connection closed between requests.
fn read_head(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut head = Vec::new();
    loop {
        if reader.read_until(b'\n', &mut head)? == 0 {
            return if head.is_empty() {
                Ok(None)
            } else {
                Err(invalid("connection closed mid-head"))
            };
        }
        if head == b"\r\n" {
            head.clear(); // a stray blank line between requests
        } else if head.ends_with(b"\r\n\r\n") {
            return Ok(Some(head));
        } else if head.len() > MAX_HEAD_BYTES {
            return Err(invalid("request head too large"));
        }
    }
}

/// A chunked request body, read through to its last chunk and trailers.
struct Chunked<R> {
    inner: R,
    left: u64,
    done: bool,
}

impl<R: BufRead> Read for Chunked<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done || buf.is_empty() {
            return Ok(0);
        }
        if self.left == 0 {
            let mut line = String::new();
            self.inner.read_line(&mut line)?;
            let size = line.split(';').next().unwrap_or("").trim();
            self.left = u64::from_str_radix(size, 16).map_err(|_| invalid("bad chunk size"))?;
            if self.left == 0 {
                // Trailers, if any, end with a blank line.
                loop {
                    line.clear();
                    if self.inner.read_line(&mut line)? == 0 || line.trim().is_empty() {
                        break;
                    }
                }
                self.done = true;
                return Ok(0);
            }
        }
        let max = buf
            .len()
            .min(usize::try_from(self.left).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..max])?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.left -= n as u64;
        if self.left == 0 {
            let mut crlf = [0u8; 2];
            self.inner.read_exact(&mut crlf)?;
        }
        Ok(n)
    }
}

fn write_reply(writer: &mut impl Write, reply: Reply) -> io::Result<()> {
    match reply {
        Reply::Text(code, body) => {
            write!(
                writer,
                "HTTP/1.1 {code} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
                if code < 400 { "OK" } else { "Error" },
                body.len()
            )?;
        }
        Reply::Stream(mut source) => {
            writer.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")?;
            writer.flush()?;
            let mut chunk = vec![0u8; STREAM_CHUNK];
            loop {
                let n = source.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                write!(writer, "{n:x}\r\n")?;
                writer.write_all(&chunk[..n])?;
                writer.write_all(b"\r\n")?;
                writer.flush()?;
            }
            writer.write_all(b"0\r\n\r\n")?;
        }
    }
    writer.flush()
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
