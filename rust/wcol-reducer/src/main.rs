//! A hash reducer served over HTTP: one reducer per process (one per container).
//!
//! - `POST /start?inputs=N`: body is the reducer program; opens the reducer.
//! - `PUT /input/{i}`: a scan edge's stream, streamed in; its end ends port `i`.
//! - `POST /absent/{i}`: scan `i` finished without opening this partition.
//! - `GET /output`: the reducer's output stream; ends when the reducer does.
//! - `GET /status`: `running` (with each open port), `ok` or `error: ...`; the host checks it
//!   once output ends.
//! - `POST /stop`: exit now; the container stops with the process.
//! - `GET /health`: liveness.

mod engine;
mod http;
mod spill;

use std::io::Read;
use std::path::PathBuf;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use engine::Edge;
use http::{Method, Reply, Request};
use spill::LocalSpill;

#[global_allocator]
static ALLOCATOR: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

/// Chunks in flight per edge before its HTTP reader waits: backpressure back to the scan.
const EDGE_CHUNKS: usize = 8;
const OUTPUT_CHUNKS: usize = 8;
const READ_CHUNK: usize = 64 * 1024;

#[derive(Clone, Debug)]
enum Status {
    Idle,
    Running,
    Done(String),
    Failed(String),
}

#[derive(Default)]
struct Job {
    edges: Vec<Option<SyncSender<Edge>>>,
    wake: Option<SyncSender<()>>,
    output: Option<Receiver<Vec<u8>>>,
    ports: Arc<Vec<PortStat>>,
}

/// What one input port has received, for `/status` while the reducer runs.
#[derive(Default)]
struct PortStat {
    bytes: AtomicU64,
    state: AtomicU8,
    /// Epoch ms the edge's request arrived, its first bytes arrived, and it ended (0: not yet).
    opened_ms: AtomicU64,
    first_ms: AtomicU64,
    ended_ms: AtomicU64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// `i:opened:first:ended` per input port, epoch ms, for the edge timeline.
fn port_times(ports: &[PortStat]) -> String {
    ports.iter().enumerate().fold(String::new(), |mut out, (i, port)| {
        let _ = write!(
            out,
            "{}{i}:{}:{}:{}",
            if out.is_empty() { "" } else { "," },
            port.opened_ms.load(Ordering::Relaxed),
            port.first_ms.load(Ordering::Relaxed),
            port.ended_ms.load(Ordering::Relaxed)
        );
        out
    })
}

const PORT_WAITING: u8 = 0;
const PORT_RECEIVING: u8 = 1;
/// The edge's request has arrived; no bytes yet.
const PORT_OPENED: u8 = 5;
/// The edge's channel is full: the engine has not taken this port's last chunks.
const PORT_BLOCKED: u8 = 2;
const PORT_ENDED: u8 = 3;
const PORT_ABSENT: u8 = 4;

struct State {
    /// Epoch ms each TCP connection to the reducer port was first seen in `/proc/net/tcp`.
    connections: Mutex<std::collections::HashMap<String, u64>>,
    job: Mutex<Job>,
    status: Mutex<Status>,
    spill_dir: PathBuf,
}

fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let spill_dir =
        PathBuf::from(std::env::var("SPILL_DIR").unwrap_or_else(|_| "/tmp/wcol-spill".into()));
    let state = Arc::new(State {
        connections: Mutex::new(std::collections::HashMap::new()),
        job: Mutex::new(Job::default()),
        status: Mutex::new(Status::Idle),
        spill_dir,
    });
    let sampler = Arc::clone(&state);
    let port_hex = format!("{:04X}", port.parse::<u16>().unwrap_or(8080));
    thread::spawn(move || sample_connections(&sampler, &port_hex));
    let served = Arc::clone(&state);
    http::serve(&format!("0.0.0.0:{port}"), move |request| handle(&served, request))
        .expect("serve reducer port");
}

fn handle(state: &Arc<State>, request: Request<'_>) -> Reply {
    let url = request.target;
    let path = url.split('?').next().unwrap_or("");
    match (request.method, path) {
        (Method::Get, "/health") => text(200, "ok"),
        (Method::Get, "/status") => text(200, &status_text(state)),
        (Method::Post, "/stop") => std::process::exit(0),
        (Method::Post, "/start") => start(state, &url, request.body),
        (Method::Get, "/output") => output(state),
        (Method::Put, route) if route.starts_with("/input/") => input(state, port_of(route), request.body),
        (Method::Post, route) if route.starts_with("/absent/") => {
            send(state, port_of(route), Edge::Absent).map_or_else(|e| text(400, &e), |()| text(200, "ok"))
        }
        _ => text(404, "not found"),
    }
}

fn start(state: &Arc<State>, url: &str, body: &mut dyn Read) -> Reply {
    let Some(inputs) = url
        .split_once("inputs=")
        .and_then(|(_, rest)| rest.split('&').next())
        .and_then(|n| n.parse::<usize>().ok())
    else {
        return text(400, "start needs ?inputs=N");
    };
    let mut program = Vec::new();
    if let Err(e) = body.read_to_end(&mut program) {
        return text(400, &format!("program body: {e}"));
    }
    let spill = match LocalSpill::new(state.spill_dir.join(format!("{}", std::process::id()))) {
        Ok(spill) => spill,
        Err(e) => return text(500, &e),
    };
    let (edge_tx, edge_rx): (Vec<_>, Vec<_>) =
        (0..inputs).map(|_| sync_channel(EDGE_CHUNKS)).unzip();
    let (wake_tx, wake_rx) = sync_channel(1);
    let (out_tx, out_rx) = sync_channel(OUTPUT_CHUNKS);
    *state.job.lock().expect("job") = Job {
        edges: edge_tx.into_iter().map(Some).collect(),
        wake: Some(wake_tx),
        output: Some(out_rx),
        ports: Arc::new((0..inputs).map(|_| PortStat::default()).collect()),
    };
    *state.status.lock().expect("status") = Status::Running;
    let state = Arc::clone(state);
    thread::spawn(move || {
        let result = engine::run(&program, edge_rx, &wake_rx, &out_tx, spill);
        // Dropping the sender ends `GET /output` only after the status is final.
        *state.status.lock().expect("status") = match result {
            Ok(report) => Status::Done(format!(
                "spillWritten={} spillRead={} spillRuns={} spillGroups={} peakLiveBytes={} peakRssKiB={} net={} doneMs={} conns={} edges={}",
                report.spill_written,
                report.spill_read,
                report.spill_runs,
                report.spill_groups,
                report.peak_live_bytes,
                peak_rss_kib().map_or_else(|| "unknown".into(), |kib| kib.to_string()),
                listen_counters(),
                now_ms(),
                connection_times(&state),
                port_times(&state.job.lock().expect("job").ports)
            )),
            Err(e) => Status::Failed(e),
        };
        drop(out_tx);
    });
    text(200, "started")
}

fn input(state: &State, port: Option<usize>, reader: &mut dyn Read) -> Reply {
    let Some(port) = port else {
        return text(400, "bad input port");
    };
    if let Some(stat) = state.job.lock().expect("job").ports.get(port) {
        stat.state.store(PORT_OPENED, Ordering::Relaxed);
        stat.opened_ms.store(now_ms(), Ordering::Relaxed);
    }
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if let Err(e) = send(state, Some(port), Edge::Bytes(buf[..n].to_vec())) {
                    return text(409, &e);
                }
            }
            Err(e) => return text(400, &format!("edge {port} read: {e}")),
        }
    }
    send(state, Some(port), Edge::End).map_or_else(|e| text(409, &e), |()| text(200, "ok"))
}

/// Deliver to port `i`'s edge and wake the engine. Blocks while the edge is full.
fn send(state: &State, port: Option<usize>, edge: Edge) -> Result<(), String> {
    let port = port.ok_or("bad port")?;
    let (sender, wake, ports) = {
        let job = state.job.lock().expect("job");
        let sender = job
            .edges
            .get(port)
            .and_then(Clone::clone)
            .ok_or_else(|| format!("no input port {port}"))?;
        (sender, job.wake.clone(), Arc::clone(&job.ports))
    };
    let stat = &ports[port];
    let after = match &edge {
        Edge::Bytes(bytes) => {
            if stat.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed) == 0 {
                stat.first_ms.store(now_ms(), Ordering::Relaxed);
            }
            PORT_RECEIVING
        }
        Edge::End => {
            stat.ended_ms.store(now_ms(), Ordering::Relaxed);
            PORT_ENDED
        }
        Edge::Absent => PORT_ABSENT,
    };
    stat.state.store(PORT_BLOCKED, Ordering::Relaxed);
    sender
        .send(edge)
        .map_err(|_| "reducer is not running".to_string())?;
    stat.state.store(after, Ordering::Relaxed);
    if let Some(wake) = wake {
        let _ = wake.try_send(());
    }
    Ok(())
}

fn output(state: &State) -> Reply {
    let Some(output) = state.job.lock().expect("job").output.take() else {
        return text(409, "no reducer output to read");
    };
    let body = ChannelReader {
        output,
        chunk: Vec::new(),
        at: 0,
    };
    Reply::Stream(Box::new(body))
}

/// The reducer's output chunks as one byte stream; ends when the engine drops its sender.
struct ChannelReader {
    output: Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    at: usize,
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.at == self.chunk.len() {
            match self.output.recv() {
                Ok(chunk) => {
                    self.chunk = chunk;
                    self.at = 0;
                }
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.chunk.len() - self.at);
        buf[..n].copy_from_slice(&self.chunk[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Diagnostic: record when each connection to the reducer port first appears, every 20 ms,
/// to tell connections that reach the container late from requests the server reads late.
fn sample_connections(state: &State, port_hex: &str) {
    let local = format!(":{port_hex} ");
    loop {
        let now = now_ms();
        for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
            let Ok(text) = std::fs::read_to_string(table) else { continue };
            let mut seen = state.connections.lock().expect("connections");
            for line in text.lines().skip(1) {
                let fields: Vec<&str> = line.split_whitespace().collect();
                // local_address remote_address st: a listening socket has an all-zero remote.
                if let [_, local_addr, remote, ..] = fields.as_slice() {
                    let listening = remote.trim_start_matches('0').starts_with(':');
                    if !listening && format!("{local_addr} ").contains(&local) {
                        seen.entry((*remote).to_string()).or_insert(now);
                    }
                }
            }
        }
        thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// First-seen times of the reducer port's connections, sorted, epoch ms.
fn connection_times(state: &State) -> String {
    let mut times: Vec<u64> = state.connections.lock().expect("connections").values().copied().collect();
    times.sort_unstable();
    times.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
}

/// The kernel's listen-queue counters and limits, to tell dropped connection attempts from
/// slow ones: `name:value` pairs, or `unavailable` where the sandbox hides them.
fn listen_counters() -> String {
    const WANTED: [&str; 4] = ["ListenOverflows", "ListenDrops", "TCPReqQFullDrop", "TCPReqQFullDoCookies"];
    let netstat = std::fs::read_to_string("/proc/net/netstat").unwrap_or_default();
    let lines: Vec<&str> = netstat.lines().filter(|line| line.starts_with("TcpExt:")).collect();
    let mut out = match lines.as_slice() {
        [names, values] => names
            .split_whitespace()
            .zip(values.split_whitespace())
            .filter(|(name, _)| WANTED.contains(name))
            .map(|(name, value)| format!("{name}:{value}"))
            .collect::<Vec<_>>()
            .join(","),
        _ => "unavailable".into(),
    };
    for (name, path) in [("somaxconn", "/proc/sys/net/core/somaxconn"), ("synBacklog", "/proc/sys/net/ipv4/tcp_max_syn_backlog")] {
        let value = std::fs::read_to_string(path).map_or_else(|_| "?".into(), |v| v.trim().to_string());
        let _ = write!(out, ",{name}:{value}");
    }
    out
}

/// The process's peak resident memory (`VmHWM`), where Linux reports it.
fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .ok()
}

fn port_of(route: &str) -> Option<usize> {
    route.rsplit('/').next().and_then(|n| n.parse().ok())
}

fn status_text(state: &State) -> String {
    match &*state.status.lock().expect("status") {
        Status::Idle => "idle".into(),
        Status::Running => {
            let ports = Arc::clone(&state.job.lock().expect("job").ports);
            // Only the ports still open: `i:state:bytes`.
            ports.iter().enumerate().fold("running".into(), |mut out, (i, port)| {
                let open = match port.state.load(Ordering::Relaxed) {
                    PORT_WAITING => Some("waiting"),
                    PORT_OPENED => Some("opened"),
                    PORT_RECEIVING => Some("receiving"),
                    PORT_BLOCKED => Some("blocked"),
                    _ => None,
                };
                if let Some(open) = open {
                    let _ = write!(out, " {i}:{open}:{}", port.bytes.load(Ordering::Relaxed));
                }
                out
            })
        }
        Status::Done(report) => format!("ok {report}"),
        Status::Failed(error) => format!("error: {error}"),
    }
}

fn text(code: u16, body: &str) -> Reply {
    Reply::Text(code, body.to_string())
}
