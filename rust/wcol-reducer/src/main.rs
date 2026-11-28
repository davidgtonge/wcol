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

use std::fmt::Write as _;
use std::io::Read;
use std::path::PathBuf;
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
    /// A [`PortState`], stored as its discriminant so HTTP threads can update it lock-free.
    state: AtomicU8,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum PortState {
    Waiting = 0,
    /// The edge's request has arrived; no bytes yet.
    Opened = 1,
    Receiving = 2,
    /// The edge's channel is full: the engine has not taken this port's last chunks.
    Blocked = 3,
    Ended = 4,
    Absent = 5,
}

impl PortStat {
    fn set(&self, state: PortState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }

    fn get(&self) -> PortState {
        match self.state.load(Ordering::Relaxed) {
            0 => PortState::Waiting,
            1 => PortState::Opened,
            2 => PortState::Receiving,
            3 => PortState::Blocked,
            4 => PortState::Ended,
            _ => PortState::Absent,
        }
    }
}

impl PortState {
    /// The `/status` name of a port still open, or `None` once it has finished.
    fn open_name(self) -> Option<&'static str> {
        match self {
            Self::Waiting => Some("waiting"),
            Self::Opened => Some("opened"),
            Self::Receiving => Some("receiving"),
            Self::Blocked => Some("blocked"),
            Self::Ended | Self::Absent => None,
        }
    }
}

/// Scan edges one reducer accepts; a reducer's fan-in is one edge per scan.
const MAX_INPUTS: usize = 4096;

struct State {
    job: Mutex<Job>,
    status: Mutex<Status>,
    spill_dir: PathBuf,
}

fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let spill_dir =
        PathBuf::from(std::env::var("SPILL_DIR").unwrap_or_else(|_| "/tmp/wcol-spill".into()));
    let state = Arc::new(State {
        job: Mutex::new(Job::default()),
        status: Mutex::new(Status::Idle),
        spill_dir,
    });
    http::serve(&format!("0.0.0.0:{port}"), move |request| {
        handle(&state, request)
    })
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
        (Method::Put, route) if route.starts_with("/input/") => {
            input(state, port_of(route), request.body)
        }
        (Method::Post, route) if route.starts_with("/absent/") => {
            send(state, port_of(route), Edge::Absent)
                .map_or_else(|e| text(400, &e), |()| text(200, "ok"))
        }
        _ => text(404, "not found"),
    }
}

fn start(state: &Arc<State>, url: &str, body: &mut dyn Read) -> Reply {
    let Some(inputs) = url
        .split_once("inputs=")
        .and_then(|(_, rest)| rest.split('&').next())
        .and_then(|n| n.parse::<usize>().ok())
        .filter(|&n| n <= MAX_INPUTS)
    else {
        return text(
            400,
            &format!("start needs ?inputs=N with N <= {MAX_INPUTS}"),
        );
    };
    // Claim the process before reading the program: a second `/start` must not replace
    // the job (and its edges) of a reducer that is already running.
    {
        let mut status = state.status.lock().expect("status");
        if !matches!(*status, Status::Idle) {
            return text(409, "reducer already started");
        }
        *status = Status::Running;
    }
    match open_job(state, inputs, body) {
        Ok(()) => text(200, "started"),
        Err((code, message)) => {
            *state.status.lock().expect("status") = Status::Failed(message.clone());
            text(code, &message)
        }
    }
}

fn open_job(state: &Arc<State>, inputs: usize, body: &mut dyn Read) -> Result<(), (u16, String)> {
    let mut program = Vec::new();
    body.read_to_end(&mut program)
        .map_err(|e| (400, format!("program body: {e}")))?;
    let spill = LocalSpill::new(state.spill_dir.join(format!("{}", std::process::id())))
        .map_err(|e| (500, e))?;
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
    let state = Arc::clone(state);
    thread::spawn(move || {
        let result = engine::run(&program, edge_rx, &wake_rx, &out_tx, spill);
        // Dropping the sender ends `GET /output` only after the status is final.
        *state.status.lock().expect("status") = match result {
            Ok(report) => Status::Done(format!(
                "spillWritten={} spillRead={} spillRuns={} spillGroups={} peakLiveBytes={} peakRssKiB={}",
                report.spill_written,
                report.spill_read,
                report.spill_runs,
                report.spill_groups,
                report.peak_live_bytes,
                peak_rss_kib().map_or_else(|| "unknown".into(), |kib| kib.to_string()),
            )),
            Err(e) => Status::Failed(e),
        };
        drop(out_tx);
    });
    Ok(())
}

fn input(state: &State, port: Option<usize>, reader: &mut dyn Read) -> Reply {
    let Some(port) = port else {
        return text(400, "bad input port");
    };
    if let Some(stat) = state.job.lock().expect("job").ports.get(port) {
        stat.set(PortState::Opened);
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
            stat.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            PortState::Receiving
        }
        Edge::End => PortState::Ended,
        Edge::Absent => PortState::Absent,
    };
    stat.set(PortState::Blocked);
    sender
        .send(edge)
        .map_err(|_| "reducer is not running".to_string())?;
    stat.set(after);
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
            ports
                .iter()
                .enumerate()
                .fold("running".into(), |mut out, (i, port)| {
                    if let Some(open) = port.get().open_name() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_states_round_trip_through_the_atomic() {
        let stat = PortStat::default();
        assert_eq!(stat.get(), PortState::Waiting);
        for state in [
            PortState::Opened,
            PortState::Receiving,
            PortState::Blocked,
            PortState::Ended,
            PortState::Absent,
        ] {
            stat.set(state);
            assert_eq!(stat.get(), state);
        }
    }
}
