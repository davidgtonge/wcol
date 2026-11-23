//! Spill runs on local disk. The engine asks its host to put, commit and read sorted runs
//! (`SPILL_OP_RUN_*`); in a Worker the host keeps them in R2, here they are files.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};

use wcol_engine::exec::stream_pool::StreamPoolSession;
use wcol_engine::spill::{SPILL_OP_RUN_COMMIT, SPILL_OP_RUN_PUT, SPILL_OP_RUN_READ};

/// One reducer's runs, under its own directory. Removed when the reducer is dropped.
pub struct LocalSpill {
    dir: PathBuf,
    open: HashMap<String, File>,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

impl LocalSpill {
    pub fn new(dir: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&dir).map_err(|e| format!("spill dir {}: {e}", dir.display()))?;
        Ok(Self {
            dir,
            open: HashMap::new(),
            bytes_written: 0,
            bytes_read: 0,
        })
    }

    /// Serve the spill request the session is waiting on, then hand it the answer.
    pub fn serve(&mut self, session: &mut StreamPoolSession) -> Result<(), String> {
        let result = match session.spill_op() {
            SPILL_OP_RUN_PUT => self.put(session),
            SPILL_OP_RUN_COMMIT => Ok(()),
            SPILL_OP_RUN_READ => self.read(session),
            op => Err(format!("unsupported spill op {op}")),
        };
        session
            .complete_spill(result.is_ok(), &[])
            .map_err(|e| e.to_string())?;
        result
    }

    fn put(&mut self, session: &StreamPoolSession) -> Result<(), String> {
        let part = RunPut::decode(session.spill_append_bytes())?;
        let body = session.spill_run_upload_body();
        if body.len() as u64 != part.byte_length {
            return Err(format!(
                "run {} part length {} != {}",
                part.key,
                body.len(),
                part.byte_length
            ));
        }
        let path = self.path(&part.key)?;
        let file = match self.open.entry(part.key.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                entry.insert(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .read(true)
                        .open(&path)
                        .map_err(|e| format!("open run {}: {e}", path.display()))?,
                )
            }
        };
        file.write_all(body)
            .map_err(|e| format!("write run {}: {e}", part.key))?;
        self.bytes_written += body.len() as u64;
        if part.last {
            file.flush().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn read(&mut self, session: &mut StreamPoolSession) -> Result<(), String> {
        let read = RunRead::decode(session.spill_append_bytes())?;
        let file =
            File::open(self.path(&read.key)?).map_err(|e| format!("run {}: {e}", read.key))?;
        let len = read.length as usize;
        let slot = session
            .spill_run_read_slot(len)
            .map_err(|e| e.to_string())?;
        // SAFETY: the session hands out a slot of `len` bytes it owns until `complete_spill`,
        // and nothing else touches the session while this slice lives.
        let dst = unsafe { std::slice::from_raw_parts_mut(slot, len) };
        file.read_exact_at(dst, read.offset).map_err(|e| {
            format!(
                "short spill read: {} at {}+{len}: {e}",
                read.key, read.offset
            )
        })?;
        self.bytes_read += len as u64;
        Ok(())
    }

    /// A run key is a relative path the engine names (`wcol-spill/<partition>/run-N.bin`).
    fn path(&self, key: &str) -> Result<PathBuf, String> {
        let relative = Path::new(key);
        if relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(format!("spill run key is not a plain relative path: {key}"));
        }
        Ok(self.dir.join(relative))
    }
}

impl Drop for LocalSpill {
    fn drop(&mut self) {
        self.open.clear();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// `u32` key length, key, `u32` part, `u8` last, `u64` part bytes, `u64` run records.
struct RunPut {
    key: String,
    last: bool,
    byte_length: u64,
}

/// `u32` key length, key, `u64` run bytes, `u64` offset, `u32` length.
struct RunRead {
    key: String,
    offset: u64,
    length: u32,
}

fn key_and_rest(payload: &[u8]) -> Result<(String, &[u8]), String> {
    let len = payload
        .get(..4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("4 bytes")) as usize)
        .ok_or("spill payload truncated")?;
    let key = payload.get(4..4 + len).ok_or("spill key truncated")?;
    let key = std::str::from_utf8(key).map_err(|_| "spill key is not UTF-8")?;
    Ok((key.to_string(), &payload[4 + len..]))
}

fn u64_at(bytes: &[u8], at: usize) -> Result<u64, String> {
    bytes
        .get(at..at + 8)
        .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")))
        .ok_or_else(|| "spill payload truncated".to_string())
}

impl RunPut {
    fn decode(payload: &[u8]) -> Result<Self, String> {
        let (key, rest) = key_and_rest(payload)?;
        let last = *rest.get(4).ok_or("run put truncated")? != 0;
        Ok(Self {
            key,
            last,
            byte_length: u64_at(rest, 5)?,
        })
    }
}

impl RunRead {
    fn decode(payload: &[u8]) -> Result<Self, String> {
        let (key, rest) = key_and_rest(payload)?;
        let length = rest
            .get(16..20)
            .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")))
            .ok_or("run read truncated")?;
        Ok(Self {
            key,
            offset: u64_at(rest, 8)?,
            length,
        })
    }
}
