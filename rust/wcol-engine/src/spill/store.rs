//! The [`SpillStore`] contract, and the native SQLite [`FileAppendSpillStore`] tests use.
//!
//! Finalize reads rows in `(key_blob, seq)` order so Rust can merge partials for the
//! same key, one bounded page at a time from a [`SpillScanResume`] cursor.

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use rusqlite::{params, params_from_iter, Connection};

/// Rows per multi-value INSERT (matches the DO host chunk).
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
const INSERT_CHUNK_ROWS: usize = 100;

/// Resume marker after a bounded batch (tuple keyset fallback).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpillScanResume {
    /// False until the first batch has been issued.
    pub started: bool,
    pub key_blob: Vec<u8>,
    pub seq: i64,
}

/// `(seq, key, payload)` of one spilled row.
pub type KeyedRow = (i64, Vec<u8>, Vec<u8>);

/// `(key, payload)` of one row in an append batch.
pub type KeyPayload = (Vec<u8>, Vec<u8>);

#[derive(Clone, Debug)]
pub struct SpillScanBatch {
    pub rows: Vec<KeyedRow>,
    /// No further rows after this batch (empty batch implies exhausted).
    pub exhausted: bool,
}

pub trait SpillStore {
    fn begin(&mut self) -> Result<crate::spill::SpillProgress<()>, String>;
    fn append_partials(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<crate::spill::SpillProgress<()>, String>;

    /// Prepare to read spill rows in merge order. Idempotent while a scan is already open.
    fn begin_ordered_scan(&mut self) -> Result<crate::spill::SpillProgress<()>, String>;

    /// Next merge-order chunk. Updates `resume` from the last row returned.
    fn next_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<crate::spill::SpillProgress<SpillScanBatch>, String>;

    fn end_ordered_scan(&mut self) -> Result<(), String>;
    fn end(&mut self) -> Result<crate::spill::SpillProgress<()>, String>;

    /// Commit the current flush as one immutable sorted run (R2 / in-memory backends).
    fn finalize_run(&mut self) -> Result<crate::spill::SpillProgress<()>, String> {
        Ok(crate::spill::SpillProgress::Ready(()))
    }

    /// True while a Cloudflare host round-trip is still in flight for this store.
    fn host_op_pending(&self) -> bool {
        false
    }

    /// Apply bytes from a completed host read (R2 merge path). No-op for SQLite/file stores.
    fn ingest_host_read_completion(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// The next run holds exactly `rows` records. The R2 store writes the count in the run
    /// header before streaming the run's first part, and sizes its part buffer from it.
    fn expect_run_rows(&mut self, _rows: usize) {}

    /// Host landed an uploaded run part and gave its buffer back.
    fn on_run_put_handoff(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Host acknowledged [`super::SPILL_OP_RUN_COMMIT`]; register the staged run (R2 path).
    fn on_run_commit_host_complete(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// More [`finalize_run`] work after a prior commit/put step (R2 multi-run flush).
    fn needs_finish_run(&self) -> bool {
        false
    }

    /// Immutable sorted runs registered for merge (R2 path).
    fn committed_run_count(&self) -> usize {
        0
    }

    /// Encoded rows and upload buffers not yet committed as a run.
    fn pending_write_bytes(&self) -> usize {
        0
    }

    /// Decoded records and read cursors held for the ordered merge.
    fn merge_buffer_bytes(&self) -> usize {
        0
    }
}

pub(crate) fn pair_bytes(rows: &[(Vec<u8>, Vec<u8>)]) -> usize {
    rows.iter().fold(0usize, |sum, (key, value)| {
        sum.saturating_add(key.capacity())
            .saturating_add(value.capacity())
    })
}

pub(crate) fn keyed_row_bytes(rows: &[(i64, Vec<u8>, Vec<u8>)]) -> usize {
    rows.iter().fold(0usize, |sum, (_, key, value)| {
        sum.saturating_add(key.capacity())
            .saturating_add(value.capacity())
    })
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub struct FileAppendSpillStore {
    path: std::path::PathBuf,
    conn: Option<Connection>,
    scan_open: bool,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl FileAppendSpillStore {
    pub fn open(path: std::path::PathBuf) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let conn = Connection::open(&path).map_err(|e| e.to_string())?;
        Ok(Self {
            path,
            conn: Some(conn),
            scan_open: false,
        })
    }
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
fn fetch_ordered_batch(
    conn: &Connection,
    resume: &mut SpillScanResume,
    max_rows: usize,
    max_bytes: usize,
) -> Result<SpillScanBatch, String> {
    if max_rows == 0 {
        return Ok(SpillScanBatch {
            rows: Vec::new(),
            exhausted: true,
        });
    }

    let mut rows_out: Vec<(i64, Vec<u8>, Vec<u8>)> = if !resume.started {
        let mut stmt = conn
            .prepare(
                "SELECT seq, key_blob, partial FROM spill_partial
                 ORDER BY key_blob, seq
                 LIMIT ?1",
            )
            .map_err(|e| e.to_string())?;
        let mapped = stmt
            .query_map(params![max_rows as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT seq, key_blob, partial FROM spill_partial
                 WHERE (key_blob, seq) > (?1, ?2)
                 ORDER BY key_blob, seq
                 LIMIT ?3",
            )
            .map_err(|e| e.to_string())?;
        let mapped = stmt
            .query_map(
                params![resume.key_blob, resume.seq, max_rows as i64],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .map_err(|e| e.to_string())?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?
    };

    let sql_count = rows_out.len();
    trim_batch_to_byte_cap(&mut rows_out, max_bytes);
    let exhausted = rows_out.is_empty() || (sql_count < max_rows);
    if let Some((seq, key, _)) = rows_out.last() {
        resume.started = true;
        resume.seq = *seq;
        resume.key_blob.clone_from(key);
    } else if !resume.started {
        resume.started = true;
    }

    Ok(SpillScanBatch {
        rows: rows_out,
        exhausted,
    })
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
fn trim_batch_to_byte_cap(rows: &mut Vec<(i64, Vec<u8>, Vec<u8>)>, max_bytes: usize) {
    if max_bytes == 0 {
        rows.clear();
        return;
    }
    let mut used = 0usize;
    let mut keep = 0usize;
    for (_seq, key, partial) in rows.iter() {
        let row_bytes = key.len().saturating_add(partial.len());
        if keep > 0 && used.saturating_add(row_bytes) > max_bytes {
            break;
        }
        used = used.saturating_add(row_bytes);
        keep += 1;
    }
    rows.truncate(keep);
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl SpillStore for FileAppendSpillStore {
    fn begin(&mut self) -> Result<super::SpillProgress<()>, String> {
        let conn = self.conn.as_ref().ok_or("spill store closed")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS spill_partial (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                key_blob BLOB NOT NULL,
                partial BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS spill_partial_key ON spill_partial (key_blob, seq);",
        )
        .map_err(|e| e.to_string())?;
        Ok(super::SpillProgress::Ready(()))
    }

    fn append_partials(
        &mut self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<super::SpillProgress<()>, String> {
        let conn = self.conn.as_ref().ok_or("spill store closed")?;
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        for chunk in rows.chunks(INSERT_CHUNK_ROWS) {
            let mut sql = String::from("INSERT INTO spill_partial (key_blob, partial) VALUES ");
            let mut bind: Vec<&[u8]> = Vec::with_capacity(chunk.len() * 2);
            for (i, (key, partial)) in chunk.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                let n = i * 2;
                sql.push_str(&format!("(?{}, ?{})", n + 1, n + 2));
                bind.push(key.as_slice());
                bind.push(partial.as_slice());
            }
            tx.execute(&sql, params_from_iter(bind))
                .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(super::SpillProgress::Ready(()))
    }

    fn begin_ordered_scan(&mut self) -> Result<super::SpillProgress<()>, String> {
        self.scan_open = true;
        Ok(super::SpillProgress::Ready(()))
    }

    fn next_ordered_batch(
        &mut self,
        resume: &mut SpillScanResume,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<super::SpillProgress<SpillScanBatch>, String> {
        if !self.scan_open {
            return Err("spill ordered scan not started".to_string());
        }
        let conn = self.conn.as_ref().ok_or("spill store closed")?;
        Ok(super::SpillProgress::Ready(fetch_ordered_batch(
            conn, resume, max_rows, max_bytes,
        )?))
    }

    fn end_ordered_scan(&mut self) -> Result<(), String> {
        self.scan_open = false;
        Ok(())
    }

    fn end(&mut self) -> Result<super::SpillProgress<()>, String> {
        let _ = self.end_ordered_scan();
        if let Some(conn) = self.conn.take() {
            conn.execute("DROP TABLE IF EXISTS spill_partial", [])
                .map_err(|e| e.to_string())?;
            drop(conn);
            let _ = std::fs::remove_file(&self.path);
        }
        Ok(super::SpillProgress::Ready(()))
    }
}

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn ordered_batches_merge_same_key_across_pages() {
        let path = std::env::temp_dir().join(format!(
            "wcol-spill-scan-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut store = FileAppendSpillStore::open(path.clone()).unwrap();
        store.begin().unwrap();
        let key_a = b"A".to_vec();
        let key_b = b"B".to_vec();
        store
            .append_partials(&[
                (key_a.clone(), b"p1".to_vec()),
                (key_b.clone(), b"p2".to_vec()),
                (key_a.clone(), b"p3".to_vec()),
                (key_a.clone(), b"p4".to_vec()),
            ])
            .unwrap();
        store.begin_ordered_scan().unwrap();
        let mut resume = SpillScanResume::default();
        let page1 = store
            .next_ordered_batch(&mut resume, 2, 1024)
            .unwrap()
            .expect_ready("page 1");
        assert_eq!(page1.rows.len(), 2);
        assert_eq!(page1.rows[0].0, 1);
        assert_eq!(page1.rows[1].0, 3);
        assert!(!page1.exhausted);
        let page2 = store
            .next_ordered_batch(&mut resume, 2, 1024)
            .unwrap()
            .expect_ready("page 2");
        assert_eq!(page2.rows.len(), 2);
        assert_eq!(page2.rows[0].0, 4);
        assert_eq!(page2.rows[1].0, 2);
        assert!(!page2.exhausted);
        let page3 = store
            .next_ordered_batch(&mut resume, 2, 1024)
            .unwrap()
            .expect_ready("page 3");
        assert!(page3.rows.is_empty());
        assert!(page3.exhausted);
        let keys: Vec<_> = page1
            .rows
            .iter()
            .chain(page2.rows.iter())
            .map(|(_, k, _)| k.clone())
            .collect();
        assert_eq!(
            keys,
            vec![key_a.clone(), key_a.clone(), key_a.clone(), key_b]
        );
        store.end().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_partials_chunks_multi_value_insert() {
        let path = std::env::temp_dir().join(format!(
            "wcol-spill-insert-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut store = FileAppendSpillStore::open(path.clone()).unwrap();
        store.begin().unwrap();
        let n = INSERT_CHUNK_ROWS + 50;
        let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
            .map(|i| (i.to_le_bytes().to_vec(), b"p".to_vec()))
            .collect();
        store.append_partials(&rows).unwrap();
        store.begin_ordered_scan().unwrap();
        let mut resume = SpillScanResume::default();
        let batch = store
            .next_ordered_batch(&mut resume, n + 1, 1024 * 1024)
            .unwrap()
            .expect_ready("insert page");
        assert_eq!(batch.rows.len(), n);
        assert!(batch.exhausted);
        store.end().unwrap();
        let _ = std::fs::remove_file(path);
    }
}
