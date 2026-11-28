//! Bounded dictionary storage for one column chunk (dict page → data page resolve).

use compact_parquet_thrift::pages::{Location, PageEvent, Value};

use super::decode::ScanLimits;
use super::Error;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Bytes(&'a [u8]),
}

#[derive(Clone, Copy)]
enum Entry {
    Int(i64),
    Float(f64),
    Bytes { off: usize, len: usize },
}

struct ByteBuild {
    expected: u32,
    filled: usize,
    start: usize,
}

/// Chunk-scoped dictionary: bounded entry count and total byte arena.
pub struct DictionaryStore {
    entries: Vec<Entry>,
    arena: Vec<u8>,
    max_entries: usize,
    max_arena_bytes: usize,
    byte_build: Option<ByteBuild>,
}

impl DictionaryStore {
    pub fn with_limits(limits: ScanLimits) -> Self {
        let max_arena = limits.budget.dictionary_bytes.min(usize::MAX);
        Self {
            entries: Vec::new(),
            arena: Vec::new(),
            max_entries: limits.max_dictionary_entries,
            max_arena_bytes: max_arena,
            byte_build: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn begin_dict_page(&mut self) {
        self.entries.clear();
        self.arena.clear();
        self.byte_build = None;
    }

    pub fn commit_dict_page(&mut self) -> Result<(), Error> {
        if self.byte_build.is_some() {
            return Err(Error::Message("dictionary page ended mid byte array"));
        }
        Ok(())
    }

    pub fn on_dict_event(&mut self, event: PageEvent<'_>) -> Result<(), Error> {
        match event {
            PageEvent::Null(_) => Err(Error::Message("dictionary page emitted a null")),
            PageEvent::Value(location, value) => {
                let _row = match location {
                    Location::Row(r) | Location::Dictionary(r) => r,
                };
                match value {
                    Value::Boolean(v) => self.push_int(if v { 1 } else { 0 }),
                    Value::I32(v) => self.push_int(i64::from(v)),
                    Value::I64(v) => self.push_int(v),
                    Value::F32(v) => self.push_float(f64::from(v)),
                    Value::F64(v) => self.push_float(v),
                    Value::BytesStart(len) => {
                        if self.byte_build.is_some() {
                            return Err(Error::Message("nested byte array in dictionary"));
                        }
                        let start = self.arena.len();
                        let need = self.arena.len().saturating_add(len as usize);
                        if need > self.max_arena_bytes {
                            return Err(Error::Message("dictionary byte arena exceeds budget"));
                        }
                        self.arena.resize(need, 0);
                        self.byte_build = Some(ByteBuild {
                            expected: len,
                            filled: 0,
                            start,
                        });
                        Ok(())
                    }
                    Value::BytesChunk(chunk) => {
                        let build = self
                            .byte_build
                            .as_mut()
                            .ok_or(Error::Message("dictionary chunk without start"))?;
                        let end = build.start + build.filled + chunk.len();
                        if end > self.arena.len() {
                            return Err(Error::Message("dictionary chunk out of arena bounds"));
                        }
                        self.arena[build.start + build.filled..end].copy_from_slice(chunk);
                        build.filled += chunk.len();
                        Ok(())
                    }
                    Value::BytesEnd => {
                        let build = self
                            .byte_build
                            .take()
                            .ok_or(Error::Message("dictionary end without start"))?;
                        if build.filled != build.expected as usize {
                            return Err(Error::Message("dictionary byte length mismatch"));
                        }
                        self.push_bytes_entry(build.start, build.filled)
                    }
                    _ => Err(Error::Message("unsupported dictionary value")),
                }
            }
        }
    }

    fn check_budget(&self, extra_bytes: usize) -> Result<(), Error> {
        if self.resident_bytes().saturating_add(extra_bytes) > self.max_arena_bytes {
            return Err(Error::Message("dictionary exceeds configured byte budget"));
        }
        Ok(())
    }

    /// Approximate live bytes (entries + arena + vector overhead is excluded).
    pub fn resident_bytes(&self) -> usize {
        self.arena
            .len()
            .saturating_add(self.entries.len() * core::mem::size_of::<Entry>())
    }

    fn push_int(&mut self, v: i64) -> Result<(), Error> {
        if self.entries.len() >= self.max_entries {
            return Err(Error::Message("dictionary exceeds configured entry budget"));
        }
        self.check_budget(0)?;
        self.entries.push(Entry::Int(v));
        Ok(())
    }

    fn push_float(&mut self, v: f64) -> Result<(), Error> {
        if self.entries.len() >= self.max_entries {
            return Err(Error::Message("dictionary exceeds configured entry budget"));
        }
        self.check_budget(0)?;
        self.entries.push(Entry::Float(v));
        Ok(())
    }

    fn push_bytes_entry(&mut self, off: usize, len: usize) -> Result<(), Error> {
        if self.entries.len() >= self.max_entries {
            return Err(Error::Message("dictionary exceeds configured entry budget"));
        }
        self.entries.push(Entry::Bytes { off, len });
        Ok(())
    }

    pub fn resolve(&self, id: u32) -> Result<ValueRef<'_>, Error> {
        let entry = self
            .entries
            .get(id as usize)
            .ok_or(Error::Message("dictionary index out of range"))?;
        match *entry {
            Entry::Int(v) => Ok(ValueRef::Int(v)),
            Entry::Float(v) => Ok(ValueRef::Float(v)),
            Entry::Bytes { off, len } => {
                let slice = self
                    .arena
                    .get(
                        off..off
                            .checked_add(len)
                            .ok_or(Error::Message("dictionary bytes corrupt"))?,
                    )
                    .ok_or(Error::Message("dictionary bytes corrupt"))?;
                Ok(ValueRef::Bytes(slice))
            }
        }
    }

    /// Map a data-page event to a logical row for integer/bool numeric batches.
    pub fn data_row_for_numeric(
        &self,
        event: PageEvent<'_>,
        numeric_int_only: bool,
    ) -> Result<Option<(i64, u8)>, Error> {
        match event {
            PageEvent::Null(_) => Ok(Some((0, 0))),
            PageEvent::Value(location, value) => {
                if matches!(location, Location::Dictionary(_)) {
                    return Err(Error::Message("data page emitted a dictionary location"));
                }
                match value {
                    Value::Boolean(v) => Ok(Some((if v { 1 } else { 0 }, 1))),
                    Value::I32(v) => Ok(Some((i64::from(v), 1))),
                    Value::I64(v) => Ok(Some((v, 1))),
                    Value::F32(_) | Value::F64(_) => {
                        Err(Error::Message("float value in integer scan"))
                    }
                    Value::DictionaryId(id) => match self.resolve(id)? {
                        ValueRef::Int(v) => Ok(Some((v, 1))),
                        ValueRef::Bytes(_) if numeric_int_only => {
                            Err(Error::Message("byte dictionary entry in numeric column"))
                        }
                        ValueRef::Bytes(_) => {
                            Err(Error::Message("byte dictionary entry in numeric column"))
                        }
                        ValueRef::Bool(v) => Ok(Some((i64::from(v), 1))),
                        ValueRef::Float(_) => {
                            Err(Error::Message("float dictionary entry in integer column"))
                        }
                        ValueRef::Null => Err(Error::Message("null dictionary entry")),
                    },
                    _ => Err(Error::Message("unsupported page value for numeric batch")),
                }
            }
        }
    }

    /// Map a data-page event to a logical `f64` (FLOAT widens exactly).
    pub fn data_row_for_float(&self, event: PageEvent<'_>) -> Result<Option<(f64, u8)>, Error> {
        match event {
            PageEvent::Null(_) => Ok(Some((0.0, 0))),
            PageEvent::Value(location, value) => {
                if matches!(location, Location::Dictionary(_)) {
                    return Err(Error::Message("data page emitted a dictionary location"));
                }
                match value {
                    Value::F32(v) => Ok(Some((f64::from(v), 1))),
                    Value::F64(v) => Ok(Some((v, 1))),
                    Value::DictionaryId(id) => match self.resolve(id)? {
                        ValueRef::Float(v) => Ok(Some((v, 1))),
                        _ => Err(Error::Message("non-float dictionary entry in float column")),
                    },
                    _ => Err(Error::Message("unsupported page value for float batch")),
                }
            }
        }
    }

    /// Data-page row as dictionary id + validity (no value expansion).
    pub fn data_dict_id_row(&self, event: PageEvent<'_>) -> Result<Option<(u32, u8)>, Error> {
        match event {
            PageEvent::Null(_) => Ok(Some((0, 0))),
            PageEvent::Value(location, value) => {
                if matches!(location, Location::Dictionary(_)) {
                    return Err(Error::Message("data page emitted a dictionary location"));
                }
                match value {
                    Value::DictionaryId(id) => {
                        self.resolve(id)?;
                        Ok(Some((id, 1)))
                    }
                    Value::Boolean(_) | Value::I32(_) | Value::I64(_) | Value::F32(_) | Value::F64(_) => {
                        Err(Error::Message(
                            "plain encoded value on dictionary-id data page (mixed encoding not supported yet)",
                        ))
                    }
                    _ => Err(Error::Message("unsupported page value for dictionary-id batch")),
                }
            }
        }
    }

    /// Copy integer/bool dictionary entries for WCOL dictionary install (no data-page ids).
    pub fn copy_int_entries(&self) -> Result<Vec<i64>, Error> {
        let mut out = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            match entry {
                Entry::Int(v) => out.push(*v),
                Entry::Float(_) => {
                    return Err(Error::Message("float dictionary entry in int export"))
                }
                Entry::Bytes { .. } => {
                    return Err(Error::Message("byte dictionary entry in int export"))
                }
            }
        }
        Ok(out)
    }

    /// Copy FLOAT/DOUBLE dictionary entries widened to `f64`.
    pub fn copy_float_entries(&self) -> Result<Vec<f64>, Error> {
        let mut out = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            match entry {
                Entry::Float(v) => out.push(*v),
                Entry::Int(_) | Entry::Bytes { .. } => {
                    return Err(Error::Message("non-float dictionary entry in float export"))
                }
            }
        }
        Ok(out)
    }

    /// Copy byte dictionary entries into one arena (`offsets.len() == entries + 1`).
    pub fn copy_byte_entries(&self) -> Result<(Vec<u32>, Vec<u8>), Error> {
        let mut offsets = Vec::with_capacity(self.entries.len() + 1);
        offsets.push(0);
        let mut bytes = Vec::new();
        for entry in &self.entries {
            match entry {
                Entry::Bytes { off, len } => {
                    let end = off
                        .checked_add(*len)
                        .ok_or(Error::Message("dictionary bytes corrupt"))?;
                    let slice = self
                        .arena
                        .get(*off..end)
                        .ok_or(Error::Message("dictionary bytes corrupt"))?;
                    bytes.extend_from_slice(slice);
                    offsets.push(
                        u32::try_from(bytes.len())
                            .map_err(|_| Error::Message("dictionary byte arena overflow"))?,
                    );
                }
                Entry::Int(_) | Entry::Float(_) => {
                    return Err(Error::Message("numeric dictionary entry in byte export"))
                }
            }
        }
        Ok((offsets, bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_parquet_thrift::pages::{Location, PageEvent, Value};

    #[test]
    fn resolve_rejects_out_of_range_id() {
        let store = DictionaryStore::with_limits(ScanLimits::for_batch_rows(4));
        assert!(store.resolve(0).is_err());
    }

    #[test]
    fn dict_bytes_arena_bounded() {
        let limits = ScanLimits {
            max_dictionary_entries: 4,
            budget: crate::formats::parquet::ingest_memory::ScanMemoryBudget {
                dictionary_bytes: 8,
                ..Default::default()
            },
            ..ScanLimits::for_batch_rows(4)
        };
        let mut store = DictionaryStore::with_limits(limits);
        store.begin_dict_page();
        store
            .on_dict_event(PageEvent::Value(
                Location::Dictionary(0),
                Value::BytesStart(10),
            ))
            .unwrap_err();
    }
}
