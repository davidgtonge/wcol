//! Owned decoder cursor: one plain-page buffer, resumable value decode without replay.

use compact_parquet_thrift::pages::Error as PageError;
use compact_parquet_thrift::pages::{
    page_layout, ColumnSpec, Location, PageEvent, PageKind, PageLayout, Physical, Value,
};
use compact_parquet_thrift::parquet::PageHeader;
use compact_parquet_thrift::values::{
    ByteArrayEvent, DeltaBinary, DeltaByteArrays, DeltaByteParts, DeltaLengthByteArrays,
    DictionaryIds, Levels, PlainBool, PlainValues, RleBool,
};

use super::decode::ScanLimits;
use super::Error;

/// Per-page decode checkpoint (no references into page bytes).
#[derive(Debug)]
pub struct PageCursor {
    layout: PageLayout,
    spec: ColumnSpec,
    present: Vec<u8>,
    level: Option<Levels>,
    level_count: usize,
    physical_count: usize,
    /// Values read from the value decoder so far (one per present row).
    values_read: usize,
    values: Option<ValueDecoder>,
    rows: RowCursorState,
    pub plain_offset: usize,
    events_emitted: u64,
    /// Byte-array bytes the next [`Self::step`] may emit; see [`Self::set_byte_budget`].
    byte_budget: Option<ByteBudget>,
}

#[derive(Clone, Copy, Debug)]
struct ByteBudget {
    bytes: usize,
    /// Emit the first value even when it alone exceeds `bytes` (nothing else is pending), so a
    /// single long value cannot stall the scan; `max_value_bytes` still bounds it.
    allow_first: bool,
}

#[derive(Debug)]
struct RowCursorState {
    count: usize,
    index: usize,
    dictionary: bool,
    optional: bool,
}

#[derive(Debug)]
enum ValueDecoder {
    Bool(PlainBool),
    BoolRle(RleBool),
    I32(PlainValues<i32>),
    I64(PlainValues<i64>),
    Dictionary(DictionaryIds),
    /// PLAIN BYTE_ARRAY (`u32` length, then bytes) read straight from the inflated page.
    Bytes {
        remaining: u64,
        max_value_bytes: u32,
    },
    /// PLAIN FIXED_LEN_BYTE_ARRAY.
    FixedBytes {
        remaining: u64,
        width: u32,
    },
    F32(PlainValues<f32>),
    F64(PlainValues<f64>),
    /// Whole DELTA_BINARY_PACKED page, then one value per step.
    DeltaI32 {
        expected: u64,
        values: Vec<i32>,
        at: usize,
        primed: bool,
    },
    DeltaI64 {
        expected: u64,
        values: Vec<i64>,
        at: usize,
        primed: bool,
    },
    /// DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY, materialized once.
    DeltaBytes {
        expected: u64,
        max_value_bytes: u32,
        values: Vec<Vec<u8>>,
        at: usize,
        primed: bool,
        prefixes: bool,
    },
}

/// One decoded value: a scalar, or a whole byte string borrowed from the page.
enum Decoded<'a> {
    Value(Value<'a>),
    Bytes(&'a [u8]),
    Owned(Vec<u8>),
}

/// What the event handler wants after an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    Continue,
    /// End this step once the current value is complete. The handler took the value but
    /// its batch is now full: lengths the cursor cannot peek (dictionary ids, a length
    /// prefix split across reads, an unprimed delta page) bypass the byte budget.
    StopAfterValue,
}

impl Flow {
    fn and(self, next: Flow) -> Flow {
        if self == Flow::StopAfterValue {
            self
        } else {
            next
        }
    }
}

/// Result of one bounded decode step on an owned page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageStep {
    /// Accepted up to `max_output_rows` logical output rows this call.
    Emitted(usize),
    /// All values in the page have been decoded.
    PageComplete,
    /// Output capacity was zero or exhausted before the page finished.
    Blocked,
}

pub struct ActivePlainPage {
    pub is_dict: bool,
    pub plain: Vec<u8>,
    pub cursor: PageCursor,
}

impl PageCursor {
    pub fn new(header: PageHeader, spec: ColumnSpec, limits: &ScanLimits) -> Result<Self, Error> {
        let layout = page_layout(header, spec).map_err(Error::Page)?;
        if layout.count > limits.max_page_values {
            return Err(Error::Message(
                "parquet page exceeds configured value budget",
            ));
        }
        let optional = spec.max_definition_level == 1 && layout.kind != PageKind::Dictionary;
        let needed = layout.count.div_ceil(8);
        let present = vec![0u8; needed.max(1)];
        let level = if optional {
            Some(match layout.kind {
                PageKind::V2 => Levels::v2(1, layout.count as u64, layout.v2_levels_bytes as u32)
                    .map_err(|e| Error::Page(e.into()))?,
                _ => Levels::v1(
                    1,
                    layout.count as u64,
                    layout.uncompressed_bytes.min(u32::MAX as usize) as u32,
                )
                .map_err(|e| Error::Page(e.into()))?,
            })
        } else {
            None
        };
        Ok(Self {
            layout,
            spec,
            present,
            level,
            level_count: 0,
            physical_count: if optional { 0 } else { layout.count },
            values_read: 0,
            byte_budget: None,
            values: None,
            rows: RowCursorState {
                count: layout.count,
                index: 0,
                dictionary: layout.kind == PageKind::Dictionary,
                optional,
            },
            plain_offset: 0,
            events_emitted: 0,
        })
    }

    /// Limit the byte-array bytes the next step emits: it stops before a value that would go
    /// past `bytes`, ending the batch early instead of overrunning the batch byte budget.
    pub fn set_byte_budget(&mut self, bytes: usize, allow_first: bool) {
        self.byte_budget = Some(ByteBudget { bytes, allow_first });
    }

    /// Cursor counters for stall diagnostics.
    pub fn debug_state(&self) -> String {
        format!(
            "rows {}/{} optional={} levels {}/{} level_open={} physical={} values_complete={:?} plain_offset={}",
            self.rows.index,
            self.rows.count,
            self.rows.optional,
            self.level_count,
            self.layout.count,
            self.level.is_some(),
            self.physical_count,
            self.values.as_ref().map(|v| v.is_complete()),
            self.plain_offset,
        )
    }

    pub fn page_complete(&self) -> bool {
        if self.layout.count == 0 {
            return self.level.is_none();
        }
        if self.rows.index < self.rows.count {
            return false;
        }
        // Once every present row has its value the page is done, even if the decoder still
        // holds bytes: RLE/bit-packed runs pad the last group past the page's value count.
        self.level.is_none()
            && (self.values_read >= self.physical_count
                || self.values.as_ref().is_none_or(|v| v.is_complete()))
    }

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }

    /// Absorb the page's definition levels and build the value decoder. Returns the offset of
    /// the values in `input`, or `None` while the levels are still incomplete.
    fn prepare(&mut self, input: &[u8]) -> Result<Option<usize>, Error> {
        let mut at = 0usize;
        if let Some(level) = &mut self.level {
            let count = self.layout.count;
            let bits = &mut self.present;
            let levels = &mut self.level_count;
            let physical = &mut self.physical_count;
            let mut err = None;
            let used = level
                .feed(input, |value| {
                    if *levels >= count {
                        err = Some(PageError::TooManyValues);
                        return;
                    }
                    if value == 1 {
                        bits[*levels >> 3] |= 1 << (*levels & 7);
                        *physical += 1;
                    }
                    *levels += 1;
                })
                .map_err(|e| Error::Page(e.into()))?;
            at += used;
            if err.is_some() {
                return Err(Error::Message("too many definition levels"));
            }
            if !level.is_complete() {
                self.plain_offset += at;
                return Ok(None);
            }
            if self.level_count != count {
                return Err(Error::Message("definition level count mismatch"));
            }
            self.level = None;
            self.values = Some(self.build_values()?);
        }

        if self.level.is_none() && self.values.is_none() {
            self.values = Some(self.build_values()?);
        }
        Ok(Some(at))
    }

    /// Bulk form of [`Self::step`] for a dictionary-encoded data page whose rows the caller keeps
    /// as dictionary ids. Appends up to `max_output_rows` ids and validity bytes (NULL rows get id
    /// 0, validity 0), decoding each run of present rows in one call instead of one event per row.
    /// Returns `None` when the page is not a dictionary-id data page; use [`Self::step`] then.
    pub fn step_dictionary_ids(
        &mut self,
        plain: &[u8],
        max_output_rows: usize,
        ids: &mut Vec<u32>,
        nulls: &mut Vec<u8>,
    ) -> Result<Option<PageStep>, Error> {
        if self.rows.dictionary || !matches!(self.layout.encoding, 2 | 8) {
            return Ok(None);
        }
        self.step_runs(
            plain,
            max_output_rows,
            nulls,
            |decoder, input, run| match decoder {
                None => {
                    ids.resize(ids.len() + run, 0);
                    Ok(Some((0, run)))
                }
                Some(ValueDecoder::Dictionary(d)) => {
                    let before = ids.len();
                    let used = d
                        .feed_limited(input, run, |id| ids.push(id))
                        .map_err(|e| Error::Page(e.into()))?;
                    Ok(Some((used, ids.len() - before)))
                }
                Some(_) => Ok(None),
            },
        )
    }

    /// Bulk form of [`Self::step`] for a PLAIN INT32 or INT64 data page whose rows the caller
    /// keeps as `i64`. Same contract as [`Self::step_dictionary_ids`]; NULL rows get value 0.
    pub fn step_plain_i64(
        &mut self,
        plain: &[u8],
        max_output_rows: usize,
        values: &mut Vec<i64>,
        nulls: &mut Vec<u8>,
    ) -> Result<Option<PageStep>, Error> {
        if self.rows.dictionary
            || self.layout.encoding != 0
            || !matches!(self.spec.physical, Physical::Int32 | Physical::Int64)
        {
            return Ok(None);
        }
        self.step_runs(plain, max_output_rows, nulls, |decoder, input, run| {
            let before = values.len();
            let used = match decoder {
                None => {
                    values.resize(values.len() + run, 0);
                    return Ok(Some((0, run)));
                }
                Some(ValueDecoder::I32(d)) => {
                    d.feed_limited(input, run, |v| values.push(i64::from(v)))
                }
                Some(ValueDecoder::I64(d)) => d.feed_limited(input, run, |v| values.push(v)),
                Some(_) => return Ok(None),
            }
            .map_err(|e| Error::Page(e.into()))?;
            Ok(Some((used, values.len() - before)))
        })
    }

    /// Shared loop of the bulk steps: walks rows in runs of equal presence and makes one `decode`
    /// call per run. For a present run it gets the page's value decoder and returns
    /// `Some((bytes used, values decoded))`, or `None` when that decoder is not the kind the caller
    /// handles; for a NULL run it gets `None` and appends placeholders.
    fn step_runs(
        &mut self,
        plain: &[u8],
        max_output_rows: usize,
        nulls: &mut Vec<u8>,
        mut decode: impl FnMut(
            Option<&mut ValueDecoder>,
            &[u8],
            usize,
        ) -> Result<Option<(usize, usize)>, Error>,
    ) -> Result<Option<PageStep>, Error> {
        if self.byte_budget.is_some() {
            return Ok(None);
        }
        if self.page_complete() {
            self.plain_offset = plain.len();
            return Ok(Some(PageStep::PageComplete));
        }
        if max_output_rows == 0 {
            return Ok(Some(PageStep::Blocked));
        }
        let input = plain.get(self.plain_offset..).unwrap_or(&[]);
        let Some(at) = self.prepare(input)? else {
            return Ok(Some(PageStep::Blocked));
        };
        let value_slice = input.get(at..).unwrap_or(&[]);
        let mut value_at = 0usize;
        let mut output_rows = 0usize;
        while output_rows < max_output_rows && self.rows.index < self.rows.count {
            let start = self.rows.index;
            let room = (max_output_rows - output_rows).min(self.rows.count - start);
            // Rows from `start` that share one presence state.
            let present = self.rows.is_present(start, &self.present);
            let mut run = 1;
            while run < room && self.rows.is_present(start + run, &self.present) == present {
                run += 1;
            }
            let got = if present {
                let decoder = self
                    .values
                    .as_mut()
                    .ok_or(Error::Message("values missing"))?;
                if decoder.is_complete() {
                    return Err(Error::Message(
                        "optional row marked present but values exhausted",
                    ));
                }
                let Some((used, got)) = decode(
                    Some(decoder),
                    value_slice.get(value_at..).unwrap_or(&[]),
                    run,
                )?
                else {
                    if output_rows == 0 && value_at == 0 {
                        return Ok(None);
                    }
                    return Err(Error::Message("page decoder changed during a bulk step"));
                };
                value_at += used;
                nulls.resize(nulls.len() + got, 1);
                self.values_read += got;
                got
            } else {
                decode(None, &[], run)?;
                nulls.resize(nulls.len() + run, 0);
                run
            };
            self.rows.index += got;
            self.events_emitted += got as u64;
            output_rows += got;
            if got < run {
                break;
            }
        }
        self.plain_offset += at + value_at;
        if self.page_complete() {
            self.plain_offset = plain.len();
        }
        Ok(Some(match (output_rows, self.page_complete()) {
            (0, true) => PageStep::PageComplete,
            (0, false) => PageStep::Blocked,
            (rows, _) => PageStep::Emitted(rows),
        }))
    }

    /// Decode at most `max_output_rows` values into the handler. The underlying PLAIN/RLE
    /// decoders stop before consuming bytes beyond the last accepted value.
    pub fn step<F>(
        &mut self,
        plain: &[u8],
        max_output_rows: usize,
        mut on_event: F,
    ) -> Result<PageStep, Error>
    where
        F: FnMut(PageEvent<'_>) -> Result<Flow, Error>,
    {
        if self.page_complete() {
            self.plain_offset = plain.len();
            return Ok(PageStep::PageComplete);
        }
        if max_output_rows == 0 {
            return Ok(PageStep::Blocked);
        }

        let before_events = self.events_emitted;
        let mut flow = Flow::Continue;
        let input = plain.get(self.plain_offset..).unwrap_or(&[]);
        let Some(mut at) = self.prepare(input)? else {
            return Ok(PageStep::Blocked);
        };
        let values = self
            .values
            .as_mut()
            .ok_or(Error::Message("values missing"))?;
        let mut output_rows = 0usize;
        let value_slice = input.get(at..).unwrap_or(&[]);
        let mut value_at = 0usize;
        let mut step_bytes = 0usize;
        loop {
            if output_rows >= max_output_rows || flow == Flow::StopAfterValue {
                break;
            }
            if self.rows.index >= self.rows.count {
                break;
            }
            let row = self.rows.index;
            if self.rows.optional && !self.rows.is_present(row, &self.present) {
                flow = on_event(PageEvent::Null(row as u32))?;
                self.events_emitted += 1;
                self.rows.index += 1;
                output_rows += 1;
                continue;
            }
            if values.is_complete() {
                return Err(Error::Message(
                    "optional row marked present but values exhausted",
                ));
            }
            let chunk = value_slice.get(value_at..).unwrap_or(&[]);
            if let (Some(budget), Some(len)) = (self.byte_budget, values.peek_value_len(chunk)) {
                let first = output_rows == 0 && budget.allow_first;
                if step_bytes.saturating_add(len) > budget.bytes && !first {
                    break;
                }
                step_bytes += len;
            }
            let (used, physical) = values.feed_one(chunk)?;
            value_at += used;
            if physical.is_none() {
                break;
            }
            self.values_read += 1;
            let at_loc = if self.rows.dictionary {
                Location::Dictionary(row as u32)
            } else {
                Location::Row(row as u32)
            };
            flow = match physical.unwrap() {
                Decoded::Value(value) => on_event(PageEvent::Value(at_loc, value))?,
                // Same event shape the page assembler produces for byte arrays.
                Decoded::Bytes(bytes) => emit_bytes(&mut on_event, at_loc, bytes)?,
                Decoded::Owned(bytes) => emit_bytes(&mut on_event, at_loc, &bytes)?,
            };
            self.events_emitted += 1;
            self.rows.index += 1;
            output_rows += 1;
        }
        if values.is_complete() {
            while output_rows < max_output_rows
                && self.rows.index < self.rows.count
                && flow == Flow::Continue
            {
                let row = self.rows.index;
                if self.rows.is_present(row, &self.present) {
                    return Err(Error::Message("trailing present row without value"));
                }
                flow = on_event(PageEvent::Null(row as u32))?;
                self.events_emitted += 1;
                self.rows.index += 1;
                output_rows += 1;
            }
        }
        at += value_at;
        self.plain_offset += at;

        if output_rows > 0 {
            if self.page_complete() {
                self.plain_offset = plain.len();
            }
            return Ok(PageStep::Emitted(output_rows));
        }
        if self.page_complete() {
            self.plain_offset = plain.len();
            return Ok(PageStep::PageComplete);
        }
        if self.events_emitted == before_events {
            return Ok(PageStep::Blocked);
        }
        Ok(PageStep::Blocked)
    }
}

/// Whether [`PageCursor`] can decode this data page for the scan's value mode.
pub fn owned_cursor_supports_data_page(
    physical: Physical,
    encoding: i32,
    dictionary_ids: bool,
) -> bool {
    if dictionary_ids {
        matches!(
            (encoding, physical),
            (0, Physical::Boolean)
                | (0, Physical::Int32)
                | (0, Physical::Int64)
                | (0, Physical::Float)
                | (0, Physical::Double)
                | (0, Physical::ByteArray)
                | (0, Physical::FixedByteArray(_))
                | (3, Physical::Boolean)
                | (2 | 8, _)
        )
    } else {
        matches!(
            (encoding, physical),
            (0, Physical::Boolean)
                | (0, Physical::Int32)
                | (0, Physical::Int64)
                | (0, Physical::Float)
                | (0, Physical::Double)
                | (0, Physical::ByteArray)
                | (0, Physical::FixedByteArray(_))
                | (3, Physical::Boolean)
                | (5, Physical::Int32)
                | (5, Physical::Int64)
                | (6 | 7, Physical::ByteArray)
        )
    }
}

fn emit_bytes(
    on_event: &mut impl FnMut(PageEvent<'_>) -> Result<Flow, Error>,
    at_loc: Location,
    bytes: &[u8],
) -> Result<Flow, Error> {
    let mut flow = on_event(PageEvent::Value(
        at_loc,
        Value::BytesStart(bytes.len() as u32),
    ))?;
    if !bytes.is_empty() {
        flow = flow.and(on_event(PageEvent::Value(
            at_loc,
            Value::BytesChunk(bytes),
        ))?);
    }
    flow = flow.and(on_event(PageEvent::Value(at_loc, Value::BytesEnd))?);
    Ok(flow)
}

impl PageCursor {
    fn build_values(&self) -> Result<ValueDecoder, Error> {
        let count64 = self.physical_count as u64;
        let encoding = self.layout.encoding;
        Ok(match (encoding, self.spec.physical) {
            (0, Physical::Boolean) => ValueDecoder::Bool(PlainBool::new(count64)),
            (0, Physical::Int32) => ValueDecoder::I32(PlainValues::new(count64)),
            (0, Physical::Int64) => ValueDecoder::I64(PlainValues::new(count64)),
            (0, Physical::ByteArray) => ValueDecoder::Bytes {
                remaining: count64,
                max_value_bytes: self.spec.max_value_bytes,
            },
            (0, Physical::FixedByteArray(width)) => {
                if width == 0 {
                    return Err(Error::Message("fixed byte array width is zero"));
                }
                ValueDecoder::FixedBytes {
                    remaining: count64,
                    width,
                }
            }
            (0, Physical::Float) => ValueDecoder::F32(PlainValues::new(count64)),
            (0, Physical::Double) => ValueDecoder::F64(PlainValues::new(count64)),
            (5, Physical::Int32) => ValueDecoder::DeltaI32 {
                expected: count64,
                values: Vec::new(),
                at: 0,
                primed: count64 == 0,
            },
            (5, Physical::Int64) => ValueDecoder::DeltaI64 {
                expected: count64,
                values: Vec::new(),
                at: 0,
                primed: count64 == 0,
            },
            (6, Physical::ByteArray) => ValueDecoder::DeltaBytes {
                expected: count64,
                max_value_bytes: self.spec.max_value_bytes,
                values: Vec::new(),
                at: 0,
                primed: count64 == 0,
                prefixes: false,
            },
            (7, Physical::ByteArray) => ValueDecoder::DeltaBytes {
                expected: count64,
                max_value_bytes: self.spec.max_value_bytes,
                values: Vec::new(),
                at: 0,
                primed: count64 == 0,
                prefixes: true,
            },
            (3, Physical::Boolean) => ValueDecoder::BoolRle(
                RleBool::new(
                    count64,
                    self.spec.max_page_bytes.min(u32::MAX as usize) as u32,
                )
                .map_err(|e| Error::Page(e.into()))?,
            ),
            (2 | 8, _) => ValueDecoder::Dictionary(
                DictionaryIds::new(
                    count64,
                    self.spec
                        .dictionary_size
                        .ok_or(Error::Message("missing dictionary size"))?,
                )
                .map_err(|e| Error::Page(e.into()))?,
            ),
            _ => {
                return Err(Error::Message(
                    "page encoding not supported by owned cursor yet",
                ))
            }
        })
    }
}

impl RowCursorState {
    fn is_present(&self, row: usize, present: &[u8]) -> bool {
        !self.optional || (present[row >> 3] & (1 << (row & 7))) != 0
    }
}

impl ValueDecoder {
    /// Byte length of the next byte-array value, if this decoder yields byte arrays.
    fn peek_value_len(&self, input: &[u8]) -> Option<usize> {
        match self {
            Self::Bytes { remaining, .. } if *remaining > 0 => input
                .get(..4)
                .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize),
            Self::FixedBytes { remaining, width } if *remaining > 0 => Some(*width as usize),
            Self::DeltaBytes {
                values, at, primed, ..
            } if *primed && *at < values.len() => Some(values[*at].len()),
            _ => None,
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Bool(d) => d.is_complete(),
            Self::BoolRle(d) => d.is_complete(),
            Self::I32(d) => d.is_complete(),
            Self::I64(d) => d.is_complete(),
            Self::F32(d) => d.is_complete(),
            Self::F64(d) => d.is_complete(),
            Self::Dictionary(d) => d.is_complete(),
            Self::Bytes { remaining, .. } | Self::FixedBytes { remaining, .. } => *remaining == 0,
            Self::DeltaI32 {
                values, at, primed, ..
            } => *primed && *at >= values.len(),
            Self::DeltaI64 {
                values, at, primed, ..
            } => *primed && *at >= values.len(),
            Self::DeltaBytes {
                values, at, primed, ..
            } => *primed && *at >= values.len(),
        }
    }

    fn feed_one<'a>(&mut self, input: &'a [u8]) -> Result<(usize, Option<Decoded<'a>>), Error> {
        match self {
            Self::Bytes {
                remaining,
                max_value_bytes,
            } => {
                if *remaining == 0 {
                    return Ok((0, None));
                }
                let prefix: [u8; 4] = input
                    .get(..4)
                    .ok_or(Error::Message("byte array length is truncated"))?
                    .try_into()
                    .unwrap();
                let len = u32::from_le_bytes(prefix);
                if len > *max_value_bytes {
                    return Err(Error::Message(
                        "byte array value exceeds the configured limit",
                    ));
                }
                let end = 4 + len as usize;
                let bytes = input
                    .get(4..end)
                    .ok_or(Error::Message("byte array value is truncated"))?;
                *remaining -= 1;
                Ok((end, Some(Decoded::Bytes(bytes))))
            }
            Self::FixedBytes { remaining, width } => {
                if *remaining == 0 {
                    return Ok((0, None));
                }
                let bytes = input
                    .get(..*width as usize)
                    .ok_or(Error::Message("fixed byte array value is truncated"))?;
                *remaining -= 1;
                Ok((*width as usize, Some(Decoded::Bytes(bytes))))
            }
            Self::Bool(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |b| v = Some(b))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::Boolean(x)))))
            }
            Self::BoolRle(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |b| v = Some(b))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::Boolean(x)))))
            }
            Self::I32(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |x| v = Some(x))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::I32(x)))))
            }
            Self::I64(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |x| v = Some(x))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::I64(x)))))
            }
            Self::F32(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |x| v = Some(x))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::F32(x)))))
            }
            Self::F64(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |x| v = Some(x))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::F64(x)))))
            }
            Self::DeltaI32 {
                expected,
                values,
                at,
                primed,
            } => take_delta_i32(input, *expected, values, at, primed),
            Self::DeltaI64 {
                expected,
                values,
                at,
                primed,
            } => take_delta_i64(input, *expected, values, at, primed),
            Self::DeltaBytes {
                expected,
                max_value_bytes,
                values,
                at,
                primed,
                prefixes,
            } => take_delta_bytes(
                input,
                *expected,
                *max_value_bytes,
                values,
                at,
                primed,
                *prefixes,
            ),
            Self::Dictionary(d) => {
                let mut v = None;
                let used = d
                    .feed_limited(input, 1, |id| v = Some(id))
                    .map_err(|e| Error::Page(e.into()))?;
                Ok((used, v.map(|x| Decoded::Value(Value::DictionaryId(x)))))
            }
        }
    }
}

fn take_delta_i32<'a>(
    input: &'a [u8],
    expected: u64,
    values: &mut Vec<i32>,
    at: &mut usize,
    primed: &mut bool,
) -> Result<(usize, Option<Decoded<'a>>), Error> {
    let used = prime_delta(input, primed, || decode_delta_i32(input, expected), values)?;
    Ok(take_queued(values, at, used, |v| {
        Decoded::Value(Value::I32(v))
    }))
}

fn take_delta_i64<'a>(
    input: &'a [u8],
    expected: u64,
    values: &mut Vec<i64>,
    at: &mut usize,
    primed: &mut bool,
) -> Result<(usize, Option<Decoded<'a>>), Error> {
    let used = prime_delta(input, primed, || decode_delta_i64(input, expected), values)?;
    Ok(take_queued(values, at, used, |v| {
        Decoded::Value(Value::I64(v))
    }))
}

fn take_delta_bytes<'a>(
    input: &'a [u8],
    expected: u64,
    max_value_bytes: u32,
    values: &mut Vec<Vec<u8>>,
    at: &mut usize,
    primed: &mut bool,
    prefixes: bool,
) -> Result<(usize, Option<Decoded<'a>>), Error> {
    let used = prime_delta(
        input,
        primed,
        || decode_delta_bytes(input, expected, max_value_bytes, prefixes),
        values,
    )?;
    if *at >= values.len() {
        return Ok((used, None));
    }
    let value = std::mem::take(&mut values[*at]);
    *at += 1;
    Ok((used, Some(Decoded::Owned(value))))
}

fn prime_delta<T>(
    input: &[u8],
    primed: &mut bool,
    decode: impl FnOnce() -> Result<(usize, Vec<T>), Error>,
    values: &mut Vec<T>,
) -> Result<usize, Error> {
    if *primed {
        return Ok(0);
    }
    if input.is_empty() {
        return Ok(0);
    }
    let (used, decoded) = decode()?;
    *values = decoded;
    *primed = true;
    Ok(used)
}

fn take_queued<'a, T: Copy>(
    values: &[T],
    at: &mut usize,
    used: usize,
    wrap: impl FnOnce(T) -> Decoded<'a>,
) -> (usize, Option<Decoded<'a>>) {
    if *at >= values.len() {
        return (used, None);
    }
    let value = values[*at];
    *at += 1;
    (used, Some(wrap(value)))
}

fn decode_delta_i32(input: &[u8], expected: u64) -> Result<(usize, Vec<i32>), Error> {
    decode_delta(input, expected, |dec, out| {
        dec.feed(input, |v| out.push(v))
            .map_err(|e| Error::Page(e.into()))
    })
}

fn decode_delta_i64(input: &[u8], expected: u64) -> Result<(usize, Vec<i64>), Error> {
    decode_delta(input, expected, |dec, out| {
        dec.feed(input, |v| out.push(v))
            .map_err(|e| Error::Page(e.into()))
    })
}

fn decode_delta<T: compact_parquet_thrift::values::DeltaInt>(
    input: &[u8],
    expected: u64,
    feed: impl Fn(&mut DeltaBinary<'_, T>, &mut Vec<T>) -> Result<usize, Error>,
) -> Result<(usize, Vec<T>), Error> {
    let mut widths = vec![0u8; 64];
    loop {
        let mut out = Vec::with_capacity(expected as usize);
        let decoded = {
            let mut dec = DeltaBinary::new(expected, &mut widths);
            let used = feed(&mut dec, &mut out);
            let finished = dec.finish().map_err(|e| Error::Page(e.into()));
            (used, finished, out)
        };
        match decoded {
            (Ok(used), Ok(()), out) => return Ok((used, out)),
            (Err(Error::Page(page)), _, _) if page_is_workspace(&page) => {
                // `feed` maps workspace errors into Page; retry below when the message carries a size.
                let _ = page;
                widths.resize(widths.len().saturating_mul(2).max(128), 0);
                if widths.len() > input.len().saturating_add(4096) {
                    return Err(Error::Message("delta width workspace exceeded the page"));
                }
            }
            (Err(e), _, _) => return Err(e),
            (Ok(_), Err(e), _) => return Err(e),
        }
    }
}

fn page_is_workspace(err: &compact_parquet_thrift::pages::Error) -> bool {
    let text = format!("{err:?}");
    text.contains("Workspace")
}

fn decode_delta_bytes(
    input: &[u8],
    expected: u64,
    max_value_bytes: u32,
    prefixes: bool,
) -> Result<(usize, Vec<Vec<u8>>), Error> {
    let count = usize::try_from(expected).map_err(|_| Error::Message("delta page is too large"))?;
    let mut widths = vec![0u8; 64];
    loop {
        let decoded = if prefixes {
            let mut parts = vec![
                DeltaByteParts {
                    prefix: 0,
                    suffix: 0
                };
                count
            ];
            let mut previous = vec![0u8; max_value_bytes as usize];
            let mut out = Vec::with_capacity(count);
            let mut current = Vec::new();
            let mut dec = DeltaByteArrays::new(
                count,
                &mut parts,
                &mut widths,
                &mut previous,
                max_value_bytes,
            )
            .map_err(|e| Error::Page(e.into()))?;
            let used = dec
                .feed(input, |event| {
                    push_byte_event(&mut out, &mut current, event)
                })
                .map_err(|e| Error::Page(e.into()));
            let finished = dec.finish().map_err(|e| Error::Page(e.into()));
            (used, finished, out)
        } else {
            let mut lengths = vec![0u32; count];
            let mut out = Vec::with_capacity(count);
            let mut current = Vec::new();
            let mut dec =
                DeltaLengthByteArrays::new(count, &mut lengths, &mut widths, max_value_bytes)
                    .map_err(|e| Error::Page(e.into()))?;
            let used = dec
                .feed(input, |event| {
                    push_byte_event(&mut out, &mut current, event)
                })
                .map_err(|e| Error::Page(e.into()));
            let finished = dec.finish().map_err(|e| Error::Page(e.into()));
            (used, finished, out)
        };
        match decoded {
            (Ok(used), Ok(()), out) => return Ok((used, out)),
            (Err(Error::Page(page)), _, _) if page_is_workspace(&page) => {
                widths.resize(widths.len().saturating_mul(2).max(128), 0);
                if widths.len() > input.len().saturating_add(4096) {
                    return Err(Error::Message("delta width workspace exceeded the page"));
                }
            }
            (Err(e), _, _) => return Err(e),
            (Ok(_), Err(e), _) => return Err(e),
        }
    }
}

fn push_byte_event(out: &mut Vec<Vec<u8>>, current: &mut Vec<u8>, event: ByteArrayEvent<'_>) {
    match event {
        ByteArrayEvent::Start(_) => current.clear(),
        ByteArrayEvent::Chunk(bytes) => current.extend_from_slice(bytes),
        ByteArrayEvent::End => out.push(std::mem::take(current)),
    }
}

/// Decompress one owned page body into the same layout [`PageAssembler`] uses:
/// V2 copies raw definition/repetition level prefix, then Snappy-expands only the values section.
/// Parquet `uncompressed_page_size` is the full logical page size (levels + values for V2); shorter
/// Snappy output is invalid unless the values section alone is shorter because levels are uncompressed
/// in the page body — never accept truncated output without matching `page_layout` section lengths.
pub fn inflate_page_body(
    header: PageHeader,
    spec: ColumnSpec,
    codec: i32,
    body: &[u8],
    scratch: &mut super::page_scratch::PageScratch,
) -> Result<Vec<u8>, Error> {
    let layout = page_layout(header, spec).map_err(Error::Page)?;
    let total = layout.uncompressed_bytes;
    scratch
        .begin_page_decode(codec, total)
        .map_err(Error::from)?;
    if total == 0 {
        return Ok(Vec::new());
    }
    if codec == 0 {
        if body.len() != total {
            return Err(Error::Page(PageError::LengthMismatch));
        }
        return Ok(body.to_vec());
    }
    if codec != 1 {
        return Err(Error::Message(
            "only uncompressed and snappy pages are supported",
        ));
    }
    let mut plain = vec![0u8; total];
    let levels = layout.v2_levels_bytes;
    if levels > 0 {
        if body.len() < levels {
            return Err(Error::Page(PageError::Truncated));
        }
        plain[..levels].copy_from_slice(&body[..levels]);
        let values_expected = total - levels;
        let tail = &body[levels..];
        if layout.values_compressed {
            let written = snappy_decompress_into(tail, &mut plain[levels..], values_expected)?;
            if written != values_expected {
                return Err(Error::Message(
                    "V2 data page: decompressed values length does not match header (check level prefix + values section)",
                ));
            }
        } else if tail.len() != values_expected {
            return Err(Error::Page(PageError::LengthMismatch));
        } else {
            plain[levels..].copy_from_slice(tail);
        }
        return Ok(plain);
    }
    if !layout.values_compressed {
        if body.len() != total {
            return Err(Error::Page(PageError::LengthMismatch));
        }
        plain.copy_from_slice(body);
        return Ok(plain);
    }
    let written = snappy_decompress_into(body, &mut plain, total)?;
    if written != total {
        return Err(Error::Message(
            "V1/dictionary page: decompressed length does not match uncompressed_page_size",
        ));
    }
    Ok(plain)
}

/// Decompress one whole Snappy block into `plain`; returns the decoded length.
///
/// Owned pages hold the entire compressed body, so this decodes the block directly (bulk
/// literal and back-reference copies) instead of streaming it a byte at a time. Every length
/// and offset is checked: page bytes are untrusted.
fn snappy_decompress_into(body: &[u8], plain: &mut [u8], expected: usize) -> Result<usize, Error> {
    if body.is_empty() {
        return if expected == 0 {
            Ok(0)
        } else {
            Err(Error::Page(PageError::Truncated))
        };
    }
    let corrupt = || Error::Message("snappy block is corrupt");
    let mut at = 0usize;
    // Preamble: uncompressed length as a little-endian varint.
    let mut len = 0usize;
    let mut shift = 0u32;
    loop {
        let byte = *body.get(at).ok_or_else(corrupt)?;
        at += 1;
        if shift > 28 {
            return Err(corrupt());
        }
        len |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    if len > expected || len > plain.len() {
        return Err(Error::Page(PageError::LengthMismatch));
    }
    let out = &mut plain[..len];
    let mut pos = 0usize;
    while at < body.len() {
        let tag = body[at];
        at += 1;
        match tag & 3 {
            0 => {
                let mut literal = usize::from(tag >> 2);
                if literal >= 60 {
                    let extra = literal - 59;
                    let bytes = body.get(at..at + extra).ok_or_else(corrupt)?;
                    at += extra;
                    literal = bytes
                        .iter()
                        .rev()
                        .fold(0usize, |acc, b| (acc << 8) | usize::from(*b));
                }
                let literal = literal + 1;
                let src = body.get(at..at + literal).ok_or_else(corrupt)?;
                out.get_mut(pos..pos + literal)
                    .ok_or_else(corrupt)?
                    .copy_from_slice(src);
                at += literal;
                pos += literal;
            }
            kind => {
                let (length, offset) = match kind {
                    1 => {
                        let low = *body.get(at).ok_or_else(corrupt)?;
                        at += 1;
                        (
                            usize::from((tag >> 2) & 7) + 4,
                            (usize::from(tag >> 5) << 8) | usize::from(low),
                        )
                    }
                    2 => {
                        let b = body.get(at..at + 2).ok_or_else(corrupt)?;
                        at += 2;
                        (
                            usize::from(tag >> 2) + 1,
                            usize::from(u16::from_le_bytes([b[0], b[1]])),
                        )
                    }
                    _ => {
                        let b = body.get(at..at + 4).ok_or_else(corrupt)?;
                        at += 4;
                        (
                            usize::from(tag >> 2) + 1,
                            u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize,
                        )
                    }
                };
                if offset == 0 || offset > pos || pos + length > out.len() {
                    return Err(corrupt());
                }
                let from = pos - offset;
                if offset >= length {
                    out.copy_within(from..from + length, pos);
                } else {
                    // Overlapping copy repeats the last `offset` bytes.
                    for i in 0..length {
                        out[pos + i] = out[from + i];
                    }
                }
                pos += length;
            }
        }
    }
    if pos != len {
        return Err(Error::Page(PageError::Truncated));
    }
    Ok(len)
}

#[cfg(test)]
mod snappy_tests {
    use super::*;

    fn decode(block: &[u8], expected: usize) -> Result<Vec<u8>, Error> {
        let mut plain = vec![0u8; expected];
        let n = snappy_decompress_into(block, &mut plain, expected)?;
        plain.truncate(n);
        Ok(plain)
    }

    #[test]
    fn literals_and_copies_including_overlap() {
        // "abcd" literal, then copy1 len 4 offset 4 ("abcd"), then copy2 len 6 offset 1 ("dddddd").
        let block = [
            14, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x04, 0x16, 0x01, 0x00,
        ];
        assert_eq!(decode(&block, 14).unwrap(), b"abcdabcddddddd");
    }

    #[test]
    fn long_literal_length_bytes() {
        let text: Vec<u8> = (0..100u8).collect();
        let mut block = vec![100, 60 << 2, 99];
        block.extend_from_slice(&text);
        assert_eq!(decode(&block, 100).unwrap(), text);
    }

    #[test]
    fn corrupt_blocks_are_errors_not_panics() {
        for block in [
            &[4u8, 0x05, 0x00][..],                // copy before any output
            &[4, 0x0c, b'a'],                      // literal past the end of input
            &[2, 0x0c, b'a', b'b', b'c', b'd'],    // literal past the declared length
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff], // runaway varint
            &[3, 0x04, b'a', b'b'],                // short output
        ] {
            assert!(decode(block, 8).is_err(), "{block:?}");
        }
    }
}
