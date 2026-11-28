//! [`NumericScan`] on hand-built column chunks: page sequences valid files contain but writers
//! rarely emit, and malformed ones that must fail instead of trapping, spinning or dropping rows.

use compact_parquet_thrift::encode::{encode_delta_i64, encode_hybrid};
use compact_parquet_thrift::{SliceSink, Writer};
use wcol::formats::parquet::decode::{DecodeProgress, ScanLimits};
use wcol::formats::parquet::schema::Leaf;
use wcol::formats::parquet::{NumericScan, ScanValueMode};
use wcol::stream::DictEntries;

const PLAIN: i32 = 0;
const DELTA_BINARY_PACKED: i32 = 5;
const RLE_DICTIONARY: i32 = 8;

fn page(
    header: impl FnOnce(&mut Writer<SliceSink<'_>>) -> compact_parquet_thrift::Result<()>,
) -> Vec<u8> {
    let mut buf = [0u8; 128];
    let mut writer = Writer::new(SliceSink::new(&mut buf));
    writer.structure(header).unwrap();
    writer.into_inner().as_written().to_vec()
}

/// A V1 data page: `values` rows (NULLs included), RLE definition levels.
fn data_page(values: i32, encoding: i32, body: &[u8]) -> Vec<u8> {
    let len = body.len() as i32;
    let mut out = page(|w| {
        w.field_i32(1, 0)?;
        w.field_i32(2, len)?;
        w.field_i32(3, len)?;
        w.field_struct(5, |w| {
            w.field_i32(1, values)?;
            w.field_i32(2, encoding)?;
            w.field_i32(3, 3)?;
            w.field_i32(4, 3)
        })
    });
    out.extend_from_slice(body);
    out
}

fn dictionary_page(entries: i32, body: &[u8]) -> Vec<u8> {
    let len = body.len() as i32;
    let mut out = page(|w| {
        w.field_i32(1, 2)?;
        w.field_i32(2, len)?;
        w.field_i32(3, len)?;
        w.field_struct(7, |w| {
            w.field_i32(1, entries)?;
            w.field_i32(2, PLAIN)
        })
    });
    out.extend_from_slice(body);
    out
}

fn plain_i64(values: &[i64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// RLE_DICTIONARY page body: bit width, then the hybrid-encoded ids.
fn dictionary_ids(bit_width: u8, ids: &[u32]) -> Vec<u8> {
    let mut out = vec![0u8; 64];
    let n = encode_hybrid(bit_width, ids, &mut out[1..]).unwrap();
    out[0] = bit_width;
    out.truncate(n + 1);
    out
}

/// V1 definition levels: a four-byte length, then the hybrid-encoded levels.
fn definition_levels(levels: &[u32]) -> Vec<u8> {
    let mut runs = vec![0u8; 64];
    let n = encode_hybrid(1, levels, &mut runs).unwrap();
    let mut out = (n as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&runs[..n]);
    out
}

fn leaf(physical: i32, max_def: u8) -> Leaf {
    Leaf {
        name: "v".into(),
        physical,
        type_length: 0,
        max_def,
        max_rep: 0,
    }
}

fn scan(chunk: &[u8], leaf: Leaf, rows: u64, mode: ScanValueMode) -> NumericScan {
    let limits = ScanLimits::for_batch_rows(8);
    let len = chunk.len() as u32;
    let mut scan = match mode {
        ScanValueMode::DictionaryIds => {
            NumericScan::begin_dict_ids_with_limits(leaf, 0, limits, len, Some(rows))
        }
        _ => NumericScan::begin_with_limits(leaf, 0, limits, len, Some(rows)),
    }
    .unwrap();
    assert_eq!(scan.push_compressed(chunk).unwrap(), chunk.len());
    scan
}

/// Every row as `(value or dictionary id, present)`, or the first decode error.
fn drain(scan: &mut NumericScan, cap: usize) -> Result<Vec<(i64, u8)>, String> {
    let mut rows = Vec::new();
    for _ in 0..1000 {
        match scan.poll(cap).map_err(|e| e.to_string())? {
            DecodeProgress::BatchReady { rows: n } => {
                let mut nulls = Vec::new();
                let values: Vec<i64> = match scan.value_mode() {
                    ScanValueMode::DictionaryIds => {
                        let mut ids = Vec::new();
                        scan.copy_dict_id_rows(&mut ids, &mut nulls, n).unwrap();
                        ids.into_iter().map(i64::from).collect()
                    }
                    _ => {
                        let mut values = Vec::new();
                        scan.copy_materialized_rows(&mut values, &mut nulls, n)
                            .unwrap();
                        values
                    }
                };
                rows.extend(values.into_iter().zip(nulls));
                scan.advance_batch(n).unwrap();
            }
            DecodeProgress::Finished => return Ok(rows),
            DecodeProgress::NeedInput => return Err("NeedInput with the whole chunk fed".into()),
        }
    }
    Err("scan did not finish".into())
}

fn dictionary_chunk(dictionaries: &[&[i64]]) -> Vec<u8> {
    let mut chunk = Vec::new();
    for entries in dictionaries {
        chunk.extend(dictionary_page(entries.len() as i32, &plain_i64(entries)));
        chunk.extend(data_page(2, RLE_DICTIONARY, &dictionary_ids(1, &[0, 1])));
    }
    chunk
}

#[test]
fn a_dictionary_page_after_a_data_page_is_rejected() {
    // The second dictionary would replace entries the first page's ids (and the batch's
    // shared dictionary) already refer to.
    let chunk = dictionary_chunk(&[&[10, 20], &[30, 40]]);
    for mode in [ScanValueMode::DictionaryIds, ScanValueMode::MaterializedI64] {
        let err = drain(&mut scan(&chunk, leaf(2, 0), 4, mode), 8).unwrap_err();
        assert!(
            err.contains("dictionary page after its first page"),
            "{mode:?}: {err}"
        );
    }
}

#[test]
fn two_dictionary_pages_in_a_row_are_rejected() {
    let mut chunk = dictionary_page(1, &plain_i64(&[10]));
    chunk.extend(dictionary_chunk(&[&[30, 40]]));
    let err = drain(
        &mut scan(&chunk, leaf(2, 0), 2, ScanValueMode::DictionaryIds),
        8,
    )
    .unwrap_err();
    assert!(
        err.contains("dictionary page after its first page"),
        "{err}"
    );
}

#[test]
fn rows_beyond_the_row_group_count_are_an_error() {
    let five = data_page(5, PLAIN, &plain_i64(&[1, 2, 3, 4, 5]));
    let mut two_pages = data_page(4, PLAIN, &plain_i64(&[1, 2, 3, 4]));
    two_pages.extend(data_page(1, PLAIN, &plain_i64(&[5])));
    for chunk in [&five, &two_pages] {
        for cap in [2, 4, 8] {
            let err = drain(
                &mut scan(chunk, leaf(2, 0), 4, ScanValueMode::MaterializedI64),
                cap,
            )
            .unwrap_err();
            assert!(err.contains("more rows than expected"), "cap {cap}: {err}");
        }
    }
}

#[test]
fn a_page_shorter_than_its_value_count_is_an_error() {
    // Four values declared, three present.
    let chunk = data_page(4, PLAIN, &plain_i64(&[1, 2, 3]));
    let err = drain(
        &mut scan(&chunk, leaf(2, 0), 4, ScanValueMode::MaterializedI64),
        8,
    )
    .unwrap_err();
    assert!(err.contains("page ended before its values"), "{err}");

    // `next_batch_into` used to spin here: Blocked with the whole chunk fed read as NeedInput.
    let mut scan = scan(&chunk, leaf(2, 0), 4, ScanValueMode::MaterializedI64);
    let (mut values, mut nulls) = (Vec::with_capacity(8), Vec::with_capacity(8));
    let mut result = Ok(Some(0));
    for _ in 0..4 {
        result = scan.next_batch_into(&mut values, &mut nulls, 8);
        if result.is_err() {
            break;
        }
    }
    assert!(result.is_err(), "{result:?}");
}

#[test]
fn a_chunk_that_ends_inside_a_page_is_an_error() {
    let whole = data_page(2, PLAIN, &plain_i64(&[1, 2]));
    let mut half_header = whole.clone();
    half_header.extend_from_slice(&whole[..3]);
    let mut half_body = whole.clone();
    half_body.extend_from_slice(&whole[..whole.len() - 4]);
    for chunk in [half_header, half_body] {
        let err = drain(
            &mut scan(&chunk, leaf(2, 0), 4, ScanValueMode::MaterializedI64),
            8,
        )
        .unwrap_err();
        assert!(err.contains("ends inside a page"), "{err}");
    }
}

#[test]
fn poll_rejects_a_zero_row_batch() {
    let chunk = data_page(2, PLAIN, &plain_i64(&[1, 2]));
    let mut scan = scan(&chunk, leaf(2, 0), 2, ScanValueMode::MaterializedI64);
    assert!(scan.poll(0).is_err());
    assert_eq!(drain(&mut scan, 8).unwrap(), vec![(1, 1), (2, 1)]);
}

#[test]
fn an_all_null_dictionary_chunk_with_an_empty_dictionary_decodes() {
    // What writers emit for a dictionary-encoded column whose row group is all NULL.
    let mut body = definition_levels(&[0, 0, 0]);
    body.push(0); // bit width of the (absent) ids
    let mut chunk = dictionary_page(0, &[]);
    chunk.extend(data_page(3, RLE_DICTIONARY, &body));
    let mut scan = scan(&chunk, leaf(6, 1), 3, ScanValueMode::DictionaryIds);
    assert_eq!(drain(&mut scan, 8).unwrap(), vec![(0, 0); 3]);
    let DictEntries::Bytes { offsets, bytes } = scan.dictionary_entries().unwrap() else {
        panic!("a BYTE_ARRAY column's dictionary must be bytes");
    };
    assert_eq!((&offsets[..], bytes.len()), (&[0u32][..], 0));
}

#[test]
fn delta_pages_after_a_dictionary_fallback_decode() {
    let mut delta = vec![0u8; 256];
    let n = encode_delta_i64(&[100, 101, 102], &mut delta).unwrap();
    let mut chunk = dictionary_page(2, &plain_i64(&[7, 9]));
    chunk.extend(data_page(2, RLE_DICTIONARY, &dictionary_ids(1, &[1, 0])));
    chunk.extend(data_page(3, DELTA_BINARY_PACKED, &delta[..n]));
    let want: Vec<(i64, u8)> = [9, 7, 100, 101, 102].map(|v| (v, 1)).to_vec();
    for mode in [ScanValueMode::MaterializedI64, ScanValueMode::DictionaryIds] {
        let mut scan = scan(&chunk, leaf(2, 0), 5, mode);
        assert_eq!(drain(&mut scan, 8).unwrap(), want, "{mode:?}");
        assert_eq!(scan.value_mode(), ScanValueMode::MaterializedI64);
    }
}
