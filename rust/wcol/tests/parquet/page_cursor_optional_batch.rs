//! Nullable [`PageCursor`] batch cap counts logical rows (null + value), not physical scalars only.

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use compact_parquet_thrift::pages::{ColumnSpec, PageEvent, Physical, Value};
use compact_parquet_thrift::parquet::page_header;
use wcol::formats::parquet::{
    inflate_page_body, open_plan, Flow, PageCursor, PageScratch, PageStep, ScanLimits,
};

#[test]
fn page_cursor_optional_four_rows_batch_three() {
    // row0 null; rows 1..=3 present with values 10,20,30
    let present = [0b0000_1110u8];
    let values = [10i64, 20, 30];
    let column = Column {
        name: "v",
        rows: 4,
        codec: SNAPPY,
        present: Some(&present),
        values: ColumnValues::I64(&values),
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch_enc = vec![0u8; scratch_len(4, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 256 * 1024];
    let n = write_file(
        &mut file_bytes,
        4,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch_enc,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-cursor-opt-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &file_bytes).unwrap();
    let (_f, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let body =
        &file_bytes[chunk.start as usize..chunk.start as usize + chunk.compressed_size as usize];
    let (header, header_len) = page_header(body).unwrap();
    let page_body = &body[header_len..];
    let spec = ColumnSpec::flat(Physical::Int64, SNAPPY).optional();
    let limits = ScanLimits::for_batch_rows(3);
    let mut scratch = PageScratch::for_limits(&limits);
    let plain = inflate_page_body(header, spec, chunk.codec, page_body, &mut scratch).unwrap();
    let mut cursor = PageCursor::new(header, spec, &limits).unwrap();
    let mut events: Vec<(bool, Option<i64>)> = Vec::new();
    let step0 = cursor
        .step(&plain, 3, |event| {
            match event {
                PageEvent::Null(_) => events.push((false, None)),
                PageEvent::Value(_, Value::I64(v)) => events.push((true, Some(v))),
                _ => panic!("unexpected {event:?}"),
            }
            Ok(Flow::Continue)
        })
        .unwrap();
    assert_eq!(step0, PageStep::Emitted(3));
    assert_eq!(
        events,
        vec![(false, None), (true, Some(10)), (true, Some(20))]
    );
    let step1 = cursor
        .step(&plain, 3, |event| {
            match event {
                PageEvent::Value(_, Value::I64(v)) => events.push((true, Some(v))),
                PageEvent::Null(_) => events.push((false, None)),
                _ => panic!("unexpected {event:?}"),
            }
            Ok(Flow::Continue)
        })
        .unwrap();
    assert_eq!(step1, PageStep::Emitted(1));
    assert_eq!(events.len(), 4);
    assert_eq!(events[3], (true, Some(30)));
    assert!(cursor.page_complete());
    let _ = std::fs::remove_file(path);
}
