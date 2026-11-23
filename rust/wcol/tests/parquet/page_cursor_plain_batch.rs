//! Direct [`PageCursor`] batch contract: 9 PLAIN i64 values, output capacity 3 per step.

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use compact_parquet_thrift::pages::{ColumnSpec, PageEvent, Physical, Value};
use compact_parquet_thrift::parquet::page_header;
use wcol::formats::parquet::{
    inflate_page_body, open_plan, Error, Flow, PageCursor, PageScratch, PageStep, ScanLimits,
};

#[test]
fn page_cursor_plain_nine_values_three_per_step() {
    let values: Vec<i64> = (0..9).collect();
    let column = Column {
        name: "v",
        rows: 9,
        codec: SNAPPY,
        present: None,
        values: ColumnValues::I64(&values),
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch_enc = vec![0u8; scratch_len(9, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 256 * 1024];
    let n = write_file(
        &mut file_bytes,
        9,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch_enc,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-cursor-step-{}-{}.parquet",
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
    let spec = ColumnSpec::flat(Physical::Int64, SNAPPY);
    let limits = ScanLimits::for_batch_rows(3);
    let mut scratch = PageScratch::for_limits(&limits);
    let plain = inflate_page_body(header, spec, chunk.codec, page_body, &mut scratch).unwrap();
    let mut cursor = PageCursor::new(header, spec, &limits).unwrap();
    let mut out = Vec::new();
    for step_num in 0..4 {
        let step = cursor
            .step(&plain, 3, |event| match event {
                PageEvent::Value(_, Value::I64(v)) => {
                    out.push(v);
                    Ok(Flow::Continue)
                }
                PageEvent::Null(_) => Err(Error::Message("unexpected null")),
                _ => Err(Error::Message("unexpected event")),
            })
            .unwrap();
        match (step_num, step) {
            (0, PageStep::Emitted(3)) | (1, PageStep::Emitted(3)) | (2, PageStep::Emitted(3)) => {}
            (3, PageStep::PageComplete) => {}
            _ => panic!("step {step_num}: got {step:?}, out={out:?}"),
        }
    }
    assert_eq!(out, values);
    assert!(cursor.page_complete());
    let _ = std::fs::remove_file(path);
}
