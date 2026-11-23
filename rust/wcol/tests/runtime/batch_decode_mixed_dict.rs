//! Per-row-group dictionary flag (review P0): plain groups must not use dict-id decode.

#[cfg(not(target_arch = "wasm32"))]
mod mixed {
    use std::collections::HashSet;
    use std::io::{Read, Seek, SeekFrom};

    use compact_parquet_thrift::encode::{
        scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
    };
    use wcol::formats::parquet::{open_plan, ParquetDecode};
    use wcol::ops::query::CatalogColumn;
    use wcol::plan::physical::IngestGroup;
    use wcol::plan::sql_parser::ColumnKind;
    use wcol::stream::OwnedWcolBatch;

    fn catalog_v() -> Vec<CatalogColumn> {
        vec![CatalogColumn {
            name: "v".into(),
            kind: ColumnKind::Number,
            physical: 1,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        }]
    }

    fn write_dict_parquet(rows: usize) -> Vec<u8> {
        let dictionary = [10i32, 20, 30];
        let ids: Vec<u32> = (0..rows).map(|i| (i % 3) as u32).collect();
        let column = Column {
            name: "v",
            rows,
            codec: SNAPPY,
            present: None,
            values: ColumnValues::DictI32 {
                dictionary: &dictionary,
                ids: &ids,
            },
        };
        write_one_column_parquet(rows, column)
    }

    fn write_plain_parquet(rows: usize) -> Vec<u8> {
        let values: Vec<i32> = (0..rows).map(|i| i as i32).collect();
        let column = Column {
            name: "v",
            rows,
            codec: SNAPPY,
            present: None,
            values: ColumnValues::I32(&values),
        };
        write_one_column_parquet(rows, column)
    }

    fn write_one_column_parquet(rows: usize, column: Column) -> Vec<u8> {
        let mut places = [ChunkPlace::default()];
        let mut scratch = vec![0u8; scratch_len(rows, 64).unwrap()];
        let mut table = vec![0u32; 256];
        let mut file_bytes = vec![0u8; 256 * 1024];
        let n = write_file(
            &mut file_bytes,
            rows as i64,
            std::slice::from_ref(&column),
            &mut places,
            &mut scratch,
            &mut table,
        )
        .unwrap();
        file_bytes.truncate(n);
        file_bytes
    }

    fn drive_decode(decode: &mut ParquetDecode, file: &mut std::fs::File) -> Result<usize, String> {
        let mut batch = OwnedWcolBatch::empty();
        let mut batches = 0usize;
        loop {
            if decode.fill_batch(&mut batch).map_err(|e| e.to_string())? {
                batches += 1;
                continue;
            }
            if decode.ingest_complete() {
                break;
            }
            let Some(read) = decode.host_read_request().map_err(|e| e.to_string())? else {
                return Err("decode stalled".into());
            };
            let mut bytes = vec![0u8; read.length as usize];
            file.seek(SeekFrom::Start(read.offset))
                .map_err(|e| e.to_string())?;
            file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
            decode
                .complete_read(read.ticket, &bytes)
                .map_err(|e| e.to_string())?;
        }
        Ok(batches)
    }

    fn ingest_one_group(path: &std::path::Path, dict_encoded: bool) -> Result<usize, String> {
        let (mut file, plan) = open_plan(path).map_err(|e| e.to_string())?;
        let chunk = &plan.chunks[0];
        let ingest = [IngestGroup {
            row_group: 0,
            rows: plan.rows as u64,
            chunks: vec![wcol::IngestGroupChunk {
                column: 0,
                offset: chunk.start,
                codec: chunk.codec,
                length: chunk.compressed_size as u32,
                dict_encoded,
            }],
        }];
        let mut decode = ParquetDecode::from_ingest_groups(8, catalog_v(), &ingest);
        drive_decode(&mut decode, &mut file)
    }

    #[test]
    fn per_row_group_dict_flag_not_global() {
        let dir = std::env::temp_dir();
        let dict_path = dir.join(format!("wcol-dict-rg-{}", std::process::id()));
        let plain_path = dir.join(format!("wcol-plain-rg-{}", std::process::id()));
        std::fs::write(&dict_path, write_dict_parquet(4)).unwrap();
        std::fs::write(&plain_path, write_plain_parquet(4)).unwrap();

        assert!(
            ingest_one_group(&dict_path, true).unwrap() >= 1,
            "dict-encoded row group"
        );
        let plain_batches = ingest_one_group(&plain_path, false).unwrap();
        assert!(plain_batches >= 1, "plain row group");

        let (mut file, plan) = open_plan(&plain_path).unwrap();
        let chunk = &plan.chunks[0];
        let mut global_decode = ParquetDecode::new(
            8,
            catalog_v(),
            vec![(
                0,
                plan.rows as u64,
                vec![(0, chunk.start, chunk.codec, chunk.compressed_size as u32)],
            )],
            HashSet::from([0]),
        );
        // A dictionary flag on a PLAIN chunk falls back to PLAIN decoding page by page.
        let global_batches = drive_decode(&mut global_decode, &mut file).unwrap();
        assert_eq!(global_batches, plain_batches);

        let _ = std::fs::remove_file(dict_path);
        let _ = std::fs::remove_file(plain_path);
    }
}
