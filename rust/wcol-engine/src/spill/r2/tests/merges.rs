use super::*;

#[test]
fn page_out_of_key_order_is_rejected() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(5)));
    let mut store = HostR2SortedRunStore::new(bridge);
    store.begin().unwrap();
    store.expect_run_rows(2);
    store.append_partials(&[(b"b".to_vec(), vec![1])]).unwrap();
    assert!(store.append_partials(&[(b"a".to_vec(), vec![1])]).is_err());
}

#[test]
fn merge_reads_share_the_budget_between_runs() {
    let budget = MERGE_READ_BUDGET;
    assert_eq!(run_read_chunk(1, budget), MAX_RUN_READ_CHUNK);
    assert_eq!(run_read_chunk(2, budget), 4 * 1024 * 1024);
    assert_eq!(run_read_chunk(16, budget), 512 * 1024);
    assert_eq!(run_read_chunk(300, budget), RUN_READ_CHUNK);
    assert_eq!(run_read_chunk(0, budget), MAX_RUN_READ_CHUNK);
}

#[test]
fn two_runs_merge_in_one_read_each() {
    let rows_a: Vec<_> = (0..2000u32)
        .map(|i| ((i * 2).to_be_bytes().to_vec(), vec![1u8; 200]))
        .collect();
    let rows_b: Vec<_> = (0..2000u32)
        .map(|i| ((i * 2 + 1).to_be_bytes().to_vec(), vec![2u8; 200]))
        .collect();
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(8)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let mut bodies = std::collections::HashMap::new();
    for rows in [&rows_a, &rows_b] {
        store.expect_run_rows(rows.len());
        store.append_partials(rows).unwrap();
        let (key, body) = commit_captured_run(&mut store, &bridge);
        assert!(body.len() > RUN_READ_CHUNK as usize);
        bodies.insert(key, body);
    }
    let reads = std::cell::Cell::new(0u32);
    let counted: std::collections::HashMap<_, _> = bodies.clone();
    let (rows, _) = merge_r2_counting(&mut store, &bridge, &counted, &reads);
    assert_eq!(rows.len(), 4000);
    assert_eq!(reads.get(), 2, "each run should come back in a single read");
}

#[test]
fn run_put_open_round_trip() {
    let plan = RunPutPlan {
        object_key: "wcol-spill/j/a1/r1/run-000001.bin".into(),
        part: 3,
        last: true,
        byte_length: 4096,
        record_count: 128,
    };
    let wire = encode_run_put_open(&plan).unwrap();
    let decoded = decode_run_put_open(&wire).unwrap();
    assert_eq!(decoded.object_key, plan.object_key);
    assert_eq!(decoded.byte_length, 4096);
    assert_eq!(decoded.record_count, 128);
}

#[test]
fn run_read_open_round_trip() {
    let plan = RunReadPlan {
        object_key: "wcol-spill/x/run-000001.bin".into(),
        byte_length: 8192,
        offset: 128,
        length: 4096,
    };
    let wire = encode_run_read_open(&plan).unwrap();
    let decoded = decode_run_read_open(&wire).unwrap();
    assert_eq!(decoded.object_key, plan.object_key);
    assert_eq!(decoded.offset, 128);
    assert_eq!(decoded.length, 4096);
}

#[test]
fn run_open_decoders_reject_truncated_keys_and_trailers() {
    let put = RunPutPlan {
        object_key: "run-a".into(),
        part: 1,
        last: false,
        byte_length: 100,
        record_count: 2,
    };
    let read = RunReadPlan {
        object_key: "run-a".into(),
        byte_length: 100,
        offset: 0,
        length: 64,
    };
    let put_wire = encode_run_put_open(&put).unwrap();
    let read_wire = encode_run_read_open(&read).unwrap();

    // Both decoders must return Err rather than panicking on malformed input.
    assert!(decode_run_put_open(&u32::MAX.to_le_bytes()).is_err());
    assert!(decode_run_read_open(&u32::MAX.to_le_bytes()).is_err());
    assert!(decode_run_put_open(&put_wire[..put_wire.len() - 1]).is_err());
    assert!(decode_run_read_open(&read_wire[..read_wire.len() - 1]).is_err());
    assert!(decode_run_put_open(&put_wire[..4 + put.object_key.len()]).is_err());
    assert!(decode_run_read_open(&read_wire[..4 + read.object_key.len()]).is_err());
}

#[test]
fn merge_preserves_exact_sorted_rows_across_mid_batch_r2_reads() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(1)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();

    let rows_a: Vec<_> = (0..180)
        .map(|i| (format!("k{:04}", i * 2).into_bytes(), vec![0xa1; 2048]))
        .collect();
    let rows_b: Vec<_> = (0..180)
        .map(|i| (format!("k{:04}", i * 2 + 1).into_bytes(), vec![0xb2; 2048]))
        .collect();
    let mut body_a = Vec::new();
    let mut body_b = Vec::new();
    encode_run(&mut body_a, &rows_a).unwrap();
    encode_run(&mut body_b, &rows_b).unwrap();
    assert!(body_a.len() > RUN_READ_CHUNK as usize);
    assert!(body_b.len() > RUN_READ_CHUNK as usize);

    for (idx, (key, body)) in [("run-a", &body_a), ("run-b", &body_b)]
        .into_iter()
        .enumerate()
    {
        store.runs.push(CommittedRunMeta {
            object_key: key.into(),
            byte_length: body.len() as u64,
            record_count: 180,
            seq_base: 1 + (idx as i64 * 180),
        });
    }
    store.global_seq = 360;
    // Keep reads at the 256 KiB floor so each run takes several, mid-batch.
    store.merge_read_budget = 2 * u64::from(RUN_READ_CHUNK);

    let complete_read = || {
        let req = bridge.borrow().pending_request().expect("pending R2 read");
        assert_eq!(req.op, SPILL_OP_RUN_READ);
        let plan = decode_run_read_open(&req.append_bytes).unwrap();
        let body = match plan.object_key.as_str() {
            "run-a" => &body_a,
            "run-b" => &body_b,
            other => panic!("unknown run {other}"),
        };
        let offset = plan.offset as usize;
        let end = (offset + plan.length as usize).min(body.len());
        assert!(offset < end, "expected a nonempty bounded run read");
        bridge
            .borrow_mut()
            .fill_run_read(&body[offset..end])
            .unwrap();
    };

    for _ in 0..16 {
        match store.begin_ordered_scan() {
            Ok(SpillProgress::Ready(())) => break,
            Ok(SpillProgress::AwaitHost) => complete_read(),
            Err(e) => panic!("scan bootstrap failed: {e}"),
        }
    }
    assert_eq!(store.bootstrap_run(), 2, "failed to bootstrap both runs");

    let mut resume = SpillScanResume::default();
    let mut output = Vec::new();
    let mut staged_mid_batch = false;
    let mut exhausted = false;
    for _ in 0..4096 {
        match store.next_ordered_batch(&mut resume, 360, 2 * 1024 * 1024) {
            Ok(SpillProgress::Ready(batch)) => {
                output.extend(
                    batch
                        .rows
                        .into_iter()
                        .map(|(_, key, payload)| (key, payload)),
                );
                if batch.exhausted {
                    exhausted = true;
                    break;
                }
            }
            Ok(SpillProgress::AwaitHost) => {
                staged_mid_batch |= !store.pending_emit.is_empty();
                complete_read();
            }
            Err(e) => panic!("merge failed: {e}"),
        }
    }
    assert!(exhausted, "merge did not complete");
    assert!(
        staged_mid_batch,
        "test must suspend after accumulating rows"
    );

    let mut expected: Vec<_> = rows_a.into_iter().chain(rows_b).collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        output, expected,
        "run merge must be sorted without lost or replayed rows"
    );
}

#[test]
fn ingest_after_host_complete_applies_run_read_buf() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(1)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let rows = vec![(b"k1".to_vec(), b"v1".to_vec())];
    let mut body = Vec::new();
    encode_run(&mut body, &rows).unwrap();
    store.runs.push(CommittedRunMeta {
        object_key: "run-a".into(),
        byte_length: body.len() as u64,
        record_count: 1,
        seq_base: 1,
    });
    store.global_seq = 1;
    store.merge = MergePhase::Bootstrapping {
        next_run: 0,
        read_target: None,
    };
    store.merge_readers = vec![RunReader {
        decoder: RunStreamDecoder::new(),
        file_offset: 0,
        emitted: 0,
        head: None,
        at_eof: false,
    }];
    store.merge = MergePhase::Bootstrapping {
        next_run: 0,
        read_target: Some((0, 0)),
    };
    let plan = RunReadPlan {
        object_key: "run-a".into(),
        byte_length: body.len() as u64,
        offset: 0,
        length: body.len() as u32,
    };
    assert!(matches!(
        await_host(
            &mut bridge.borrow_mut(),
            HostSpillRequest {
                op: SPILL_OP_RUN_READ,
                append_bytes: encode_run_read_open(&plan).unwrap(),
            },
        ),
        Ok(SpillProgress::AwaitHost)
    ));
    bridge.borrow_mut().fill_run_read(&body).unwrap();
    store.ingest_host_read_completion().unwrap();
    assert!(store.read_target().is_none());
    assert!(store.merge_readers[0].at_eof);
}

#[test]
fn single_host_read_buffers_all_run_records() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(1)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let rows: Vec<_> = (0..50)
        .map(|i| (format!("k{i:03}").into_bytes(), vec![i as u8]))
        .collect();
    let mut body = Vec::new();
    encode_run(&mut body, &rows).unwrap();
    store.runs.push(CommittedRunMeta {
        object_key: "run-a".into(),
        byte_length: body.len() as u64,
        record_count: rows.len() as u64,
        seq_base: 1,
    });
    store.global_seq = rows.len() as i64;
    store.merge = MergePhase::Bootstrapping {
        next_run: 0,
        read_target: None,
    };
    store.merge_readers = vec![RunReader {
        decoder: RunStreamDecoder::new(),
        file_offset: 0,
        emitted: 0,
        head: None,
        at_eof: false,
    }];
    store.merge = MergePhase::Bootstrapping {
        next_run: 0,
        read_target: Some((0, 0)),
    };
    bridge.borrow_mut().fill_run_read(&body).unwrap();
    store.ingest_host_read_completion().unwrap();
    assert!(store.merge_readers[0].at_eof);
    let mut resume = SpillScanResume::default();
    store.begin_ordered_scan().unwrap();
    let mut seen = 0usize;
    loop {
        match store.next_ordered_batch(&mut resume, 100, 1024 * 1024) {
            Ok(SpillProgress::Ready(batch)) => {
                seen += batch.rows.len();
                if batch.exhausted {
                    break;
                }
            }
            Ok(SpillProgress::AwaitHost) => panic!("single read should buffer the run"),
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(seen, rows.len());
}

#[test]
fn merge_reads_two_runs_via_host_chunks() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(1)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let rows_a = vec![(b"k1".to_vec(), b"a".to_vec())];
    let rows_b = vec![(b"k2".to_vec(), b"b".to_vec())];
    let mut body_a = Vec::new();
    encode_run(&mut body_a, &rows_a).unwrap();
    let mut body_b = Vec::new();
    encode_run(&mut body_b, &rows_b).unwrap();
    store.runs.push(CommittedRunMeta {
        object_key: "run-a".into(),
        byte_length: body_a.len() as u64,
        record_count: 1,
        seq_base: 1,
    });
    store.runs.push(CommittedRunMeta {
        object_key: "run-b".into(),
        byte_length: body_b.len() as u64,
        record_count: 1,
        seq_base: 2,
    });
    store.global_seq = 2;

    let mut bodies = [body_a, body_b];
    let mut resume = SpillScanResume::default();
    loop {
        match store.begin_ordered_scan() {
            Ok(SpillProgress::Ready(())) => break,
            Ok(SpillProgress::AwaitHost) => {
                let req = bridge.borrow().pending_request().expect("pending");
                assert_eq!(req.op, SPILL_OP_RUN_READ);
                let plan = decode_run_read_open(&req.append_bytes).unwrap();
                let chunk = if plan.object_key == "run-a" {
                    bodies[0].clone()
                } else {
                    bodies[1].clone()
                };
                bridge
                    .borrow_mut()
                    .fill_run_read(&chunk)
                    .expect("read complete");
            }
            Err(e) => panic!("{e}"),
        }
    }
    let mut total = 0usize;
    loop {
        match store.next_ordered_batch(&mut resume, 10, 1024 * 1024) {
            Ok(SpillProgress::Ready(batch)) => {
                total += batch.rows.len();
                if batch.exhausted {
                    break;
                }
            }
            Ok(SpillProgress::AwaitHost) => {
                let req = bridge.borrow().pending_request().expect("pending");
                let plan = decode_run_read_open(&req.append_bytes).unwrap();
                let chunk = if plan.object_key == "run-a" {
                    std::mem::take(&mut bodies[0])
                } else {
                    std::mem::take(&mut bodies[1])
                };
                bridge.borrow_mut().fill_run_read(&chunk).unwrap();
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(total, 2);
}

#[test]
fn memory_and_r2_merge_the_same_key_seq_order() {
    let runs = [
        vec![(b"b".to_vec(), vec![1]), (b"d".to_vec(), vec![1])],
        vec![(b"a".to_vec(), vec![2]), (b"d".to_vec(), vec![2])],
        vec![(b"a".to_vec(), vec![3]), (b"c".to_vec(), vec![3])],
    ];
    let mut memory = crate::spill::MemorySortedRunStore::new();
    memory.begin().unwrap();
    for run in &runs {
        memory.append_partials(run).unwrap();
        memory.finalize_run().unwrap();
    }
    memory.begin_ordered_scan().unwrap();
    let mut resume = SpillScanResume::default();
    let mut memory_rows = Vec::new();
    loop {
        let batch = memory
            .next_ordered_batch(&mut resume, 16, 1024 * 1024)
            .unwrap()
            .expect_ready("memory merge");
        memory_rows.extend(batch.rows.into_iter().map(|row| (row.1, row.0)));
        if batch.exhausted {
            break;
        }
    }

    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(3)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let mut bodies = std::collections::HashMap::new();
    for run in &runs {
        store.expect_run_rows(run.len());
        store.append_partials(run).unwrap();
        let (key, body) = commit_captured_run(&mut store, &bridge);
        bodies.insert(key, body);
    }
    let (r2_rows, _peak) = merge_r2(&mut store, &bridge, &bodies);
    assert_eq!(memory_rows, r2_rows);
    let keys: Vec<_> = memory_rows.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(
        keys,
        vec![
            b"a".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"d".to_vec(),
        ]
    );
    let seqs: Vec<_> = memory_rows.iter().map(|(_, seq)| *seq).collect();
    assert_eq!(seqs, vec![3, 5, 1, 6, 2, 4]);
}

#[test]
fn many_run_merge_stays_within_one_read_chunk_per_run() {
    for run_count in [10usize, 100, 300] {
        let bridge = Rc::new(RefCell::new(HostSpillBridge::new(run_count as u64)));
        let mut store = HostR2SortedRunStore::new(bridge.clone());
        store.begin().unwrap();
        let mut bodies = std::collections::HashMap::new();
        let mut max_encoded = 0u64;
        for index in 0..run_count {
            let rows = vec![(format!("k{index:04}").into_bytes(), vec![1u8])];
            store.expect_run_rows(rows.len());
            store.append_partials(&rows).unwrap();
            let (key, body) = commit_captured_run(&mut store, &bridge);
            max_encoded = max_encoded.max(body.len() as u64);
            bodies.insert(key, body);
        }
        assert!(max_encoded > 0);
        assert!(max_encoded < u64::from(RUN_READ_CHUNK));
        let (rows, peak) = merge_r2(&mut store, &bridge, &bodies);
        assert_eq!(rows.len(), run_count);
        let mut sorted = rows.clone();
        sorted.sort();
        assert_eq!(rows, sorted, "{run_count} runs merged out of order");
        let ceiling = run_count.saturating_mul(RUN_READ_CHUNK as usize);
        assert!(
            peak <= ceiling,
            "{run_count} runs retained {peak} decoded bytes, ceiling {ceiling}"
        );
    }
}
