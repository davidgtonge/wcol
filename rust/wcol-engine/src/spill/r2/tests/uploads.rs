use super::*;

#[test]
fn two_finalize_cycles_register_both_runs() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(9)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let rows_a: Vec<_> = (0..3)
        .map(|i| (format!("a{i}").into_bytes(), vec![1u8]))
        .collect();
    let rows_b: Vec<_> = (0..5)
        .map(|i| (format!("b{i}").into_bytes(), vec![2u8]))
        .collect();
    store.expect_run_rows(rows_a.len());
    store.append_partials(&rows_a).unwrap();
    drive_put_commit(&mut store, &bridge);
    assert_eq!(store.runs.len(), 1);
    assert_eq!(store.runs[0].record_count, 3);
    store.expect_run_rows(rows_b.len());
    store.append_partials(&rows_b).unwrap();
    drive_put_commit(&mut store, &bridge);
    assert_eq!(store.runs.len(), 2);
    assert_eq!(store.runs[1].record_count, 5);
    assert_eq!(store.run_seq, 2);
}

#[test]
fn finalize_run_stages_single_encoded_body_in_bridge() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(1)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    store.expect_run_rows(1);
    store
        .append_partials(&[(b"alpha".to_vec(), b"partial".to_vec())])
        .unwrap();
    assert!(matches!(
        store.finalize_run().unwrap(),
        SpillProgress::AwaitHost
    ));
    assert_eq!(bridge.borrow().pending_op(), Some(SPILL_OP_RUN_PUT));
    let staged_len = bridge.borrow().run_upload.len();
    assert!(
        staged_len > 0,
        "encoded run body should live in bridge.run_upload"
    );
    assert_eq!(bridge.borrow().run_upload_off, 0);
}

#[test]
fn append_while_put_in_flight_survives_commit() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(1)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    let rows_a = vec![(b"k1".to_vec(), b"v1".to_vec())];
    let rows_b = vec![(b"k2".to_vec(), b"v2".to_vec())];
    store.expect_run_rows(rows_a.len());
    store.append_partials(&rows_a).unwrap();
    assert!(matches!(
        store.finalize_run().unwrap(),
        SpillProgress::AwaitHost
    ));
    assert_eq!(bridge.borrow().pending_op(), Some(SPILL_OP_RUN_PUT));
    bridge.borrow_mut().complete_run_put().unwrap();
    store.on_run_put_handoff().unwrap();
    store.expect_run_rows(rows_b.len());
    store.append_partials(&rows_b).unwrap();
    drive_put_commit(&mut store, &bridge);
    assert_eq!(store.runs.len(), 2);
    assert_eq!(store.runs[0].record_count, 1);
    assert_eq!(store.runs[1].record_count, 1);
}

#[test]
fn pages_encode_the_same_body_as_one_encoded_run() {
    let rows: Vec<_> = (0..7u8)
        .map(|i| (vec![b'k', i], vec![i; usize::from(i) + 1]))
        .collect();
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(4)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    store.expect_run_rows(rows.len());
    for page in rows.chunks(3) {
        store.append_partials(page).unwrap();
    }
    let (_key, body) = commit_captured_run(&mut store, &bridge);
    let mut expected = Vec::new();
    encode_run(&mut expected, &rows).unwrap();
    assert_eq!(body, expected);
    assert_eq!(store.runs[0].record_count, 7);
}

#[test]
fn expected_rows_size_the_body_once() {
    let rows: Vec<_> = (0..1000u32)
        .map(|i| (i.to_be_bytes().to_vec(), vec![7u8; 40]))
        .collect();
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(6)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    store.expect_run_rows(rows.len());
    let mut pages = rows.chunks(64);
    store.append_partials(pages.next().unwrap()).unwrap();
    let reserved = store.run_body.capacity();
    for page in pages {
        store.expect_run_rows(page.len());
        store.append_partials(page).unwrap();
    }
    assert_eq!(store.run_body.capacity(), reserved, "run body reallocated");
    let (_key, body) = commit_captured_run(&mut store, &bridge);
    assert!(body.len() <= reserved);
}

#[test]
fn next_run_reuses_the_uploaded_body() {
    let rows: Vec<_> = (0..500u32)
        .map(|i| (i.to_be_bytes().to_vec(), vec![3u8; 64]))
        .collect();
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(7)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    store.expect_run_rows(rows.len());
    store.expect_run_rows(rows.len());
    store.append_partials(&rows).unwrap();
    let capacity = store.run_body.capacity();
    let _ = commit_captured_run(&mut store, &bridge);
    // What the graph host does once the upload finishes: the body comes back, cleared.
    let spare: Vec<u8> = Vec::with_capacity(capacity);
    let first = spare.as_ptr();
    bridge.borrow_mut().run_upload = spare;
    store.expect_run_rows(rows.len());
    store.expect_run_rows(rows.len());
    store.append_partials(&rows).unwrap();
    assert_eq!(
        store.run_body.as_ptr(),
        first,
        "second run allocated a new body"
    );
    assert_eq!(store.run_body.capacity(), capacity);
}

#[test]
fn long_run_streams_in_equal_parts() {
    let rows: Vec<_> = (0..40_000u32)
        .map(|i| (i.to_be_bytes().to_vec(), vec![5u8; 300]))
        .collect();
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(11)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    store.expect_run_rows(rows.len());
    let mut parts = Vec::new();
    let mut peak = 0usize;
    for page in rows.chunks(64) {
        let progress = store.append_partials(page).unwrap();
        peak = peak.max(store.run_body.capacity());
        if matches!(progress, SpillProgress::AwaitHost) {
            let req = bridge.borrow().pending_request().expect("part put");
            let plan = decode_run_put_open(&req.append_bytes).unwrap();
            assert!(!plan.last);
            assert_eq!(plan.part as usize, parts.len() + 1);
            assert_eq!(plan.byte_length as usize, RUN_PART_BYTES);
            parts.push(std::mem::take(&mut bridge.borrow_mut().run_upload));
            bridge.borrow_mut().complete_run_put().unwrap();
            store.on_run_put_handoff().unwrap();
        }
    }
    let (_key, last) = commit_captured_run(&mut store, &bridge);
    assert!(parts.len() >= 2, "expected a multipart run");
    assert!(peak <= put::PART_BUFFER_BYTES, "part buffer grew to {peak}");
    let body: Vec<u8> = parts.into_iter().flatten().chain(last).collect();
    let mut expected = Vec::new();
    encode_run(&mut expected, &rows).unwrap();
    assert!(
        body == expected,
        "streamed parts differ from the encoded run"
    );
    assert_eq!(store.runs[0].byte_length, expected.len() as u64);
}

#[test]
fn run_with_more_pages_than_declared_is_rejected() {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(12)));
    let mut store = HostR2SortedRunStore::new(bridge);
    store.begin().unwrap();
    assert!(store.append_partials(&[(b"a".to_vec(), vec![1])]).is_err());
    store.expect_run_rows(1);
    store.append_partials(&[(b"a".to_vec(), vec![1])]).unwrap();
    assert!(store.append_partials(&[(b"b".to_vec(), vec![1])]).is_err());
}

#[test]
fn merge_reads_reuse_the_part_buffer() {
    let rows: Vec<_> = (0..100u32)
        .map(|i| (i.to_be_bytes().to_vec(), vec![1u8; 32]))
        .collect();
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(13)));
    let mut store = HostR2SortedRunStore::new(bridge.clone());
    store.begin().unwrap();
    store.expect_run_rows(rows.len());
    store.append_partials(&rows).unwrap();
    let (key, body) = commit_captured_run(&mut store, &bridge);
    // What the graph host does after an upload: the part buffer comes back, cleared.
    let spare: Vec<u8> = Vec::with_capacity(1 << 20);
    let at = spare.as_ptr();
    bridge.borrow_mut().run_upload = spare;
    let bodies = std::collections::HashMap::from([(key, body)]);
    let (merged, _) = merge_r2(&mut store, &bridge, &bodies);
    assert_eq!(merged.len(), rows.len());
    assert_eq!(bridge.borrow_mut().run_read_slot(16).as_ptr(), at);
}
