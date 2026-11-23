//! I-06: query kernels are not re-exported under `formats/`.

use std::path::PathBuf;

#[test]
fn formats_no_query_forwarder_tree() {
    let query_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../wcol-engine/src/formats/wcol/decode/query");
    assert!(
        !query_dir.exists(),
        "I-06: delete forwarder tree {}; import ops::query::wcol instead",
        query_dir.display()
    );
}
