//! I-07: `stream/` must not depend on `exec/` or `formats/`.

use std::fs;
use std::path::PathBuf;

#[test]
fn stream_module_has_no_exec_or_formats_imports() {
    let stream_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../wcol-engine/src/stream");
    let mut violations = Vec::new();
    for entry in fs::read_dir(&stream_dir).expect("stream dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy();
        let text = fs::read_to_string(&path).expect("read stream source");
        for (line_no, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") || trimmed.starts_with("///") || trimmed.starts_with("//!")
            {
                continue;
            }
            if line.contains("crate::exec::") || line.contains("crate::formats::") {
                violations.push(format!("{}:{}: {}", name, line_no + 1, trimmed));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "I-07 stream layer must not import exec/ or formats/:\n{}",
        violations.join("\n")
    );
}
