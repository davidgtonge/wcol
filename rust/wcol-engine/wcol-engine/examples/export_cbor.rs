use std::path::PathBuf;
use wcol_engine::engine::Engine;
use wcol_engine::protocol::{decode_input, encode_input, encode_output, WorkerInput};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/.cbor-fixtures")
}

fn main() {
    let dir = fixture_dir();
    let mut engine = Engine::new();
    let init_out = engine.init();
    std::fs::write(dir.join("rust_init_out.cbor"), encode_output(&init_out)).unwrap();

    let url_bytes = std::fs::read(dir.join("url_changed.cbor")).expect("url_changed.cbor");
    let input = decode_input(&url_bytes).expect("decode url_changed");
    let out = engine.handle_input(input);

    std::fs::write(
        dir.join("url_changed_out.json"),
        serde_json::to_string_pretty(&out).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("url_changed_out.cbor"), encode_output(&out)).unwrap();
    std::fs::write(dir.join("rust_init.cbor"), encode_input(&WorkerInput::Init)).unwrap();
}
