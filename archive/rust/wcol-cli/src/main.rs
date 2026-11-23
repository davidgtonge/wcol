use anyhow::Result;
use clap::Parser;

mod app;
mod cli;

use cli::Args;

#[no_mangle]
pub extern "C" fn wcol_now_ms() -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        0.0
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        now.as_secs_f64() * 1000.0
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    app::run(args)
}
