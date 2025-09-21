use std::path::{Path, PathBuf};

use anyhow::Result;
use wcol_encoder::convert_to_wcol;

use super::shared::default_output_path;

pub(crate) fn run_convert(
    input: &Path,
    out: Option<PathBuf>,
    show_schema: bool,
    show_stats: bool,
    split_row_groups: Option<usize>,
) -> Result<()> {
    let output = out.unwrap_or_else(|| default_output_path(input));
    let outputs = convert_to_wcol(input, &output, show_schema, show_stats, split_row_groups)?;
    for path in outputs {
        println!("Wrote {}", path.display());
    }
    Ok(())
}
