mod bench;
mod convert;
mod parity;
mod query;
mod shared;

use anyhow::{anyhow, Result};

use crate::cli::{Args, Commands};

pub(crate) fn run(args: Args) -> Result<()> {
    match args.command {
        Some(Commands::Convert {
            input,
            out,
            show_schema,
            show_stats,
            split_row_groups,
        }) => convert::run_convert(&input, out, show_schema, show_stats, split_row_groups),
        Some(Commands::Query {
            file,
            sql,
            sql_file,
            workers,
            format,
            native,
        }) => query::run_query_cmd(&file, sql, sql_file, workers, format, native),
        Some(Commands::Bench {
            file,
            sql_file,
            runs,
            warmup,
            workers,
            only,
            native,
        }) => bench::run_bench_cmd(&file, &sql_file, runs, warmup, workers, only, native),
        Some(Commands::BenchWorker { file }) => bench::run_bench_worker_cmd(&file),
        Some(Commands::Parity {
            wcol_file,
            parquet_file,
            sql_file,
            workers,
            only,
            native,
        }) => parity::run_parity_cmd(&wcol_file, &parquet_file, &sql_file, workers, only, native),
        None => {
            let input = args
                .input
                .ok_or_else(|| anyhow!("missing input path (or use subcommands)"))?;
            convert::run_convert(&input, args.out, args.show_schema, args.show_stats, None)
        }
    }
}
