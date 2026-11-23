use std::path::Path;

use anyhow::{anyhow, Result};
use wcol::{aggregate_column, open_plan, Extreme, Sum};

pub fn run(file: &Path, column: Option<&str>) -> Result<()> {
    let (_handle, plan) = open_plan(file).map_err(|err| anyhow!(err.to_string()))?;
    println!("rows {}", plan.rows);
    for (index, leaf) in plan.leaves.iter().enumerate() {
        let chunks = plan
            .chunks
            .iter()
            .filter(|chunk| chunk.leaf as usize == index)
            .count();
        println!(
            "{}\t{}\tdef={}\trep={}\tchunks={chunks}",
            leaf.name,
            physical_name(leaf.physical),
            leaf.max_def,
            leaf.max_rep,
        );
    }
    let names: Vec<String> = match column {
        Some(name) => vec![name.to_string()],
        None => plan.leaves.iter().map(|leaf| leaf.name.clone()).collect(),
    };
    let mut failed = false;
    for name in &names {
        match aggregate_column(file, name) {
            Ok(agg) => {
                let kind = if agg.bytes { "bytes" } else { "sum" };
                println!(
                    "{name}\tcount={}\tnulls={}\t{kind}={}\tmin={}\tmax={}",
                    agg.count,
                    agg.nulls,
                    fmt_sum(&agg.sum),
                    fmt_extreme(agg.count, &agg.min),
                    fmt_extreme(agg.count, &agg.max)
                );
            }
            Err(err) if column.is_none() => {
                println!("{name}\tskipped\t{err}");
            }
            Err(err) => {
                eprintln!("{name}: {err}");
                failed = true;
            }
        }
    }
    if failed {
        return Err(anyhow!("column could not be aggregated"));
    }
    Ok(())
}

fn physical_name(physical: i32) -> &'static str {
    match physical {
        0 => "boolean",
        1 => "int32",
        2 => "int64",
        3 => "int96",
        4 => "float",
        5 => "double",
        6 => "byte_array",
        7 => "fixed_len_byte_array",
        _ => "unknown",
    }
}

fn fmt_sum(sum: &Sum) -> String {
    match sum {
        Sum::Int(value) => value.to_string(),
        Sum::Float(value) => fmt(*value),
    }
}

fn fmt_extreme(count: u64, extreme: &Extreme) -> String {
    if count == 0 {
        return "nan".to_string();
    }
    match extreme {
        Extreme::Int(value) => value.to_string(),
        Extreme::Float(value) => fmt(*value),
    }
}

fn fmt(value: f64) -> String {
    if value.is_nan() {
        "nan".to_string()
    } else if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}
