use crate::constants::TYPE_F64;
use crate::query::filter::build_single_mask;
use crate::query::mask::mask_count;
use crate::types::{Column, ColumnData};

#[no_mangle]
pub unsafe extern "C" fn bench_f64_filter(
    op: u32,
    rhs: f64,
    rhs2: f64,
    rows: u32,
    iters: u32,
    seed: u64,
) -> u64 {
    let rows = (rows as usize).min(crate::constants::ROWS_PER_CHUNK);
    if rows == 0 || iters == 0 {
        return 0;
    }

    let col = Column {
        id: 0,
        name: "bench_f64".to_string(),
        logical_type: TYPE_F64,
        physical_type: TYPE_F64,
        flags: 0,
        encoding: 0,
        dict_id: 0,
        dict_index_width: 0,
        scale: 0,
    };

    let mut state = seed | 1;
    let mut values = Vec::with_capacity(rows);
    for _ in 0..rows {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let u = ((state >> 11) as f64) * (1.0 / ((1u64 << 53) as f64));
        let mut value = (u * 2048.0) - 1024.0;
        if (state & 0x3ff) == 0 {
            value = f64::NAN;
        }
        values.push(value);
    }
    let data = ColumnData::F64(values);

    let mut checksum = 0u64;
    let mut shift = 0.0f64;
    for _ in 0..iters {
        let mask = build_single_mask(&col, &data, op as u8, rhs + shift, rhs2 + shift, rows);
        let c = mask_count(&mask) as u64;
        checksum = checksum
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(c ^ 0xA076_1D64_78BD_642F);
        shift += 0.25;
        if shift >= 4.0 {
            shift = 0.0;
        }
    }
    std::hint::black_box(checksum)
}
