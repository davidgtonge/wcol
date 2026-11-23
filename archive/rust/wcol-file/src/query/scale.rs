pub(crate) fn scaled_rhs_pair(value: f64, value2: f64, scale: i32) -> Option<(i64, i64)> {
    let rhs = scale_f64_to_i64(value, scale)?;
    let rhs2 = scale_f64_to_i64(value2, scale)?;
    Some((rhs, rhs2))
}

fn pow10_i64(exp: i32) -> Option<i64> {
    if exp < 0 {
        return None;
    }
    let mut out: i64 = 1;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

pub(crate) fn scale_f64_to_i64(value: f64, scale: i32) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let scaled = if scale == 0 {
        value
    } else if scale > 0 {
        value * (scale as f64)
    } else {
        let factor = pow10_i64(-scale)? as f64;
        value * factor
    };
    let rounded = scaled.round();
    if rounded < (i64::MIN as f64) || rounded > (i64::MAX as f64) {
        return None;
    }
    Some(rounded as i64)
}

pub(crate) fn scale_int_value(value: f64, scale: i32) -> f64 {
    if scale == 0 {
        return value;
    }
    if scale > 0 {
        value / (scale as f64)
    } else {
        value * 10f64.powi(scale)
    }
}
