use crate::constants::{OP_BETWEEN, OP_EQ, OP_GT, OP_GTE, OP_LT, OP_LTE, OP_NEQ};

#[inline]
fn cmp_scalar<T>(val: T, op: u8, rhs: T, rhs2: T) -> bool
where
    T: PartialEq + PartialOrd,
{
    match op {
        OP_EQ => val == rhs,
        OP_NEQ => val != rhs,
        OP_LT => val < rhs,
        OP_LTE => val <= rhs,
        OP_GT => val > rhs,
        OP_GTE => val >= rhs,
        OP_BETWEEN => val >= rhs && val <= rhs2,
        _ => false,
    }
}

macro_rules! define_cmp {
    ($name:ident, $ty:ty) => {
        #[inline]
        pub(crate) fn $name(val: $ty, op: u8, rhs: $ty, rhs2: $ty) -> bool {
            cmp_scalar(val, op, rhs, rhs2)
        }
    };
}

define_cmp!(cmp_i32, i32);
define_cmp!(cmp_i64, i64);
define_cmp!(cmp_u32, u32);

#[inline]
pub(crate) fn cmp_f64(val: f64, op: u8, rhs: f64, rhs2: f64) -> bool {
    if val.is_nan() {
        return false;
    }
    cmp_scalar(val, op, rhs, rhs2)
}
