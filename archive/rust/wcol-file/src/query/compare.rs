// Filter compare opcodes (same values as `formats/wcol/decode/constants.rs`).
const OP_EQ: u8 = 0;
const OP_NEQ: u8 = 1;
const OP_LT: u8 = 2;
const OP_LTE: u8 = 3;
const OP_GT: u8 = 4;
const OP_GTE: u8 = 5;
const OP_BETWEEN: u8 = 6;

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
