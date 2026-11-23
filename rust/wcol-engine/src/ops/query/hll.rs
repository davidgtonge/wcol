//! HyperLogLog sketch for `approx_count_distinct`, ported from the `.wcol` file engine.
//!
//! Precision is fixed at p = 14 (16 384 registers, ~0.8% standard error) so sketches from any
//! worker merge. Values are hashed as `i64` with xxh3, whatever their Parquet width, so shards
//! reading different physical types still agree. Small sketches stay sparse (a sorted list of
//! `(register, rank)`) and turn dense once that list would outgrow the registers.

use xxhash_rust::xxh3::xxh3_64;

pub const HLL_PRECISION: u8 = 14;
const REGISTERS: usize = 1 << HLL_PRECISION;
/// Bytes of a dense sketch: the most one group's sketch holds.
pub const HLL_DENSE_BYTES: usize = REGISTERS;
/// Sparse entries take 3 bytes on the wire; past this many a dense sketch is smaller.
const SPARSE_MAX: usize = REGISTERS / 3;

const TAG_SPARSE: u8 = 0;
const TAG_DENSE: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Registers {
    /// `(register << 8) | rank`, sorted by register, at most one entry per register.
    Sparse(Vec<u32>),
    Dense(Box<[u8]>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hll {
    registers: Registers,
}

impl Default for Hll {
    fn default() -> Self {
        Self::new()
    }
}

impl Hll {
    pub fn new() -> Self {
        Self {
            registers: Registers::Sparse(Vec::new()),
        }
    }

    #[inline]
    pub fn insert_i64(&mut self, value: i64) {
        self.insert_hash(hash_i64(value));
    }

    #[inline]
    pub fn insert_hash(&mut self, hash: u64) {
        let (register, rank) = register_rank(hash);
        self.set_max(register, rank);
    }

    #[inline]
    fn set_max(&mut self, register: usize, rank: u8) {
        match &mut self.registers {
            Registers::Dense(regs) => {
                if rank > regs[register] {
                    regs[register] = rank;
                }
            }
            Registers::Sparse(entries) => {
                match entries.binary_search_by_key(&register, |e| (*e >> 8) as usize) {
                    Ok(at) => {
                        if rank > (entries[at] & 0xff) as u8 {
                            entries[at] = ((register as u32) << 8) | u32::from(rank);
                        }
                    }
                    Err(at) => {
                        entries.insert(at, ((register as u32) << 8) | u32::from(rank));
                        if entries.len() > SPARSE_MAX {
                            self.densify();
                        }
                    }
                }
            }
        }
    }

    fn densify(&mut self) {
        if let Registers::Sparse(entries) = &self.registers {
            let mut regs = vec![0u8; REGISTERS].into_boxed_slice();
            for e in entries {
                regs[(*e >> 8) as usize] = (*e & 0xff) as u8;
            }
            self.registers = Registers::Dense(regs);
        }
    }

    /// Register-wise maximum: the sketch of the union of both inputs.
    pub fn merge(&mut self, other: &Hll) {
        match &other.registers {
            Registers::Sparse(entries) => {
                for e in entries {
                    self.set_max((*e >> 8) as usize, (*e & 0xff) as u8);
                }
            }
            Registers::Dense(theirs) => {
                self.densify();
                if let Registers::Dense(ours) = &mut self.registers {
                    for (o, t) in ours.iter_mut().zip(theirs.iter()) {
                        *o = (*o).max(*t);
                    }
                }
            }
        }
    }

    /// Estimated distinct count (standard estimator with the small-range correction).
    pub fn estimate(&self) -> f64 {
        let m = REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let (sum, zeros) = match &self.registers {
            Registers::Dense(regs) => regs.iter().fold((0.0f64, 0u32), |(sum, zeros), r| {
                (sum + 2f64.powi(-i32::from(*r)), zeros + u32::from(*r == 0))
            }),
            Registers::Sparse(entries) => {
                let set: f64 = entries
                    .iter()
                    .map(|e| 2f64.powi(-((*e & 0xff) as i32)))
                    .sum();
                let zeros = (REGISTERS - entries.len()) as u32;
                (set + f64::from(zeros), zeros)
            }
        };
        let raw = alpha * m * m / sum;
        if raw <= 2.5 * m && zeros > 0 {
            return m * (m / f64::from(zeros)).ln();
        }
        raw
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.registers {
                Registers::Sparse(entries) => entries.capacity() * std::mem::size_of::<u32>(),
                Registers::Dense(regs) => regs.len(),
            }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match &self.registers {
            Registers::Sparse(entries) => {
                out.push(TAG_SPARSE);
                out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for e in entries {
                    out.extend_from_slice(&((*e >> 8) as u16).to_le_bytes());
                    out.push((*e & 0xff) as u8);
                }
            }
            Registers::Dense(regs) => {
                out.push(TAG_DENSE);
                out.extend_from_slice(regs);
            }
        }
    }

    /// Decode one sketch from the front of `bytes`; returns it and the bytes used.
    pub fn decode(bytes: &[u8]) -> Result<(Hll, usize), String> {
        let tag = *bytes.first().ok_or("truncated HLL sketch")?;
        match tag {
            TAG_SPARSE => {
                let count = u32::from_le_bytes(
                    bytes
                        .get(1..5)
                        .ok_or("truncated HLL sketch")?
                        .try_into()
                        .unwrap(),
                ) as usize;
                if count > SPARSE_MAX {
                    return Err("HLL sparse sketch is too large".to_string());
                }
                let body = bytes.get(5..5 + count * 3).ok_or("truncated HLL sketch")?;
                let mut entries = Vec::with_capacity(count);
                let mut last = None;
                for chunk in body.chunks_exact(3) {
                    let register = u16::from_le_bytes([chunk[0], chunk[1]]) as usize;
                    let rank = chunk[2];
                    if register >= REGISTERS || rank as usize > 64 - HLL_PRECISION as usize + 1 {
                        return Err("HLL sketch entry out of range".to_string());
                    }
                    if last.is_some_and(|l| l >= register) {
                        return Err("HLL sparse sketch is not sorted".to_string());
                    }
                    last = Some(register);
                    entries.push(((register as u32) << 8) | u32::from(rank));
                }
                Ok((
                    Hll {
                        registers: Registers::Sparse(entries),
                    },
                    5 + count * 3,
                ))
            }
            TAG_DENSE => {
                let regs = bytes.get(1..1 + REGISTERS).ok_or("truncated HLL sketch")?;
                Ok((
                    Hll {
                        registers: Registers::Dense(regs.to_vec().into_boxed_slice()),
                    },
                    1 + REGISTERS,
                ))
            }
            _ => Err("unknown HLL sketch encoding".to_string()),
        }
    }
}

#[inline]
pub fn hash_i64(value: i64) -> u64 {
    xxh3_64(&value.to_le_bytes())
}

/// Top `p` bits pick the register; the rank is 1 + leading zeros of the rest.
#[inline]
fn register_rank(hash: u64) -> (usize, u8) {
    let register = (hash >> (64 - HLL_PRECISION)) as usize;
    let rest = hash << HLL_PRECISION;
    let rank = if rest == 0 {
        64 - HLL_PRECISION + 1
    } else {
        rest.leading_zeros() as u8 + 1
    };
    (register, rank)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch(values: impl IntoIterator<Item = i64>) -> Hll {
        let mut hll = Hll::new();
        for v in values {
            hll.insert_i64(v);
        }
        hll
    }

    #[test]
    fn estimates_are_within_a_few_percent() {
        for n in [0i64, 1, 10, 1_000, 50_000, 400_000] {
            let estimate = sketch(0..n).estimate();
            let error = (estimate - n as f64).abs() / (n.max(1) as f64);
            assert!(error < 0.03, "n {n}: estimate {estimate}");
        }
    }

    #[test]
    fn duplicates_do_not_count() {
        let once = sketch(0..5_000).estimate();
        let thrice = sketch((0..15_000).map(|v| v % 5_000)).estimate();
        assert_eq!(once, thrice);
    }

    #[test]
    fn merge_equals_sketch_of_union_sparse_and_dense() {
        for (a, b) in [
            (0..100, 50..300),
            (0..20_000, 10_000..60_000),
            (0..10, 0..40_000),
        ] {
            let mut merged = sketch(a.clone());
            merged.merge(&sketch(b.clone()));
            let union = sketch(a.chain(b));
            assert_eq!(merged.estimate(), union.estimate());
        }
    }

    #[test]
    fn encode_round_trips_and_sparse_stays_small() {
        for n in [0i64, 3, 2_000, 30_000] {
            let hll = sketch(0..n);
            let mut bytes = Vec::new();
            hll.encode(&mut bytes);
            let (back, used) = Hll::decode(&bytes).unwrap();
            assert_eq!(used, bytes.len());
            assert_eq!(back, hll);
            assert!(bytes.len() <= 1 + REGISTERS);
        }
        let mut small = Vec::new();
        sketch(0..3).encode(&mut small);
        assert_eq!(small.len(), 5 + 3 * 3);
    }

    #[test]
    fn decode_rejects_bad_input() {
        assert!(Hll::decode(&[]).is_err());
        assert!(Hll::decode(&[TAG_SPARSE, 1, 0, 0, 0, 0xff, 0xff, 1]).is_err());
        assert!(Hll::decode(&[9]).is_err());
    }
}
