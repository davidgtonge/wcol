//! Stateful iterative kernels. Resident graph sinks own their feature partitions; kernels
//! define local rounds, partial merging, and global updates.

pub mod resident;
pub mod wire;

/// An algorithm executed as coordinated rounds over resident worker partitions.
///
/// `GlobalState` and `Partial` remain typed inside the kernel. Implementations provide
/// explicit wire encodings because RPC messages and durable checkpoints outlive one call.
pub trait IterativeKernel {
    type Resident;
    type GlobalState;
    type Partial;
    type Error;

    fn load(&self, values: &[f32], dimensions: usize) -> Result<Self::Resident, Self::Error>;
    fn compute(
        &self,
        resident: &Self::Resident,
        state: &Self::GlobalState,
        iteration: u64,
    ) -> Result<Self::Partial, Self::Error>;
    fn merge(
        &self,
        left: Self::Partial,
        right: Self::Partial,
    ) -> Result<Self::Partial, Self::Error>;
    fn update(
        &self,
        state: &mut Self::GlobalState,
        partial: Self::Partial,
    ) -> Result<bool, Self::Error>;
    fn encode_state(&self, state: &Self::GlobalState) -> Result<Vec<u8>, Self::Error>;
    fn decode_state(&self, bytes: &[u8]) -> Result<Self::GlobalState, Self::Error>;
    fn encode_partial(&self, partial: &Self::Partial) -> Result<Vec<u8>, Self::Error>;
    fn decode_partial(&self, bytes: &[u8]) -> Result<Self::Partial, Self::Error>;
}

/// Merge partials in deterministic bounded fan-in levels. Input order is preserved so
/// retries and differently sized batches use the same merge tree when their leaves match.
pub fn merge_tree<K: IterativeKernel>(
    kernel: &K,
    mut partials: Vec<K::Partial>,
    fan_in: usize,
) -> Result<Option<K::Partial>, K::Error> {
    assert!(
        fan_in >= 2,
        "iterative reduction fan-in must be at least two"
    );
    while partials.len() > 1 {
        let mut next = Vec::with_capacity(partials.len().div_ceil(fan_in));
        let mut group = partials.into_iter();
        loop {
            let Some(mut merged) = group.next() else {
                break;
            };
            for partial in group.by_ref().take(fan_in - 1) {
                merged = kernel.merge(merged, partial)?;
            }
            next.push(merged);
        }
        partials = next;
    }
    Ok(partials.pop())
}

#[derive(Clone, Debug, PartialEq)]
pub struct KMeansConfig {
    pub clusters: usize,
    pub dimensions: usize,
    pub max_iterations: u32,
    pub tolerance: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KMeansState {
    pub centroids: Vec<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KMeansPartial {
    pub sums: Vec<f64>,
    pub counts: Vec<u64>,
    pub inertia: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KMeansResident {
    values: Vec<f32>,
    rows: usize,
    dimensions: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeatureMoments {
    pub counts: Vec<u64>,
    pub sums: Vec<f64>,
    pub sum_squares: Vec<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KMeansError {
    InvalidConfig,
    InvalidMatrix,
    InvalidState,
    InvalidPartial,
    InvalidWire,
}

/// K-means local work and global update for the resident iterative runtime.
#[derive(Clone, Debug)]
pub struct KMeansKernel {
    pub config: KMeansConfig,
}

impl KMeansKernel {
    /// Take ownership of normalized feature memory so resident sessions do not duplicate it.
    pub fn load_owned(
        &self,
        values: Vec<f32>,
        dimensions: usize,
    ) -> Result<KMeansResident, KMeansError> {
        self.validate_config()?;
        if dimensions != self.config.dimensions
            || values.len() % dimensions != 0
            || values.iter().any(|value| !value.is_finite())
        {
            return Err(KMeansError::InvalidMatrix);
        }
        Ok(KMeansResident {
            rows: values.len() / dimensions,
            values,
            dimensions,
        })
    }

    /// Build deterministic K-means++ centroids from a bounded row-major sample.
    pub fn seeded_centroids(&self, sample: &[f32], seed: u64) -> Result<KMeansState, KMeansError> {
        self.validate_config()?;
        let d = self.config.dimensions;
        if sample.is_empty() || sample.len() % d != 0 || sample.iter().any(|v| !v.is_finite()) {
            return Err(KMeansError::InvalidMatrix);
        }
        let rows = sample.len() / d;
        let mut random = SplitMix64(seed);
        let mut chosen = Vec::with_capacity(self.config.clusters);
        chosen.push((random.next() as usize) % rows);
        let mut distances = vec![f64::INFINITY; rows];
        while chosen.len() < self.config.clusters {
            let latest = chosen[chosen.len() - 1];
            let center = &sample[latest * d..(latest + 1) * d];
            let mut total = 0.0;
            for row in 0..rows {
                let values = &sample[row * d..(row + 1) * d];
                let distance = values.iter().zip(center).fold(0.0, |sum, (value, c)| {
                    let delta = f64::from(*value) - f64::from(*c);
                    sum + delta * delta
                });
                distances[row] = distances[row].min(distance);
                total += distances[row];
            }
            if total == 0.0 {
                chosen.push(chosen.len() % rows);
                continue;
            }
            let target = random.unit() * total;
            let mut cumulative = 0.0;
            let mut selected = rows - 1;
            for (row, distance) in distances.iter().enumerate() {
                cumulative += *distance;
                if cumulative >= target {
                    selected = row;
                    break;
                }
            }
            chosen.push(selected);
        }
        let centroids = chosen
            .into_iter()
            .flat_map(|row| {
                sample[row * d..(row + 1) * d]
                    .iter()
                    .map(|value| f64::from(*value))
            })
            .collect();
        self.initial_state(centroids)
    }

    pub fn initial_state(&self, centroids: Vec<f64>) -> Result<KMeansState, KMeansError> {
        self.validate_config()?;
        if centroids.len() != self.config.clusters * self.config.dimensions
            || centroids.iter().any(|value| !value.is_finite())
        {
            return Err(KMeansError::InvalidState);
        }
        Ok(KMeansState { centroids })
    }

    fn validate_config(&self) -> Result<(), KMeansError> {
        if self.config.clusters == 0
            || self.config.dimensions == 0
            || self.config.max_iterations == 0
            || !self.config.tolerance.is_finite()
            || self.config.tolerance < 0.0
        {
            return Err(KMeansError::InvalidConfig);
        }
        Ok(())
    }

    fn validate_state(&self, state: &KMeansState) -> Result<(), KMeansError> {
        if state.centroids.len() != self.config.clusters * self.config.dimensions
            || state.centroids.iter().any(|value| !value.is_finite())
        {
            return Err(KMeansError::InvalidState);
        }
        Ok(())
    }

    fn validate_partial(&self, partial: &KMeansPartial) -> Result<(), KMeansError> {
        if partial.sums.len() != self.config.clusters * self.config.dimensions
            || partial.counts.len() != self.config.clusters
            || !partial.inertia.is_finite()
            || partial.inertia < 0.0
            || partial.sums.iter().any(|value| !value.is_finite())
        {
            return Err(KMeansError::InvalidPartial);
        }
        Ok(())
    }
}

impl FeatureMoments {
    pub fn from_nullable_rows(values: &[f64], dimensions: usize) -> Result<Self, KMeansError> {
        if dimensions == 0 || values.is_empty() || values.len() % dimensions != 0 {
            return Err(KMeansError::InvalidMatrix);
        }
        let mut moments = Self {
            counts: vec![0; dimensions],
            sums: vec![0.0; dimensions],
            sum_squares: vec![0.0; dimensions],
        };
        for row in values.chunks_exact(dimensions) {
            for (column, value) in row.iter().enumerate() {
                if value.is_nan() {
                    continue;
                }
                if !value.is_finite() {
                    return Err(KMeansError::InvalidMatrix);
                }
                moments.counts[column] += 1;
                moments.sums[column] += value;
                moments.sum_squares[column] += value * value;
            }
        }
        Ok(moments)
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), KMeansError> {
        if self.counts.len() != other.counts.len()
            || other.sums.len() != other.counts.len()
            || other.sum_squares.len() != other.counts.len()
        {
            return Err(KMeansError::InvalidMatrix);
        }
        for index in 0..self.counts.len() {
            self.counts[index] = self.counts[index]
                .checked_add(other.counts[index])
                .ok_or(KMeansError::InvalidMatrix)?;
            self.sums[index] += other.sums[index];
            self.sum_squares[index] += other.sum_squares[index];
        }
        Ok(())
    }

    /// Nulls become zero after centering; constant and all-null columns become zero.
    pub fn standardize(&self, values: &[f64]) -> Result<Vec<f32>, KMeansError> {
        let dimensions = self.counts.len();
        if dimensions == 0 || values.len() % dimensions != 0 {
            return Err(KMeansError::InvalidMatrix);
        }
        let means: Vec<f64> = self
            .counts
            .iter()
            .enumerate()
            .map(|(column, count)| {
                if *count == 0 {
                    0.0
                } else {
                    self.sums[column] / *count as f64
                }
            })
            .collect();
        let scales: Vec<f64> = self
            .counts
            .iter()
            .enumerate()
            .map(|(column, count)| {
                if *count == 0 {
                    return 0.0;
                }
                let mean = means[column];
                let variance = (self.sum_squares[column] / *count as f64 - mean * mean).max(0.0);
                if variance <= f64::EPSILON {
                    0.0
                } else {
                    variance.sqrt()
                }
            })
            .collect();
        let mut output = Vec::with_capacity(values.len());
        for row in values.chunks_exact(dimensions) {
            for (column, value) in row.iter().enumerate() {
                let standardized = if scales[column] == 0.0 || value.is_nan() {
                    0.0
                } else {
                    (value - means[column]) / scales[column]
                };
                let value = standardized as f32;
                if !value.is_finite() {
                    return Err(KMeansError::InvalidMatrix);
                }
                output.push(value);
            }
        }
        Ok(output)
    }
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

impl IterativeKernel for KMeansKernel {
    type Resident = KMeansResident;
    type GlobalState = KMeansState;
    type Partial = KMeansPartial;
    type Error = KMeansError;

    fn load(&self, values: &[f32], dimensions: usize) -> Result<Self::Resident, Self::Error> {
        self.load_owned(values.to_vec(), dimensions)
    }

    fn compute(
        &self,
        resident: &Self::Resident,
        state: &Self::GlobalState,
        _iteration: u64,
    ) -> Result<Self::Partial, Self::Error> {
        self.validate_state(state)?;
        if resident.dimensions != self.config.dimensions
            || resident.values.len() != resident.rows * resident.dimensions
        {
            return Err(KMeansError::InvalidMatrix);
        }
        let k = self.config.clusters;
        let d = self.config.dimensions;
        let mut partial = KMeansPartial {
            sums: vec![0.0; k * d],
            counts: vec![0; k],
            inertia: 0.0,
        };
        for row in resident.values.chunks_exact(d) {
            let mut best_cluster = 0;
            let mut best_distance = f64::INFINITY;
            for cluster in 0..k {
                let centroid = &state.centroids[cluster * d..(cluster + 1) * d];
                let distance = row.iter().zip(centroid).fold(0.0, |sum, (value, center)| {
                    let delta = f64::from(*value) - center;
                    sum + delta * delta
                });
                if distance < best_distance {
                    best_distance = distance;
                    best_cluster = cluster;
                }
            }
            partial.counts[best_cluster] += 1;
            partial.inertia += best_distance;
            for (dimension, value) in row.iter().enumerate() {
                partial.sums[best_cluster * d + dimension] += f64::from(*value);
            }
        }
        Ok(partial)
    }

    fn merge(
        &self,
        mut left: Self::Partial,
        right: Self::Partial,
    ) -> Result<Self::Partial, Self::Error> {
        self.validate_partial(&left)?;
        self.validate_partial(&right)?;
        for (a, b) in left.sums.iter_mut().zip(right.sums) {
            *a += b;
        }
        for (a, b) in left.counts.iter_mut().zip(right.counts) {
            *a = a.checked_add(b).ok_or(KMeansError::InvalidPartial)?;
        }
        left.inertia += right.inertia;
        if !left.inertia.is_finite() {
            return Err(KMeansError::InvalidPartial);
        }
        Ok(left)
    }

    fn update(
        &self,
        state: &mut Self::GlobalState,
        partial: Self::Partial,
    ) -> Result<bool, Self::Error> {
        self.validate_state(state)?;
        self.validate_partial(&partial)?;
        let d = self.config.dimensions;
        let mut max_movement = 0.0f64;
        for cluster in 0..self.config.clusters {
            let count = partial.counts[cluster];
            if count == 0 {
                continue;
            }
            for dimension in 0..d {
                let at = cluster * d + dimension;
                let next = partial.sums[at] / count as f64;
                let delta = next - state.centroids[at];
                max_movement = max_movement.max(delta * delta);
                state.centroids[at] = next;
            }
        }
        Ok(max_movement.sqrt() <= self.config.tolerance)
    }

    fn encode_state(&self, state: &Self::GlobalState) -> Result<Vec<u8>, Self::Error> {
        self.validate_state(state)?;
        let mut out = wire_header(1, state.centroids.len())?;
        for value in &state.centroids {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode_state(&self, bytes: &[u8]) -> Result<Self::GlobalState, Self::Error> {
        let count = read_wire_header(bytes, 1)?;
        if count != self.config.clusters * self.config.dimensions || bytes.len() != 9 + count * 8 {
            return Err(KMeansError::InvalidWire);
        }
        let centroids = bytes[9..]
            .chunks_exact(8)
            .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        self.initial_state(centroids)
    }

    fn encode_partial(&self, partial: &Self::Partial) -> Result<Vec<u8>, Self::Error> {
        self.validate_partial(partial)?;
        let mut out = wire_header(2, partial.sums.len())?;
        out.extend_from_slice(&(partial.counts.len() as u32).to_le_bytes());
        out.extend_from_slice(&partial.inertia.to_le_bytes());
        for value in &partial.sums {
            out.extend_from_slice(&value.to_le_bytes());
        }
        for value in &partial.counts {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn decode_partial(&self, bytes: &[u8]) -> Result<Self::Partial, Self::Error> {
        if bytes.len() < 20 || &bytes[..4] != b"WIT1" || bytes[4] != 2 {
            return Err(KMeansError::InvalidWire);
        }
        let sums = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let counts = u32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
        if sums != self.config.clusters * self.config.dimensions
            || counts != self.config.clusters
            || bytes.len() != 21 + sums * 8 + counts * 8
        {
            return Err(KMeansError::InvalidWire);
        }
        let inertia = f64::from_le_bytes(bytes[13..21].try_into().unwrap());
        let mut at = 21;
        let read_f64s = |at: &mut usize, n: usize| {
            let values = bytes[*at..*at + n * 8]
                .chunks_exact(8)
                .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
                .collect::<Vec<_>>();
            *at += n * 8;
            values
        };
        let sum_values = read_f64s(&mut at, sums);
        let count_values = bytes[at..]
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        let partial = KMeansPartial {
            sums: sum_values,
            counts: count_values,
            inertia,
        };
        self.validate_partial(&partial)?;
        Ok(partial)
    }
}

fn wire_header(kind: u8, count: usize) -> Result<Vec<u8>, KMeansError> {
    let count = u32::try_from(count).map_err(|_| KMeansError::InvalidWire)?;
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(b"WIT1");
    out.push(kind);
    out.extend_from_slice(&count.to_le_bytes());
    Ok(out)
}

fn read_wire_header(bytes: &[u8], kind: u8) -> Result<usize, KMeansError> {
    if bytes.len() < 9 || &bytes[..4] != b"WIT1" || bytes[4] != kind {
        return Err(KMeansError::InvalidWire);
    }
    Ok(u32::from_le_bytes(
        bytes[5..9]
            .try_into()
            .map_err(|_| KMeansError::InvalidWire)?,
    ) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kernel(k: usize, d: usize) -> KMeansKernel {
        KMeansKernel {
            config: KMeansConfig {
                clusters: k,
                dimensions: d,
                max_iterations: 100,
                tolerance: 1e-4,
            },
        }
    }

    #[test]
    fn resident_round_merges_and_updates_centroids() {
        let kernel = kernel(2, 1);
        let resident = kernel.load(&[0.0, 1.0, 9.0, 10.0], 1).unwrap();
        let mut state = kernel.initial_state(vec![0.0, 10.0]).unwrap();
        let partial = kernel.compute(&resident, &state, 1).unwrap();
        assert_eq!(partial.counts, vec![2, 2]);
        assert!(!kernel.update(&mut state, partial).unwrap());
        assert_eq!(state.centroids, vec![0.5, 9.5]);
        let partial = kernel.compute(&resident, &state, 2).unwrap();
        assert!(kernel.update(&mut state, partial).unwrap());
    }

    #[test]
    fn partial_merge_is_deterministic_and_wire_round_trips() {
        let kernel = kernel(2, 1);
        let a = KMeansPartial {
            sums: vec![1.0, 9.0],
            counts: vec![1, 1],
            inertia: 2.0,
        };
        let b = KMeansPartial {
            sums: vec![2.0, 18.0],
            counts: vec![2, 2],
            inertia: 3.0,
        };
        let merged = kernel.merge(a, b).unwrap();
        assert_eq!(merged.counts, vec![3, 3]);
        assert_eq!(
            kernel
                .decode_partial(&kernel.encode_partial(&merged).unwrap())
                .unwrap(),
            merged
        );
        let state = kernel.initial_state(vec![0.5, 9.5]).unwrap();
        assert_eq!(
            kernel
                .decode_state(&kernel.encode_state(&state).unwrap())
                .unwrap(),
            state
        );
    }

    #[test]
    fn invalid_matrix_and_wire_are_rejected() {
        let kernel = kernel(2, 2);
        assert_eq!(
            kernel.load(&[1.0, 2.0, 3.0], 2),
            Err(KMeansError::InvalidMatrix)
        );
        assert_eq!(kernel.decode_state(b"bad"), Err(KMeansError::InvalidWire));
    }

    #[test]
    fn feature_moments_impute_nulls_and_zero_constant_columns() {
        let moments =
            FeatureMoments::from_nullable_rows(&[1.0, f64::NAN, 3.0, 7.0, 5.0, 7.0], 2).unwrap();
        let normalized = moments
            .standardize(&[1.0, f64::NAN, 3.0, 7.0, 5.0, 7.0])
            .unwrap();
        assert!((normalized[0] + 1.224_744_9).abs() < 1e-6);
        assert_eq!(&normalized[1..4], &[0.0, 0.0, 0.0]);
        assert!((normalized[4] - 1.224_744_9).abs() < 1e-6);
        assert_eq!(normalized[5], 0.0);
        let all_null = FeatureMoments::from_nullable_rows(&[f64::NAN, f64::NAN], 1).unwrap();
        assert_eq!(
            all_null.standardize(&[f64::NAN, f64::NAN]).unwrap(),
            vec![0.0, 0.0]
        );
    }

    #[test]
    fn seeded_initialization_is_repeatable() {
        let kernel = kernel(3, 1);
        let sample = [0.0, 1.0, 10.0, 11.0];
        assert_eq!(
            kernel.seeded_centroids(&sample, 42).unwrap(),
            kernel.seeded_centroids(&sample, 42).unwrap()
        );
    }

    #[test]
    fn merge_tree_reduces_with_bounded_fan_in() {
        let kernel = kernel(1, 1);
        let partials = (1..=9)
            .map(|value| KMeansPartial {
                sums: vec![f64::from(value)],
                counts: vec![1],
                inertia: 0.0,
            })
            .collect();
        let merged = merge_tree(&kernel, partials, 3).unwrap().unwrap();
        assert_eq!(merged.sums, vec![45.0]);
        assert_eq!(merged.counts, vec![9]);
    }
}
