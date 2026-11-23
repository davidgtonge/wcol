//! Resident sink of a planned graph. Typed batches stay in the graph's Wasm instance.

use super::{wire, FeatureMoments, IterativeKernel, KMeansConfig, KMeansKernel, KMeansResident};
use crate::stream::{OwnedWcolBatch, WcolColumn};

enum Data {
    Loading(Vec<f64>),
    Loaded(Vec<f64>),
    Normalized(KMeansResident),
}

enum Command {
    Normalize(Vec<u8>),
    Round(Vec<u8>),
    Memory,
}

pub struct ResidentExec {
    kernel: KMeansKernel,
    data: Data,
    pending: Option<Command>,
    response: Vec<u8>,
    memory_limit: usize,
    load_peak: usize,
    normalize_peak: usize,
    round_peak: usize,
}

impl ResidentExec {
    pub fn new(config: KMeansConfig) -> Result<Self, String> {
        let kernel = KMeansKernel { config };
        kernel
            .validate_config()
            .map_err(|e| format!("resident config: {e:?}"))?;
        Ok(Self {
            kernel,
            data: Data::Loading(Vec::new()),
            pending: None,
            response: Vec::new(),
            memory_limit: 40 * 1024 * 1024,
            load_peak: 0,
            normalize_peak: 0,
            round_peak: 0,
        })
    }

    pub fn loading(&self) -> bool {
        matches!(self.data, Data::Loading(_))
    }
    pub fn ready(&self) -> bool {
        !self.loading() && self.pending.is_none()
    }
    pub fn response(&self) -> &[u8] {
        &self.response
    }

    pub fn load_batch(&mut self, batch: &OwnedWcolBatch) -> Result<(), String> {
        let Data::Loading(raw) = &mut self.data else {
            return Err("resident graph already loaded".into());
        };
        let mut columns = Vec::new();
        let view = batch.view_with(&mut columns);
        if view.columns.len() != self.kernel.config.dimensions {
            return Err("resident feature count mismatch".into());
        }
        let rows = (0..view.row_count)
            .filter(|row| {
                batch
                    .selection_mask()
                    .is_none_or(|mask| mask.is_selected(*row))
            })
            .count();
        let values = rows
            .checked_mul(view.columns.len())
            .and_then(|added| raw.len().checked_add(added))
            .ok_or("resident feature size overflow")?;
        // Reserve normalization headroom while loading, before growing the raw matrix.
        let (capacity, load_peak) =
            plan_feature_capacity(raw.capacity(), values, self.memory_limit)?;
        raw.try_reserve_exact(capacity.saturating_sub(raw.len()))
            .map_err(|e| format!("resident feature allocation: {e}"))?;
        self.load_peak = self.load_peak.max(load_peak);
        for row in 0..view.row_count {
            if batch
                .selection_mask()
                .is_some_and(|mask| !mask.is_selected(row))
            {
                continue;
            }
            for column in view.columns {
                let value = match column {
                    WcolColumn::I64 { values, nulls, .. } => {
                        if nulls[row] == 0 {
                            f64::NAN
                        } else {
                            values[row] as f64
                        }
                    }
                    WcolColumn::F64 { values, nulls, .. } => {
                        if nulls[row] == 0 {
                            f64::NAN
                        } else {
                            values[row]
                        }
                    }
                    WcolColumn::DictId {
                        ids,
                        nulls,
                        dictionary,
                        ..
                    } => {
                        if nulls[row] == 0 {
                            f64::NAN
                        } else if let Some(values) = dictionary.int_entries() {
                            *values
                                .get(ids[row] as usize)
                                .ok_or("resident dictionary id out of range")?
                                as f64
                        } else if let Some(values) = dictionary.float_entries() {
                            *values
                                .get(ids[row] as usize)
                                .ok_or("resident dictionary id out of range")?
                        } else {
                            return Err("resident features must be numeric".into());
                        }
                    }
                    _ => return Err("resident features must be numeric".into()),
                };
                if value.is_infinite() {
                    return Err("resident feature is infinite".into());
                }
                raw.push(value);
            }
        }
        Ok(())
    }

    pub fn finish_load(&mut self) -> Result<(), String> {
        let Data::Loading(raw) = &self.data else {
            return Err("resident graph already loaded".into());
        };
        let dims = self.kernel.config.dimensions;
        let moments = if raw.is_empty() {
            FeatureMoments {
                counts: vec![0; dims],
                sums: vec![0.0; dims],
                sum_squares: vec![0.0; dims],
            }
        } else {
            FeatureMoments::from_nullable_rows(raw, dims)
                .map_err(|e| format!("resident moments: {e:?}"))?
        };
        self.response = wire::encode_kmeans_moments(&moments, &raw[..raw.len().min(dims * 4096)]);
        let Data::Loading(raw) = std::mem::replace(&mut self.data, Data::Loading(Vec::new()))
        else {
            unreachable!()
        };
        self.data = Data::Loaded(raw);
        Ok(())
    }

    /// Commands are queued by RPC; the existing graph pump executes them on its next step.
    pub fn command(&mut self, kind: u32, bytes: &[u8]) -> Result<(), String> {
        if kind == 0 {
            if !self.loading() || self.load_peak != 0 || bytes.len() != 8 {
                return Err("resident memory limit must be configured before loading".into());
            }
            let limit = u64::from_le_bytes(bytes.try_into().unwrap());
            let limit = usize::try_from(limit).map_err(|_| "resident memory limit out of range")?;
            if limit == 0 {
                return Err("resident memory limit must be positive".into());
            }
            self.memory_limit = limit;
            return Ok(());
        }
        if !self.ready() {
            return Err("resident graph is not ready for a command".into());
        }
        let command = match (kind, &self.data) {
            (1, Data::Loaded(_)) => Command::Normalize(bytes.to_vec()),
            (2, Data::Normalized(_)) => Command::Round(bytes.to_vec()),
            (3, _) if bytes.is_empty() => Command::Memory,
            _ => return Err("invalid resident command or phase".into()),
        };
        self.response.clear();
        self.pending = Some(command);
        Ok(())
    }

    pub fn poll_command(&mut self) -> Result<(), String> {
        let Some(command) = self.pending.take() else {
            return Ok(());
        };
        match command {
            Command::Normalize(bytes) => {
                let dims = self.kernel.config.dimensions;
                let (means, scales) = wire::decode_kmeans_normalization(&bytes, dims)?;
                let Data::Loaded(raw) = &self.data else {
                    return Err("resident data is not loaded".into());
                };
                let peak = raw
                    .capacity()
                    .checked_mul(size_of::<f64>())
                    .and_then(|bytes| bytes.checked_add(raw.len() * size_of::<f32>()))
                    .ok_or("resident feature size overflow")?;
                check_memory_limit(peak, self.memory_limit)?;
                self.normalize_peak = self.normalize_peak.max(peak);
                let values = raw
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        let column = index % dims;
                        if value.is_nan() || scales[column] == 0.0 {
                            0.0
                        } else {
                            ((value - means[column]) / scales[column]) as f32
                        }
                    })
                    .collect();
                let resident = self
                    .kernel
                    .load_owned(values, dims)
                    .map_err(|e| format!("resident normalization: {e:?}"))?;
                self.data = Data::Normalized(resident);
            }
            Command::Memory => {
                let live = match &self.data {
                    Data::Loading(raw) | Data::Loaded(raw) => raw.capacity() * size_of::<f64>(),
                    Data::Normalized(resident) => resident.values.capacity() * size_of::<f32>(),
                };
                self.response.extend_from_slice(b"WRM1");
                for value in [
                    self.memory_limit,
                    live,
                    self.load_peak,
                    self.normalize_peak,
                    self.round_peak,
                ] {
                    self.response
                        .extend_from_slice(&(value as u64).to_le_bytes());
                }
            }
            Command::Round(bytes) => {
                let Data::Normalized(resident) = &self.data else {
                    return Err("resident data is not normalized".into());
                };
                self.round_peak = self
                    .round_peak
                    .max(resident.values.capacity() * size_of::<f32>());
                let state = self
                    .kernel
                    .decode_state(&bytes)
                    .map_err(|e| format!("resident state: {e:?}"))?;
                let partial = self
                    .kernel
                    .compute(resident, &state, 0)
                    .map_err(|e| format!("resident round: {e:?}"))?;
                self.response = self
                    .kernel
                    .encode_partial(&partial)
                    .map_err(|e| format!("resident partial: {e:?}"))?;
            }
        }
        Ok(())
    }
}

fn check_memory_limit(bytes: usize, limit: usize) -> Result<(), String> {
    if bytes > limit {
        return Err(format!("wcol-fatal: resident feature memory limit exceeded: required {bytes} bytes, limit {limit} bytes"));
    }
    Ok(())
}

/// Account for both reallocating raw storage and the later normalization overlap.
fn plan_feature_capacity(
    current: usize,
    values: usize,
    limit: usize,
) -> Result<(usize, usize), String> {
    let peak = |capacity: usize| -> Option<(usize, usize)> {
        let raw = capacity.checked_mul(size_of::<f64>())?;
        let loading = if capacity > current {
            raw.checked_add(current.checked_mul(size_of::<f64>())?)?
        } else {
            raw
        };
        let normalizing = raw.checked_add(values.checked_mul(size_of::<f32>())?)?;
        Some((loading.max(normalizing), loading))
    };
    let minimum = current.max(values);
    let grown = if values > current {
        minimum.max(current.saturating_mul(2))
    } else {
        current
    };
    let capacity = match peak(grown) {
        Some((bytes, _)) if bytes <= limit => grown,
        _ => minimum,
    };
    let (required, loading) = peak(capacity).ok_or("resident feature size overflow")?;
    check_memory_limit(required, limit)?;
    Ok((capacity, loading))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_budget_accounts_for_reallocation_and_normalization() {
        assert_eq!(plan_feature_capacity(0, 100, 1200).unwrap(), (100, 800));
        assert!(plan_feature_capacity(0, 100, 1199)
            .unwrap_err()
            .contains("wcol-fatal:"));
        // Doubling is affordable, including old and new buffers during realloc.
        assert_eq!(plan_feature_capacity(100, 101, 2400).unwrap(), (200, 2400));
        // Under a tighter cap, reserve exactly the requested space.
        assert_eq!(plan_feature_capacity(100, 101, 1608).unwrap(), (101, 1608));
        assert!(plan_feature_capacity(100, 101, 1607).is_err());
        assert!(plan_feature_capacity(0, usize::MAX, usize::MAX).is_err());
    }

    #[test]
    fn memory_limit_must_precede_load_and_statistics_are_versioned() {
        let mut exec = ResidentExec::new(KMeansConfig {
            clusters: 2,
            dimensions: 2,
            max_iterations: 10,
            tolerance: 1e-4,
        })
        .unwrap();
        assert!(exec.command(0, &0u64.to_le_bytes()).is_err());
        exec.command(0, &1024u64.to_le_bytes()).unwrap();
        exec.finish_load().unwrap();
        assert!(exec.command(0, &2048u64.to_le_bytes()).is_err());
        exec.command(3, &[]).unwrap();
        exec.poll_command().unwrap();
        assert_eq!(&exec.response()[..4], b"WRM1");
        assert_eq!(exec.response().len(), 44);
        assert_eq!(
            u64::from_le_bytes(exec.response()[4..12].try_into().unwrap()),
            1024
        );
    }
}
