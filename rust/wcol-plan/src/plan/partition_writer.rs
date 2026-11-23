//! Demultiplex final rows into open partition objects.
//!
//! A partition stays open until the task finishes. A later batch can contain a
//! value that already has an object, so the writer does not close when the value
//! changes. Past `max_open_partitions` the task fails. There is no exchange.

use std::collections::BTreeMap;

use crate::plan::distributed::{
    attempt_object_key, partition_path_value, OutputObjectRecord, PartitionValue,
    PartitionWriterConfig, TaskId,
};

struct OpenPartition {
    values: Vec<String>,
    rows: u64,
    committed_bytes: u64,
    buffered: usize,
}

/// One node's open partition writers for a single attempt.
pub struct PartitionDemux {
    config: PartitionWriterConfig,
    job_id: String,
    task_id: TaskId,
    attempt: u32,
    partition_names: Vec<String>,
    open: BTreeMap<String, OpenPartition>,
}

impl PartitionDemux {
    pub fn open(
        config: PartitionWriterConfig,
        job_id: impl Into<String>,
        task_id: TaskId,
        attempt: u32,
        partition_names: Vec<String>,
    ) -> Result<Self, String> {
        config.validate()?;
        if partition_names.is_empty() || partition_names.iter().any(|name| name.is_empty()) {
            return Err("partition demux needs named output columns".into());
        }
        Ok(Self {
            config,
            job_id: job_id.into(),
            task_id,
            attempt,
            partition_names,
            open: BTreeMap::new(),
        })
    }

    /// Append one output row. `values` are the bound output columns, in `partitionBy` order.
    pub fn push_row(
        &mut self,
        values: &[PartitionValue<'_>],
        row_bytes: usize,
    ) -> Result<(), String> {
        if values.len() != self.partition_names.len() {
            return Err("partition value count does not match partitionBy".into());
        }
        let encoded = values
            .iter()
            .cloned()
            .map(partition_path_value)
            .collect::<Result<Vec<_>, _>>()?;
        let path = self
            .partition_names
            .iter()
            .zip(encoded.iter())
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("/");
        if !self.open.contains_key(&path) {
            if self.open.len() >= self.config.max_open_partitions {
                return Err(format!(
                    "partition cardinality {} exceeds max_open_partitions {}",
                    self.open.len() + 1,
                    self.config.max_open_partitions
                ));
            }
            self.open.insert(
                path.clone(),
                OpenPartition {
                    values: encoded,
                    rows: 0,
                    committed_bytes: 0,
                    buffered: 0,
                },
            );
        }
        let part_bytes = self.config.multipart_part_bytes;
        let part = self.open.get_mut(&path).unwrap();
        part.rows += 1;
        part.buffered = part.buffered.saturating_add(row_bytes);
        while part.buffered >= part_bytes {
            part.committed_bytes += part_bytes as u64;
            part.buffered -= part_bytes;
        }
        // After a full part is flushed, each open buffer is smaller than one part.
        // `validate` already requires room for one part per open partition, so this
        // fires only when that invariant is broken.
        let buffered: usize = self.open.values().map(|part| part.buffered).sum();
        if buffered > self.config.max_buffered_bytes {
            return Err(format!(
                "partition buffers hold {buffered} bytes, limit is {}",
                self.config.max_buffered_bytes
            ));
        }
        Ok(())
    }

    pub fn open_partitions(&self) -> usize {
        self.open.len()
    }

    /// One record per distinct partition value this attempt wrote.
    pub fn finish(self) -> Vec<OutputObjectRecord> {
        let PartitionDemux {
            job_id,
            task_id,
            attempt,
            open,
            ..
        } = self;
        open.into_iter()
            .map(|(path, part)| OutputObjectRecord {
                task_id: task_id.clone(),
                attempt,
                object_key: attempt_object_key(
                    &job_id,
                    &task_id,
                    attempt,
                    &format!("{path}/part.parquet"),
                ),
                rows: part.rows,
                bytes: part.committed_bytes + part.buffered as u64,
                partition_values: part.values,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demux(max_open: usize, max_buffered: usize) -> PartitionDemux {
        let mut config = PartitionWriterConfig::default();
        config.max_open_partitions = max_open;
        config.max_buffered_bytes = max_buffered.max(max_open * config.multipart_part_bytes);
        PartitionDemux::open(config, "job", TaskId(7), 1, vec!["Day".into()]).unwrap()
    }

    #[test]
    fn a_repeated_value_stays_on_the_same_object() {
        let mut writer = demux(8, 8 * 5 * 1024 * 1024);
        let first = PartitionValue::Date {
            year: 2013,
            month: 7,
            day: 1,
        };
        let second = PartitionValue::Date {
            year: 2013,
            month: 7,
            day: 2,
        };
        writer.push_row(std::slice::from_ref(&first), 10).unwrap();
        writer.push_row(&[second], 10).unwrap();
        writer.push_row(&[first], 10).unwrap();
        assert_eq!(writer.open_partitions(), 2);
        let records = writer.finish();
        assert_eq!(records.len(), 2);
        let july1 = records
            .iter()
            .find(|record| record.partition_values == ["2013-07-01"])
            .unwrap();
        assert_eq!(july1.rows, 2);
        assert!(july1.object_key.contains("/1/Day=2013-07-01/part.parquet"));
        assert_eq!(july1.attempt, 1);
    }

    #[test]
    fn cardinality_and_short_parts_fail_closed() {
        let mut writer = demux(1, 5 * 1024 * 1024);
        writer
            .push_row(
                &[PartitionValue::Date {
                    year: 2013,
                    month: 7,
                    day: 1,
                }],
                10,
            )
            .unwrap();
        let err = writer.push_row(
            &[PartitionValue::Date {
                year: 2013,
                month: 7,
                day: 2,
            }],
            10,
        );
        assert!(err.unwrap_err().contains("max_open_partitions"));
    }

    #[test]
    fn a_large_row_flushes_a_full_part_and_keeps_the_partition() {
        let config = PartitionWriterConfig {
            max_open_partitions: 1,
            ..Default::default()
        };
        let mut writer =
            PartitionDemux::open(config, "job", TaskId(7), 3, vec!["Day".into()]).unwrap();
        writer
            .push_row(&[PartitionValue::Text("a")], 5 * 1024 * 1024 + 12)
            .unwrap();
        let records = writer.finish();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].rows, 1);
        assert_eq!(records[0].bytes, 5 * 1024 * 1024 + 12);
        assert_eq!(records[0].attempt, 3);
    }
}
