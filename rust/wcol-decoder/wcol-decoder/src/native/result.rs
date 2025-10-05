use std::collections::BTreeMap;

use crate::constants::{
    AGG_KIND_APPROX_DISTINCT, AGG_KIND_AVG, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR, AGG_KIND_MAX,
    AGG_KIND_MIN, AGG_KIND_SUM,
};
use crate::ffi;

use super::error::NativeResult;
use super::exec::{copy_aggs_bytes, copy_groups_bytes, copy_rows_bytes};
use super::helpers::{checked_count, read_f64, read_out_bytes, read_u32, read_u64};
use super::runtime::NativeRuntime;
use super::types::{AggregateStats, GroupAggInfo, GroupKeyInfo, GroupResult, QueryResult};

impl NativeRuntime {
    pub(crate) fn read_result(&self, plan: u32) -> NativeResult<QueryResult> {
        let group_count =
            checked_count("plan_group_count", unsafe { ffi::plan_group_count(plan) })?;
        let agg_count = checked_count("plan_agg_count", unsafe { ffi::plan_agg_count(plan) })?;
        let rows = if group_count == 0 && agg_count == 0 {
            self.read_rows(plan)?
        } else {
            Vec::new()
        };
        Ok(QueryResult {
            rows,
            aggregates: self.read_aggregates(plan, agg_count)?,
            groups: self.read_groups(plan, group_count)?,
        })
    }

    fn read_rows(&self, plan: u32) -> NativeResult<Vec<u64>> {
        let bytes = copy_rows_bytes(plan)?;
        let mut out = Vec::with_capacity(bytes.len() / 8);
        for i in (0..bytes.len()).step_by(8) {
            out.push(read_u64(&bytes, i));
        }
        Ok(out)
    }

    fn read_aggregates(
        &self,
        plan: u32,
        count: usize,
    ) -> NativeResult<BTreeMap<String, AggregateStats>> {
        if count == 0 {
            return Ok(BTreeMap::new());
        }

        let bytes = copy_aggs_bytes(plan)?;
        let mut out = BTreeMap::new();
        let mut offset = 0usize;

        for _ in 0..count {
            let col_id = read_u32(&bytes, offset);
            offset += 4;
            let kind = bytes[offset];
            offset += 1;
            let agg_offset = bytes[offset] as i8;
            offset += 3;
            let sum = read_f64(&bytes, offset);
            offset += 8;
            let min = read_f64(&bytes, offset);
            offset += 8;
            let max = read_f64(&bytes, offset);
            offset += 8;
            let count_value = read_u32(&bytes, offset);
            offset += 4;

            let mean = if kind == AGG_KIND_AVG {
                if count_value == 0 {
                    0.0
                } else {
                    sum / count_value as f64
                }
            } else if kind == AGG_KIND_APPROX_DISTINCT {
                sum
            } else if kind == AGG_KIND_COUNT || kind == AGG_KIND_COUNT_STAR {
                count_value as f64
            } else if count_value == 0 {
                0.0
            } else {
                sum / count_value as f64
            };

            let col_name = self.column_name(col_id)?;
            let expr = if agg_offset == 0 {
                col_name
            } else {
                format!(
                    "{col_name} {} {}",
                    if agg_offset > 0 { "+" } else { "-" },
                    agg_offset.unsigned_abs()
                )
            };

            let name = if kind == AGG_KIND_COUNT_STAR {
                "count_star()".to_string()
            } else if kind == AGG_KIND_COUNT {
                format!("count({expr})")
            } else if kind == AGG_KIND_SUM {
                format!("sum({expr})")
            } else if kind == AGG_KIND_AVG {
                format!("avg({expr})")
            } else if kind == AGG_KIND_MIN {
                format!("min({expr})")
            } else if kind == AGG_KIND_MAX {
                format!("max({expr})")
            } else if kind == AGG_KIND_APPROX_DISTINCT {
                format!("approx_count_distinct({expr})")
            } else {
                expr
            };

            out.insert(
                name,
                AggregateStats {
                    count: count_value,
                    sum,
                    min,
                    max,
                    mean,
                },
            );
        }

        Ok(out)
    }

    fn read_groups(&self, plan: u32, count: usize) -> NativeResult<Option<GroupResult>> {
        if count == 0 {
            return Ok(None);
        }

        let key_count = checked_count("plan_group_key_count", unsafe {
            ffi::plan_group_key_count(plan)
        })?;
        let mut key_info = Vec::with_capacity(key_count);

        if key_count > 0 {
            let info_bytes = read_out_bytes(key_count * 8, |ptr, len| unsafe {
                ffi::plan_group_key_info(plan, ptr, len)
            })?;
            let mut offset = 0usize;
            for _ in 0..key_count {
                let col_id = read_u32(&info_bytes, offset);
                offset += 4;
                let physical_type = info_bytes[offset];
                offset += 1;
                let flags = info_bytes[offset];
                offset += 1;
                offset += 2;
                key_info.push(GroupKeyInfo {
                    col_id,
                    physical_type,
                    flags,
                });
            }
        }

        let agg_count = checked_count("plan_group_agg_count", unsafe {
            ffi::plan_group_agg_count(plan)
        })?;
        let mut aggs = Vec::with_capacity(agg_count);
        if agg_count > 0 {
            let agg_bytes = read_out_bytes(agg_count * 8, |ptr, len| unsafe {
                ffi::plan_copy_group_aggs(plan, ptr, len)
            })?;
            let mut offset = 0usize;
            for _ in 0..agg_count {
                let col_id = read_u32(&agg_bytes, offset);
                offset += 4;
                let kind = agg_bytes[offset];
                offset += 4;
                aggs.push(GroupAggInfo { col_id, kind });
            }
        }

        let bytes = copy_groups_bytes(plan)?;
        let mut keys = Vec::with_capacity(count);
        let mut keys2 = if key_count > 1 {
            Some(Vec::with_capacity(count))
        } else {
            None
        };
        let mut values = Vec::with_capacity(count);
        let mut offset = 0usize;

        for _ in 0..count {
            let k1 = read_u64(&bytes, offset);
            offset += 8;
            let k2 = read_u64(&bytes, offset);
            offset += 8;
            keys.push(k1);
            if let Some(out) = keys2.as_mut() {
                out.push(k2);
            }

            let mut row = Vec::with_capacity(aggs.len());
            for agg in &aggs {
                let sum = read_f64(&bytes, offset);
                offset += 8;
                let min = read_f64(&bytes, offset);
                offset += 8;
                let max = read_f64(&bytes, offset);
                offset += 8;
                let count_value = read_u32(&bytes, offset);
                offset += 4;
                offset += 4;

                let mean = if agg.kind == AGG_KIND_AVG {
                    if count_value == 0 {
                        0.0
                    } else {
                        sum / count_value as f64
                    }
                } else if agg.kind == AGG_KIND_APPROX_DISTINCT {
                    sum
                } else if agg.kind == AGG_KIND_COUNT || agg.kind == AGG_KIND_COUNT_STAR {
                    count_value as f64
                } else if count_value == 0 {
                    0.0
                } else {
                    sum / count_value as f64
                };

                row.push(AggregateStats {
                    count: count_value,
                    sum,
                    min,
                    max,
                    mean,
                });
            }

            values.push(row);
        }

        Ok(Some(GroupResult {
            keys,
            keys2,
            key_info,
            aggs,
            values,
        }))
    }

    fn column_name(&self, col_id: u32) -> NativeResult<String> {
        let bytes = read_out_bytes(64, |ptr, len| unsafe {
            ffi::runtime_column_name(self.runtime, col_id, ptr, len)
        })?;
        Ok(String::from_utf8(bytes)?)
    }
}
