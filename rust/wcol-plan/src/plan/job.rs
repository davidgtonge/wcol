//! Day-1 coordinator job request: one source object, a sink, and attempt publication.
//!
//! SQL still chooses the rows. This module checks the request and, once the caller
//! has the query's output field names, binds `partitionBy` to those fields.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::plan::distributed::{
    task_id_hex, verify_output_manifest, verify_scan_task_coverage, OutputObjectRecord,
    ParquetOutputPolicy, ScanTaskInput, TaskId,
};
use crate::plan::physical::IngestGroup;
use crate::plan::sql_parser::{parse_statement, KMeansOptions, Statement};

/// One object. A prefix, a glob, and a list are not a source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectLocation {
    Bucket { bucket: String, key: String },
    Url(String),
}

/// One concat input. `row_groups` copies only that half-open range of its row groups,
/// unchanged; a job with ranges splits rather than compacts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConcatInput {
    pub location: ObjectLocation,
    pub row_groups: Option<std::ops::Range<u32>>,
}

/// The single table a json or parquet job reads: one object, or several with one schema
/// (`"keys": [...]` in one bucket). Order is the job's source order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedObject {
    pub table: String,
    pub locations: Vec<ObjectLocation>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JobRequest {
    CreateModel {
        idempotency_key: Option<String>,
        name: String,
        options: KMeansOptions,
        source: NamedObject,
        query_sql: String,
    },
    Json {
        idempotency_key: Option<String>,
        source: NamedObject,
        sql: String,
        row_cap: u32,
    },
    Parquet {
        idempotency_key: Option<String>,
        source: NamedObject,
        sql: Option<String>,
        bucket: String,
        prefix: String,
        partition_by: Vec<String>,
        target_file_bytes: u64,
        target_row_group_rows: u32,
    },
    Concat {
        idempotency_key: Option<String>,
        bucket: String,
        key: String,
        inputs: Vec<ConcatInput>,
    },
}

impl JobRequest {
    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            JobRequest::Json {
                idempotency_key, ..
            }
            | JobRequest::CreateModel {
                idempotency_key, ..
            }
            | JobRequest::Parquet {
                idempotency_key, ..
            }
            | JobRequest::Concat {
                idempotency_key, ..
            } => idempotency_key.as_deref(),
        }
    }

    /// Output ordinals for a parquet `partitionBy`. Empty means one file per task.
    pub fn bind_partition_by(&self, output_fields: &[String]) -> Result<Vec<u32>, String> {
        match self {
            JobRequest::Parquet { partition_by, .. } => {
                bind_partition_columns(partition_by, output_fields)
            }
            JobRequest::Json { .. }
            | JobRequest::CreateModel { .. }
            | JobRequest::Concat { .. } => {
                Err("partitionBy is only valid on a parquet sink".into())
            }
        }
    }
}

/// Parse a `POST /jobs` body. Day 1 executes one source table. Concat does not use `sources` or SQL.
pub fn parse_job_request(body: &str) -> Result<JobRequest, String> {
    let value: Value = serde_json::from_str(body).map_err(|err| format!("job json: {err}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "job body must be an object".to_string())?;
    let idempotency_key = optional_string(obj.get("idempotencyKey"), "idempotencyKey")?;
    if matches!(idempotency_key.as_deref(), Some("")) {
        return Err("idempotencyKey is empty".into());
    }
    let sql = optional_string(obj.get("sql"), "sql")?;
    let sql = sql.filter(|text| !text.trim().is_empty());
    let sources = parse_sources(obj.get("sources"))?;
    if let Some(sql_text) = sql
        .as_deref()
        .filter(|text| text.trim_start().to_ascii_uppercase().starts_with("CREATE"))
    {
        if let Statement::CreateModel {
            name,
            options,
            query,
            query_sql,
        } = parse_statement(sql_text)
            .map_err(|err| format!("sql parse error at {}: {}", err.at, err.msg))?
        {
            if obj.get("output").is_some() {
                return Err("CREATE MODEL does not take an output sink".into());
            }
            let source = one_source(sources)?;
            if query.from.0 != source.table {
                return Err(format!("FROM {} is not a table in sources", query.from.0));
            }
            if name.contains('/') || name.contains('\\') || name == "." || name == ".." {
                return Err("invalid model name".into());
            }
            return Ok(JobRequest::CreateModel {
                idempotency_key,
                name,
                options,
                source,
                query_sql: query_sql.trim().to_string(),
            });
        }
    }
    let output = obj
        .get("output")
        .ok_or_else(|| "output is required".to_string())?;
    let format = output
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| "output.format is required".to_string())?;
    match format {
        "json" => parse_json(idempotency_key, sql, sources, output),
        "parquet" => parse_parquet(idempotency_key, sql, sources, output),
        "concat" => parse_concat(idempotency_key, sql, sources, obj, output),
        other => Err(format!("unknown output format {other}")),
    }
}

/// Bind `partitionBy` names to the final output schema. Names are exact, including aliases.
pub fn bind_partition_columns(
    requested: &[String],
    output_fields: &[String],
) -> Result<Vec<u32>, String> {
    let mut indexes = Vec::with_capacity(requested.len());
    for name in requested {
        if name.is_empty() {
            return Err("partitionBy name is empty".into());
        }
        if indexes.iter().any(|index| {
            output_fields
                .get(*index as usize)
                .is_some_and(|field| field == name)
        }) {
            return Err(format!("partitionBy lists {name} twice"));
        }
        let mut found = output_fields
            .iter()
            .enumerate()
            .filter(|(_, field)| *field == name);
        let Some((index, _)) = found.next() else {
            return Err(format!(
                "partitionBy {name} is not an output column ({})",
                output_fields.join(", ")
            ));
        };
        if found.next().is_some() {
            return Err(format!("output column {name} is ambiguous"));
        }
        indexes.push(u32::try_from(index).map_err(|_| "output column index does not fit")?);
    }
    Ok(indexes)
}

/// `<prefix>_manifest.json`. A prefix of `hits/` publishes `hits/_manifest.json`.
pub fn manifest_object_key(prefix: &str) -> String {
    if prefix.ends_with('/') {
        format!("{prefix}_manifest.json")
    } else {
        format!("{prefix}/_manifest.json")
    }
}

/// First job id for a client key. A missing key always starts a new job.
#[derive(Clone, Debug, Default)]
pub struct IdempotencyTable {
    jobs: HashMap<String, String>,
}

impl IdempotencyTable {
    pub fn start(&mut self, key: Option<&str>, job_id: impl Into<String>) -> String {
        let job_id = job_id.into();
        let Some(key) = key else {
            return job_id;
        };
        self.jobs.entry(key.to_string()).or_insert(job_id).clone()
    }
}

/// Objects one successful attempt wrote. A later attempt of the same task replaces it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttemptReport {
    pub task_id: TaskId,
    pub attempt: u32,
    pub objects: Vec<OutputObjectRecord>,
}

/// Accepted attempts. Publication keeps only the current successful attempt of each planned task.
#[derive(Clone, Debug, Default)]
pub struct AttemptLog {
    current: HashMap<TaskId, AttemptReport>,
}

impl AttemptLog {
    pub fn accept(&mut self, report: AttemptReport) -> Result<(), String> {
        if report.objects.is_empty() {
            return Err("attempt published no objects".into());
        }
        for object in &report.objects {
            if object.task_id != report.task_id || object.attempt != report.attempt {
                return Err("output record does not match the attempt".into());
            }
            if object.object_key.is_empty() {
                return Err("output object key is empty".into());
            }
        }
        if let Some(existing) = self.current.get(&report.task_id) {
            if existing.attempt > report.attempt {
                return Err(format!(
                    "stale attempt {} for task {}",
                    report.attempt,
                    task_id_hex(&report.task_id)
                ));
            }
            if existing.attempt == report.attempt {
                if existing.objects == report.objects {
                    return Ok(());
                }
                return Err(format!(
                    "attempt {} for task {} reported different objects",
                    report.attempt,
                    task_id_hex(&report.task_id)
                ));
            }
        }
        self.current.insert(report.task_id.clone(), report);
        Ok(())
    }

    /// The commit set. Every planned scan task has one accepted attempt, and the
    /// output records pass [`verify_output_manifest`].
    pub fn publish(
        &self,
        groups: &[IngestGroup],
        tasks: &[ScanTaskInput],
    ) -> Result<Vec<OutputObjectRecord>, String> {
        verify_scan_task_coverage(groups, tasks)?;
        let mut records = Vec::new();
        for task in tasks {
            let report = self.current.get(&task.task_id).ok_or_else(|| {
                format!(
                    "task {} has no successful attempt",
                    task_id_hex(&task.task_id)
                )
            })?;
            records.extend(report.objects.clone());
        }
        if self.current.len() != tasks.len() {
            return Err("attempt log has a task that was not planned".into());
        }
        verify_output_manifest(&records)?;
        Ok(records)
    }
}

fn parse_json(
    idempotency_key: Option<String>,
    sql: Option<String>,
    sources: BTreeMap<String, Vec<ObjectLocation>>,
    output: &Value,
) -> Result<JobRequest, String> {
    if output.get("partitionBy").is_some() {
        return Err("partitionBy is only valid on a parquet sink".into());
    }
    let sql = sql.ok_or_else(|| "json output requires sql".to_string())?;
    let source = one_source(sources)?;
    let row_cap = match output.get("rowCap") {
        None => 1000,
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| "rowCap must be a positive integer".to_string())?,
        Some(_) => return Err("rowCap must be a positive integer".into()),
    };
    Ok(JobRequest::Json {
        idempotency_key,
        source,
        sql,
        row_cap,
    })
}

fn parse_parquet(
    idempotency_key: Option<String>,
    sql: Option<String>,
    sources: BTreeMap<String, Vec<ObjectLocation>>,
    output: &Value,
) -> Result<JobRequest, String> {
    let source = one_source(sources)?;
    let bucket = required_string(output, "bucket")?;
    let prefix = required_string(output, "prefix")?;
    if prefix.is_empty() {
        return Err("output.prefix is empty".into());
    }
    let partition_by = match output.get("partitionBy") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| "partitionBy entries must be strings".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("partitionBy must be an array of column names".into()),
    };
    let defaults = ParquetOutputPolicy::default();
    let target_file_bytes = optional_u64(output.get("targetFileBytes"), "targetFileBytes")?
        .unwrap_or(defaults.target_file_bytes);
    let target_row_group_rows =
        optional_u64(output.get("targetRowGroupRows"), "targetRowGroupRows")?
            .unwrap_or(defaults.target_row_group_rows as u64);
    if target_file_bytes == 0 || target_row_group_rows == 0 {
        return Err("parquet sizes must be positive".into());
    }
    let target_row_group_rows = u32::try_from(target_row_group_rows)
        .map_err(|_| "targetRowGroupRows does not fit".to_string())?;
    Ok(JobRequest::Parquet {
        idempotency_key,
        source,
        sql,
        bucket,
        prefix,
        partition_by,
        target_file_bytes,
        target_row_group_rows,
    })
}

fn parse_concat(
    idempotency_key: Option<String>,
    sql: Option<String>,
    sources: BTreeMap<String, Vec<ObjectLocation>>,
    body: &serde_json::Map<String, Value>,
    output: &Value,
) -> Result<JobRequest, String> {
    if sql.is_some() {
        return Err("concat does not take sql".into());
    }
    if !sources.is_empty() {
        return Err("concat does not take sources".into());
    }
    if output.get("partitionBy").is_some() {
        return Err("concat does not take partitionBy".into());
    }
    let bucket = required_string(output, "bucket")?;
    let key = required_string(output, "key")?;
    let location = parse_object(
        &Value::Object(serde_json::Map::from_iter([
            ("bucket".to_string(), Value::String(bucket.clone())),
            ("key".to_string(), Value::String(key)),
        ])),
        "output",
    )?;
    let ObjectLocation::Bucket { bucket, key } = location else {
        return Err("concat output needs a bucket key".into());
    };
    let inputs = body
        .get("inputs")
        .and_then(Value::as_array)
        .ok_or_else(|| "concat requires inputs".to_string())?;
    if inputs.is_empty() {
        return Err("concat requires at least one input".into());
    }
    let inputs = inputs
        .iter()
        .map(|input| {
            Ok(ConcatInput {
                location: parse_object(input, "inputs")?,
                row_groups: parse_row_groups(input)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(JobRequest::Concat {
        idempotency_key,
        bucket,
        key,
        inputs,
    })
}

fn parse_sources(value: Option<&Value>) -> Result<BTreeMap<String, Vec<ObjectLocation>>, String> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let obj = value
        .as_object()
        .ok_or_else(|| "sources must be an object of table names".to_string())?;
    let mut sources = BTreeMap::new();
    for (table, location) in obj {
        if table.is_empty() {
            return Err("source table name is empty".into());
        }
        sources.insert(
            table.clone(),
            parse_table_objects(location, &format!("sources.{table}"))?,
        );
    }
    Ok(sources)
}

fn one_source(sources: BTreeMap<String, Vec<ObjectLocation>>) -> Result<NamedObject, String> {
    if sources.len() != 1 {
        return Err("day 1 runs one source table".into());
    }
    let (table, locations) = sources.into_iter().next().unwrap();
    Ok(NamedObject { table, locations })
}

/// `{bucket, key}`, `{key: url}`, or `{bucket, keys: [...]}`: a table's objects, in order.
fn parse_table_objects(value: &Value, what: &str) -> Result<Vec<ObjectLocation>, String> {
    let Some(keys) = value.get("keys") else {
        return Ok(vec![parse_object(value, what)?]);
    };
    if value.get("key").is_some() {
        return Err(format!("{what} takes key or keys, not both"));
    }
    let bucket = value
        .get("bucket")
        .ok_or_else(|| format!("{what}.keys needs a bucket"))?;
    let keys = keys
        .as_array()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| format!("{what}.keys must be a non-empty list"))?;
    let mut locations = Vec::with_capacity(keys.len());
    for key in keys {
        let object = serde_json::json!({ "bucket": bucket, "key": key });
        let location = parse_object(&object, &format!("{what}.keys"))?;
        if locations.contains(&location) {
            return Err(format!("{what}.keys lists {key} twice"));
        }
        locations.push(location);
    }
    Ok(locations)
}

/// `"rowGroups": [start, end]`, half-open and non-empty.
fn parse_row_groups(input: &Value) -> Result<Option<std::ops::Range<u32>>, String> {
    let Some(value) = input.get("rowGroups") else {
        return Ok(None);
    };
    let bound = |index: usize| {
        value
            .get(index)
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| "rowGroups must be [start, end]".to_string())
    };
    if value.as_array().map(Vec::len) != Some(2) {
        return Err("rowGroups must be [start, end]".into());
    }
    let (start, end) = (bound(0)?, bound(1)?);
    if start >= end {
        return Err("rowGroups must be a non-empty [start, end) range".into());
    }
    Ok(Some(start..end))
}

fn parse_object(value: &Value, what: &str) -> Result<ObjectLocation, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| format!("{what} must be an object"))?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{what}.key is required"))?;
    if key.is_empty() || key.contains('*') || key.ends_with('/') {
        return Err(format!(
            "{what}.key must be one object, not a prefix or glob"
        ));
    }
    match obj.get("bucket").and_then(Value::as_str) {
        None => {
            if !(key.starts_with("https://") || key.starts_with("http://")) {
                return Err(format!("{what} needs a bucket or an http(s) url"));
            }
            Ok(ObjectLocation::Url(key.to_string()))
        }
        Some("") => Err(format!("{what}.bucket is empty")),
        Some(bucket) => Ok(ObjectLocation::Bucket {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }),
    }
}

fn optional_string(value: Option<&Value>, name: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(format!("{name} must be a string")),
    }
}

fn required_string(value: &Value, name: &str) -> Result<String, String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() || name == "prefix")
        .map(str::to_string)
        .ok_or_else(|| format!("output.{name} is required"))
}

fn optional_u64(value: Option<&Value>, name: &str) -> Result<Option<u64>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("{name} must be a positive integer")),
        Some(_) => Err(format!("{name} must be a positive integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hits() -> &'static str {
        r#"{
          "idempotencyKey": "client-token",
          "sources": { "hits": { "bucket": "in", "key": "hits.parquet" } },
          "sql": "SELECT CAST(EventTime AS DATE) AS Day, UserID FROM hits",
          "output": {
            "format": "parquet",
            "bucket": "out",
            "prefix": "hits/",
            "partitionBy": ["Day"]
          }
        }"#
    }

    #[test]
    fn parquet_request_binds_the_output_alias() {
        let job = parse_job_request(hits()).unwrap();
        let fields = vec!["Day".into(), "UserID".into()];
        assert_eq!(job.bind_partition_by(&fields).unwrap(), vec![0]);
        let err = job.bind_partition_by(&["UserID".into()]).unwrap_err();
        assert!(err.contains("not an output column"), "{err}");
        assert_eq!(manifest_object_key("hits/"), "hits/_manifest.json");
        assert_eq!(job.idempotency_key(), Some("client-token"));
    }

    #[test]
    fn request_shape_rejections() {
        assert!(parse_job_request(r#"{"sources":{"a":{"bucket":"b","key":"a"},"c":{"bucket":"b","key":"c"}},"sql":"select 1","output":{"format":"json"}}"#).is_err());
        assert!(parse_job_request(r#"{"sources":{"hits":{"bucket":"in","key":"hits/*"}},"output":{"format":"parquet","bucket":"out","prefix":"hits/"}}"#).is_err());
        assert!(parse_job_request(r#"{"sql":"select 1","output":{"format":"concat","bucket":"out","key":"merged.parquet"},"inputs":[{"bucket":"out","key":"a.parquet"}]}"#).is_err());
        let concat = parse_job_request(r#"{"output":{"format":"concat","bucket":"out","key":"hits/merged.parquet"},"inputs":[{"bucket":"out","key":"a.parquet"},{"key":"https://example.com/b.parquet"}]}"#).unwrap();
        assert!(matches!(concat, JobRequest::Concat { inputs, .. } if inputs.len() == 2));
    }

    #[test]
    fn create_model_is_a_sinkless_job_request() {
        let body = r#"{
          "idempotencyKey":"train-1",
          "sources":{"customers":{"bucket":"input","key":"customers.parquet"}},
          "sql":"CREATE MODEL customer_segments OPTIONS (model_type='kmeans', num_clusters=12) AS SELECT income, spending FROM customers WHERE income > 0"
        }"#;
        let JobRequest::CreateModel {
            name,
            options,
            source,
            query_sql,
            ..
        } = parse_job_request(body).unwrap()
        else {
            panic!("expected create model request");
        };
        assert_eq!(name, "customer_segments");
        assert_eq!(options.clusters, 12);
        assert_eq!(source.table, "customers");
        assert!(query_sql.starts_with("SELECT"));
        let bad = body.replace("FROM customers", "FROM missing");
        assert!(parse_job_request(&bad)
            .unwrap_err()
            .contains("not a table in sources"));
    }

    #[test]
    fn a_table_source_may_list_its_objects() {
        let body = |source: &str| {
            format!(
                r#"{{"sources":{{"hits":{source}}},"sql":"select count(*) from hits","output":{{"format":"json"}}}}"#
            )
        };
        let JobRequest::Json { source, .. } = parse_job_request(&body(
            r#"{"bucket":"b","keys":["p/0.parquet","p/1.parquet"]}"#,
        ))
        .unwrap() else {
            panic!("not json");
        };
        assert_eq!(
            source.locations,
            vec![
                ObjectLocation::Bucket {
                    bucket: "b".into(),
                    key: "p/0.parquet".into()
                },
                ObjectLocation::Bucket {
                    bucket: "b".into(),
                    key: "p/1.parquet".into()
                },
            ]
        );
        for bad in [
            r#"{"bucket":"b","keys":[]}"#,
            r#"{"keys":["p/0.parquet"]}"#,
            r#"{"bucket":"b","key":"a","keys":["c"]}"#,
            r#"{"bucket":"b","keys":["p/0.parquet","p/0.parquet"]}"#,
            r#"{"bucket":"b","keys":["p/*"]}"#,
        ] {
            assert!(parse_job_request(&body(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn concat_inputs_take_a_row_group_range() {
        let body = |groups: &str| {
            format!(
                r#"{{"output":{{"format":"concat","bucket":"out","key":"p.parquet"}},"inputs":[{{"bucket":"out","key":"a.parquet","rowGroups":{groups}}}]}}"#
            )
        };
        let JobRequest::Concat { inputs, .. } = parse_job_request(&body("[3, 7]")).unwrap() else {
            panic!("not a concat");
        };
        assert_eq!(inputs[0].row_groups, Some(3..7));
        for bad in ["[7, 3]", "[3, 3]", "[3]", "\"3-7\"", "[-1, 2]"] {
            assert!(parse_job_request(&body(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_later_attempt_replaces_the_published_objects() {
        let groups = vec![IngestGroup {
            row_group: 42,
            rows: 10,
            chunks: Vec::new(),
        }];
        let tasks = vec![ScanTaskInput {
            task_id: TaskId(7),
            source_row_groups: vec![42],
        }];
        let mut log = AttemptLog::default();
        log.accept(report(1, "old")).unwrap();
        log.accept(report(2, "new")).unwrap();
        assert!(log.accept(report(1, "late")).is_err());
        let published = log.publish(&groups, &tasks).unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].attempt, 2);
        assert!(published[0].object_key.contains("new"));
    }

    fn report(attempt: u32, name: &str) -> AttemptReport {
        let task_id = TaskId(7);
        AttemptReport {
            task_id: task_id.clone(),
            attempt,
            objects: vec![OutputObjectRecord {
                task_id,
                attempt,
                object_key: format!("_jobs/job/{name}.parquet"),
                rows: 3,
                bytes: 10,
                partition_values: vec!["2013-07-01".into()],
            }],
        }
    }

    #[test]
    fn the_same_idempotency_key_returns_the_first_job() {
        let mut table = IdempotencyTable::default();
        assert_eq!(table.start(Some("client-token"), "job-1"), "job-1");
        assert_eq!(table.start(Some("client-token"), "job-2"), "job-1");
        assert_eq!(table.start(None, "job-3"), "job-3");
    }
}
