use crate::patch::{apply_patches, PatchSegment, ViewModelPatch};
use serde_json::Value;

pub fn diff_value(prev: &Value, next: &Value) -> Vec<ViewModelPatch> {
    diff_at(prev, next, &[])
}

fn diff_at(prev: &Value, next: &Value, path: &[PatchSegment]) -> Vec<ViewModelPatch> {
    if prev == next {
        return Vec::new();
    }

    match (prev, next) {
        (Value::Array(a), Value::Array(b)) => diff_arrays(a, b, path),
        (Value::Object(a), Value::Object(b)) => diff_objects(a, b, path),
        _ => vec![ViewModelPatch::Replace {
            path: path.to_vec(),
            value: next.clone(),
        }],
    }
}

fn diff_arrays(a: &[Value], b: &[Value], path: &[PatchSegment]) -> Vec<ViewModelPatch> {
    if a.len() == b.len() {
        let mut patches = Vec::new();
        for (i, (left, right)) in a.iter().zip(b.iter()).enumerate() {
            patches.extend(diff_at(left, right, &path_with_index(path, i)));
        }
        if !patches.is_empty() {
            return patches;
        }
    }

    if a.len() > b.len() {
        let mut patches = Vec::new();
        for (i, (left, right)) in a.iter().zip(b.iter()).enumerate() {
            patches.extend(diff_at(left, right, &path_with_index(path, i)));
        }
        for i in (b.len()..a.len()).rev() {
            patches.push(ViewModelPatch::Remove {
                path: path_with_index(path, i),
            });
        }
        return patches;
    }

    let mut patches = Vec::new();
    for (i, (left, right)) in a.iter().zip(b.iter()).enumerate() {
        patches.extend(diff_at(left, right, &path_with_index(path, i)));
    }
    for (i, right) in b.iter().enumerate().skip(a.len()) {
        patches.push(ViewModelPatch::Replace {
            path: path_with_index(path, i),
            value: right.clone(),
        });
    }
    patches
}

fn diff_objects(
    a: &serde_json::Map<String, Value>,
    b: &serde_json::Map<String, Value>,
    path: &[PatchSegment],
) -> Vec<ViewModelPatch> {
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
    keys.sort();
    keys.dedup();

    let mut patches = Vec::new();
    for key in keys {
        let child_path = path_with_key(path, key);
        match (a.get(key), b.get(key)) {
            (None, Some(value)) => patches.push(ViewModelPatch::Replace {
                path: child_path,
                value: value.clone(),
            }),
            (Some(_), None) => patches.push(ViewModelPatch::Remove { path: child_path }),
            (Some(left), Some(right)) => patches.extend(diff_at(left, right, &child_path)),
            (None, None) => {}
        }
    }
    patches
}

fn path_with_index(path: &[PatchSegment], index: usize) -> Vec<PatchSegment> {
    let mut next = path.to_vec();
    next.push(PatchSegment::Index(index));
    next
}

fn path_with_key(path: &[PatchSegment], key: &str) -> Vec<PatchSegment> {
    let mut next = path.to_vec();
    next.push(PatchSegment::Key(key.to_string()));
    next
}

pub fn diff_serializable<T: serde::Serialize>(prev: &T, next: &T) -> Vec<ViewModelPatch> {
    let prev_value = serde_json::to_value(prev).expect("view model must serialize");
    let next_value = serde_json::to_value(next).expect("view model must serialize");
    diff_value(&prev_value, &next_value)
}

/// Validates `apply_patches` matches diff output in debug builds.
pub fn diff_serializable_checked<T: serde::Serialize>(prev: &T, next: &T) -> Vec<ViewModelPatch> {
    let prev_value = serde_json::to_value(prev).expect("view model must serialize");
    let next_value = serde_json::to_value(next).expect("view model must serialize");
    let patches = diff_serializable(prev, next);
    debug_assert_eq!(apply_patches(&prev_value, &patches), next_value);
    patches
}
