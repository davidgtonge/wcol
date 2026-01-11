use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[cfg_attr(
    feature = "typegen",
    ts(tag = "op", rename_all = "camelCase")
)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum ViewModelPatch {
    Replace {
        path: Vec<PatchSegment>,
        #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
        value: Value,
    },
    Remove {
        path: Vec<PatchSegment>,
    },
    Insert {
        path: Vec<PatchSegment>,
        #[cfg_attr(feature = "typegen", ts(type = "unknown"))]
        value: Value,
    },
}

#[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PatchSegment {
    Index(usize),
    Key(String),
}

pub fn apply_patches(value: &Value, patches: &[ViewModelPatch]) -> Value {
    let mut next = value.clone();
    for patch in patches {
        next = apply_one(&next, patch);
    }
    next
}

fn apply_one(root: &Value, patch: &ViewModelPatch) -> Value {
    match patch {
        ViewModelPatch::Replace { path, value } if path.is_empty() => value.clone(),
        ViewModelPatch::Remove { path } if path.is_empty() => {
            panic!("cannot remove root view model")
        }
        ViewModelPatch::Insert { path, value } if path.is_empty() => value.clone(),
        ViewModelPatch::Replace { path, value }
        | ViewModelPatch::Insert { path, value } => mutate_at(root, path, 0, patch, Some(value)),
        ViewModelPatch::Remove { path } => mutate_at(root, path, 0, patch, None),
    }
}

fn mutate_at(
    node: &Value,
    path: &[PatchSegment],
    depth: usize,
    operation: &ViewModelPatch,
    insert_value: Option<&Value>,
) -> Value {
    let head = &path[depth];
    let is_last = depth + 1 == path.len();

    match node {
        Value::Array(items) => {
            let idx = segment_index(head);
            let mut arr = items.clone();
            if is_last {
                apply_leaf_array(&mut arr, idx, operation, insert_value);
                return Value::Array(arr);
            }
            let child = arr.get(idx).cloned().unwrap_or(Value::Null);
            arr[idx] = mutate_at(&child, path, depth + 1, operation, insert_value);
            Value::Array(arr)
        }
        Value::Object(map) => {
            let key = segment_key(head);
            let mut obj = map.clone();
            if is_last {
                apply_leaf_object(&mut obj, &key, operation, insert_value);
                return Value::Object(obj);
            }
            let child = obj.get(&key).cloned().unwrap_or(Value::Null);
            obj.insert(key, mutate_at(&child, path, depth + 1, operation, insert_value));
            Value::Object(obj)
        }
        _ => panic!("cannot patch path at segment {head:?}"),
    }
}

fn apply_leaf_array(
    arr: &mut Vec<Value>,
    idx: usize,
    patch: &ViewModelPatch,
    insert_value: Option<&Value>,
) {
    match patch {
        ViewModelPatch::Remove { .. } => {
            arr.remove(idx);
        }
        ViewModelPatch::Insert { .. } => {
            arr.insert(idx, insert_value.unwrap().clone());
        }
        ViewModelPatch::Replace { .. } => {
            let value = insert_value.unwrap().clone();
            if idx == arr.len() {
                arr.push(value);
            } else {
                arr[idx] = value;
            }
        }
    }
}

fn apply_leaf_object(
    obj: &mut serde_json::Map<String, Value>,
    key: &str,
    patch: &ViewModelPatch,
    insert_value: Option<&Value>,
) {
    match patch {
        ViewModelPatch::Remove { .. } => {
            obj.remove(key);
        }
        ViewModelPatch::Insert { .. } | ViewModelPatch::Replace { .. } => {
            obj.insert(key.to_string(), insert_value.unwrap().clone());
        }
    }
}

fn segment_index(segment: &PatchSegment) -> usize {
    match segment {
        PatchSegment::Index(i) => *i,
        PatchSegment::Key(s) => s.parse().expect("array segment must be numeric"),
    }
}

fn segment_key(segment: &PatchSegment) -> String {
    match segment {
        PatchSegment::Key(s) => s.clone(),
        PatchSegment::Index(i) => i.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::diff_value;
    use serde_json::json;

    #[test]
    fn apply_round_trip() {
        let prev = json!({ "score": 0, "paused": false });
        let next = json!({ "score": 10, "paused": false });
        let patches = diff_value(&prev, &next);
        let applied = apply_patches(&prev, &patches);
        assert_eq!(applied, next);
    }

    #[test]
    fn nested_replace_only() {
        let prev = json!({ "panel": { "status": "idle", "count": 0 } });
        let next = json!({ "panel": { "status": "busy", "count": 0 } });
        let patches = diff_value(&prev, &next);
        assert_eq!(
            patches,
            vec![ViewModelPatch::Replace {
                path: vec![
                    PatchSegment::Key("panel".into()),
                    PatchSegment::Key("status".into())
                ],
                value: json!("busy"),
            }]
        );
        assert_eq!(apply_patches(&prev, &patches), next);
    }
}
