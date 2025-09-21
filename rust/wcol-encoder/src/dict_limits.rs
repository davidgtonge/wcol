use std::collections::HashSet;
use std::sync::OnceLock;

use crate::constants::MAX_DICT_VALUES;

const LARGE_DICT_DEFAULT_MAX: usize = 1 << 20;

fn parse_usize_env(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub(crate) fn default_max_dict_values() -> usize {
    parse_usize_env("WCOL_ENCODER_MAX_DICT_VALUES", MAX_DICT_VALUES)
}

pub(crate) fn large_dict_max_values() -> usize {
    parse_usize_env("WCOL_ENCODER_LARGE_DICT_MAX", LARGE_DICT_DEFAULT_MAX)
}

fn large_dict_column_names() -> &'static HashSet<String> {
    static NAMES: OnceLock<HashSet<String>> = OnceLock::new();
    NAMES.get_or_init(|| {
        let raw = std::env::var("WCOL_ENCODER_LARGE_DICT_COLUMNS").unwrap_or_else(|_| {
            "crate_name,dep_crate_name,parent_crate_name".to_string()
        });
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    })
}

pub(crate) fn is_large_dict_column(name: &str) -> bool {
    large_dict_column_names().contains(name)
}

pub(crate) fn dict_value_limit_for_column(name: &str) -> usize {
    if is_large_dict_column(name) {
        large_dict_max_values()
    } else {
        default_max_dict_values()
    }
}
