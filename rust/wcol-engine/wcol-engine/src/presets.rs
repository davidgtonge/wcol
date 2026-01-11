use crate::state::DatasetKind;

#[derive(Debug, Clone, Copy)]
pub struct PresetMeta {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
}

pub const CRATES_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "topCrates",
        label: "Most downloaded crates",
        description: "Which crates have the highest total downloads across all versions?",
    },
    PresetMeta {
        id: "byLicense",
        label: "Crates by license",
        description: "How are downloads distributed across SPDX licenses?",
    },
    PresetMeta {
        id: "mitLicense",
        label: "Popular MIT crates",
        description: "Top crates published under the MIT license",
    },
    PresetMeta {
        id: "yankedCrates",
        label: "Crates with yanked versions",
        description: "Which crates have the most yanked versions in the index?",
    },
    PresetMeta {
        id: "editionYanked",
        label: "Edition × yanked",
        description: "How do Rust editions and yanked flags affect download totals?",
    },
    PresetMeta {
        id: "select",
        label: "Mega-download versions",
        description: "Browse individual versions with more than 10M downloads",
    },
    PresetMeta {
        id: "filter",
        label: "High-download versions",
        description: "Find versions with unusually high download counts (>1M)",
    },
];

pub const DEPS_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "topDependencies",
        label: "Most depended-on crates",
        description: "Which crates appear most often as dependencies?",
    },
    PresetMeta {
        id: "dependsOnSerde",
        label: "Crates that depend on serde",
        description: "Rank parent crates by how many version edges depend on serde",
    },
    PresetMeta {
        id: "tokioDependents",
        label: "Who depends on tokio?",
        description: "Parent crates with a dependency edge to tokio",
    },
    PresetMeta {
        id: "optionalDeps",
        label: "Optional dependencies",
        description: "Browse optional dependency edges",
    },
    PresetMeta {
        id: "browseEdges",
        label: "Browse dependency edges",
        description: "Sample parent → dependency pairs",
    },
];

pub const CATEGORIES_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "topCategories",
        label: "Top categories by downloads",
        description: "Which crates.io categories drive the most download totals?",
    },
    PresetMeta {
        id: "webProgramming",
        label: "Web programming crates",
        description: "Most downloaded crates in the Web programming category",
    },
    PresetMeta {
        id: "browseCategories",
        label: "Browse crate categories",
        description: "Sample crate × category rows with download totals",
    },
];

pub const MAINTAINERS_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "topMaintainers",
        label: "Most prolific maintainers",
        description: "Which owners maintain the most crates?",
    },
    PresetMeta {
        id: "dtolnayCrates",
        label: "Crates by dtolnay",
        description: "Crates owned or maintained by dtolnay",
    },
    PresetMeta {
        id: "browseMaintainers",
        label: "Browse maintainer roster",
        description: "Sample crate × owner rows with download totals",
    },
];

pub const TRENDS_CRATE_30D_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "fastestGrowing",
        label: "Fastest-growing crates",
        description: "Top crates by total downloads in the last 30 days (pre-aggregated rollup)",
    },
];

pub const TRENDS_SERDE_VERSIONS_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "serdeVersionAdoption",
        label: "Serde version adoption",
        description: "Total downloads per serde version (pre-aggregated rollup)",
    },
];

pub const TRENDS_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "fastestGrowing",
        label: "Fastest-growing crates",
        description: "Crates with the most downloads in the last 30 days of daily stats",
    },
    PresetMeta {
        id: "serdeVersionAdoption",
        label: "Serde version adoption",
        description: "Which serde versions still receive daily downloads?",
    },
    PresetMeta {
        id: "browseTrends",
        label: "Browse daily download rows",
        description: "Sample version × date download facts from the trends table",
    },
];

pub const HITS_PRESETS: &[PresetMeta] = &[
    PresetMeta {
        id: "filter",
        label: "Filter + preview",
        description: "Point lookup on CounterID",
    },
    PresetMeta {
        id: "select",
        label: "SELECT columns",
        description: "Late column materialization after filter",
    },
    PresetMeta {
        id: "group1",
        label: "Group by CounterID",
        description: "Group-by aggregation on ClickBench hits",
    },
];

pub fn presets_for_kind(kind: Option<DatasetKind>) -> &'static [PresetMeta] {
    match kind {
        Some(DatasetKind::Hits) => HITS_PRESETS,
        Some(DatasetKind::Dependencies) => DEPS_PRESETS,
        Some(DatasetKind::Categories) => CATEGORIES_PRESETS,
        Some(DatasetKind::Maintainers) => MAINTAINERS_PRESETS,
        Some(DatasetKind::Trends) => TRENDS_PRESETS,
        _ => CRATES_PRESETS,
    }
}

pub fn presets_for_dataset(
    kind: Option<DatasetKind>,
    dataset_id: Option<&str>,
) -> &'static [PresetMeta] {
    match dataset_id {
        Some("trends-crate-30d") => TRENDS_CRATE_30D_PRESETS,
        Some("trends-serde-versions") => TRENDS_SERDE_VERSIONS_PRESETS,
        _ => presets_for_kind(kind),
    }
}

pub fn default_preset_id(kind: Option<DatasetKind>, dataset_id: Option<&str>) -> &'static str {
    match dataset_id {
        Some("trends-serde-versions") => "serdeVersionAdoption",
        Some("trends-crate-30d") => "fastestGrowing",
        _ => match kind {
            Some(DatasetKind::Hits) => "filter",
            Some(DatasetKind::Dependencies) => "topDependencies",
            Some(DatasetKind::Categories) => "topCategories",
            Some(DatasetKind::Maintainers) => "topMaintainers",
            Some(DatasetKind::Trends) => "fastestGrowing",
            _ => "topCrates",
        },
    }
}

pub fn preset_exists(
    kind: Option<DatasetKind>,
    dataset_id: Option<&str>,
    id: &str,
) -> bool {
    presets_for_dataset(kind, dataset_id)
        .iter()
        .any(|p| p.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::DatasetKind;

    #[test]
    fn rollup_dataset_defaults() {
        assert_eq!(
            default_preset_id(Some(DatasetKind::Trends), Some("trends-crate-30d")),
            "fastestGrowing"
        );
        assert_eq!(
            default_preset_id(Some(DatasetKind::Trends), Some("trends-serde-versions")),
            "serdeVersionAdoption"
        );
        assert_eq!(presets_for_dataset(Some(DatasetKind::Trends), Some("trends-crate-30d")).len(), 1);
    }
}
