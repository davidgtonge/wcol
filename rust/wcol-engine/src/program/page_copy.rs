//! Page-copy compaction shapes the engine executes. The planner fills them in.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopySegment {
    pub input_index: u16,
    pub offset: u64,
    pub length: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergedColumnPlan {
    pub leaf: u32,
    pub num_values: i64,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub segments: Vec<CopySegment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergedRowGroupPlan {
    pub num_rows: i64,
    pub columns: Vec<MergedColumnPlan>,
}
