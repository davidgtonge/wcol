//! Physical exchange partitioning (placement-agnostic; host wires WCS1 edges).

/// How rows are routed across logical output partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExchangePartitioning {
    Single,
    Broadcast,
    Hash {
        keys: Vec<u32>,
        partitions: u32,
        hash_version: u32,
    },
}

/// Hash shuffle parameters for [`super::HashPartitionOp`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashPartitionPlan {
    pub keys: Vec<u32>,
    pub partitions: u32,
    pub hash_version: u32,
}

impl HashPartitionPlan {
    pub fn new(keys: Vec<u32>, partitions: u32, hash_version: u32) -> Result<Self, String> {
        if keys.is_empty() {
            return Err("hash partition requires at least one key column".to_string());
        }
        if partitions == 0 {
            return Err("hash partition requires partitions > 0".to_string());
        }
        Ok(Self {
            keys,
            partitions,
            hash_version,
        })
    }
}

impl From<HashPartitionPlan> for ExchangePartitioning {
    fn from(plan: HashPartitionPlan) -> Self {
        ExchangePartitioning::Hash {
            keys: plan.keys,
            partitions: plan.partitions,
            hash_version: plan.hash_version,
        }
    }
}

/// Hash or range fan-out from a node fragment step.
#[derive(Clone, Debug, PartialEq)]
pub enum BatchPartitionPlan {
    Hash(HashPartitionPlan),
    Range(super::range_partition::RangePartitionPlan),
}

impl BatchPartitionPlan {
    pub fn partitions(&self) -> u32 {
        match self {
            Self::Hash(plan) => plan.partitions,
            Self::Range(plan) => plan.partitions,
        }
    }
}
