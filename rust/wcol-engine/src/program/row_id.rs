//! A row's position in a table, packed into one non-negative `i64` so filters, sorts and row
//! frames carry it as an ordinary column. A late-materialization pass reads rows back by it.

/// Where a row lives: the job source (object), its row group, and the row inside that group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowPosition {
    pub source: u16,
    pub row_group: u32,
    pub row: u64,
}

const ROW_BITS: u32 = 28;
const GROUP_BITS: u32 = 20;
const SOURCE_BITS: u32 = 15;

/// Rows a row group may hold for its rows to have ids.
pub const MAX_GROUP_ROWS: u64 = 1 << ROW_BITS;
/// Row groups an object may hold for its rows to have ids.
pub const MAX_ROW_GROUPS: u32 = 1 << GROUP_BITS;
/// Sources a table may span for its rows to have ids.
pub const MAX_SOURCES: u32 = 1 << SOURCE_BITS;

/// The id of `position`, or `None` when a part is out of range.
pub fn row_id(position: RowPosition) -> Option<i64> {
    if u32::from(position.source) >= MAX_SOURCES
        || position.row_group >= MAX_ROW_GROUPS
        || position.row >= MAX_GROUP_ROWS
    {
        return None;
    }
    let packed = (u64::from(position.source) << (GROUP_BITS + ROW_BITS))
        | (u64::from(position.row_group) << ROW_BITS)
        | position.row;
    i64::try_from(packed).ok()
}

/// The position [`row_id`] packed into `id`, or `None` for a negative id.
pub fn row_position(id: i64) -> Option<RowPosition> {
    let packed = u64::try_from(id).ok()?;
    Some(RowPosition {
        source: u16::try_from(packed >> (GROUP_BITS + ROW_BITS)).ok()?,
        row_group: ((packed >> ROW_BITS) & u64::from(MAX_ROW_GROUPS - 1)) as u32,
        row: packed & (MAX_GROUP_ROWS - 1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_and_order_by_position() {
        let positions = [
            RowPosition {
                source: 0,
                row_group: 0,
                row: 0,
            },
            RowPosition {
                source: 0,
                row_group: 0,
                row: MAX_GROUP_ROWS - 1,
            },
            RowPosition {
                source: 0,
                row_group: 225,
                row: 7,
            },
            RowPosition {
                source: (MAX_SOURCES - 1) as u16,
                row_group: MAX_ROW_GROUPS - 1,
                row: 3,
            },
        ];
        let ids: Vec<i64> = positions.iter().map(|p| row_id(*p).unwrap()).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        for (id, position) in ids.iter().zip(positions) {
            assert_eq!(row_position(*id), Some(position));
        }
    }

    #[test]
    fn out_of_range_parts_have_no_id() {
        assert_eq!(
            row_id(RowPosition {
                source: 0,
                row_group: MAX_ROW_GROUPS,
                row: 0
            }),
            None
        );
        assert_eq!(
            row_id(RowPosition {
                source: 0,
                row_group: 0,
                row: MAX_GROUP_ROWS
            }),
            None
        );
        assert_eq!(row_position(-1), None);
    }
}
