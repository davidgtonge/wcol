//! `wcs_open` only accepts a v2 program. Query execution is a node or coordinator fragment.

use wcol::open_wcol_plan;

#[test]
fn wcs_open_rejects_v1_byte_plan() {
    assert!(open_wcol_plan(&[0], 256).is_err());
}
