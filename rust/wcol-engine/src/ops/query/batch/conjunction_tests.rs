use super::*;
use crate::ir::{CmpOp, ColumnKind, Predicate, Scalar};
use crate::stream::OwnedWcolBatch;

fn text_col(name: &str) -> CatalogColumn {
    CatalogColumn {
        name: name.into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }
}

fn utf8(values: &[&str]) -> (Vec<u32>, Vec<u8>, Vec<u8>) {
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend(value.as_bytes());
        offsets.push(bytes.len() as u32);
    }
    (offsets, bytes, vec![1; values.len()])
}

#[test]
fn conjunction_keeps_rows_that_survive_the_cheaper_compare() {
    let catalog = vec![text_col("Title"), text_col("URL"), text_col("SearchPhrase")];
    let mut filter = BatchFilter::from_predicates(
        &[
            Predicate::Like {
                column: 0,
                pattern: "%Google%".into(),
                negated: false,
            },
            Predicate::Like {
                column: 1,
                pattern: "%.google.%".into(),
                negated: true,
            },
            Predicate::Compare {
                column: 2,
                op: CmpOp::Ne,
                value: Scalar::Text(String::new()),
            },
        ],
        &catalog,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        filter.filters[0],
        filter::ColumnFilter::Text {
            pred: string_filter::TextPred::Empty(false),
            ..
        }
    ));
    let (title_off, title_bytes, title_nulls) =
        utf8(&["Google", "Google", "Other", "Google", "Google"]);
    let (url_off, url_bytes, url_nulls) = utf8(&[
        "http://example.com",
        "http://www.google.com/a",
        "http://example.com",
        "http://example.com",
        "http://example.com",
    ]);
    let (phrase_off, phrase_bytes, phrase_nulls) = utf8(&["hi", "hi", "hi", "", "hi"]);
    let mut batch = OwnedWcolBatch::empty();
    batch.push_utf8_column(0, title_off, title_bytes, title_nulls);
    batch.push_utf8_column(1, url_off, url_bytes, url_nulls);
    batch.push_utf8_column(2, phrase_off, phrase_bytes, phrase_nulls);
    batch.set_row_count(5).unwrap();
    let mut columns = Vec::new();
    let view = batch.view_with(&mut columns);
    let selected = filter.selection(&view).unwrap();
    let mut hits = Vec::new();
    mask::for_each_set(selection_words(&selected).as_ref(), |row| hits.push(row));
    assert_eq!(hits, vec![0, 4]);
}
