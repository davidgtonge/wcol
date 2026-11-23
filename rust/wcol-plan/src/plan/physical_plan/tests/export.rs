use super::*;

#[test]
fn aggregate_parquet_sink_follows_final_reduction() {
    let plan = lower_to_physical(
        &example(),
        &catalog(nearby_groups()),
        &nodes(2),
        &export_config(false),
    )
    .unwrap();
    let sink = &plan.operators[plan.root as usize];
    assert!(matches!(
        sink.kind,
        OpKind::Sink {
            kind: SinkKind::Parquet { .. }
        }
    ));
    assert!(sink.node.is_none());
    assert!(matches!(
        plan.operators[sink.inputs[0] as usize].kind,
        OpKind::FinalAggregate { .. }
    ));
    assert!(plan
        .operators
        .iter()
        .all(|op| { op.node.is_none() || !matches!(op.kind, OpKind::Sink { .. }) }));
}

#[test]
fn partition_by_rejects_a_grouped_export() {
    let err = lower_to_physical(
        &example(),
        &catalog(nearby_groups()),
        &nodes(2),
        &export_config(true),
    );
    assert!(matches!(err, Err(PlanError::Unsupported(_))));
}

#[test]
fn partition_by_sinks_on_each_scan_node() {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Filter {
            predicates: vec![Predicate::Compare(Compare {
                column: 0,
                op: CmpOp::Gt,
                value: Literal::Int(1),
            })],
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![0],
            }),
        },
    };
    let plan = lower_to_physical(
        &logical,
        &catalog(vec![chunk(0, 0, 0, 64), chunk(0, 1, 10_000, 64)]),
        &nodes(2),
        &export_config(true),
    )
    .unwrap();
    let node_sinks = plan
        .operators
        .iter()
        .filter(|op| {
            op.node.is_some()
                && matches!(
                    op.kind,
                    OpKind::Sink {
                        kind: SinkKind::Parquet { .. }
                    }
                )
        })
        .count();
    assert!(node_sinks >= 1);
    assert!(plan.operators.iter().any(|op| matches!(
        &op.kind,
        OpKind::Sink {
            kind: SinkKind::Parquet {
                partition_columns,
                ..
            }
        } if partition_columns == &[0]
    )));
    assert!(plan
        .operators
        .iter()
        .all(|op| op.node.is_some() || !matches!(op.kind, OpKind::Sink { .. })));
}
