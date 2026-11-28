//! Open a [`GraphHost`] for each fragment kind.

use super::*;

impl GraphHost {
    pub fn open_page_compact(cur: &mut PlanCursor<'_>, capacity: usize) -> WcolResult<Self> {
        let graph = crate::formats::parquet::page_compact::open_page_compact_graph(cur)?;
        Self::new(graph, GraphHostIngress::HostParquet, capacity)
    }

    /// [`OP_NODE_FRAGMENT`](crate::program::stream_program::OP_NODE_FRAGMENT): run one node of a
    /// physical plan. Reads are the plan's fetches; operators come from the fragment, not SQL.
    pub fn open_node_fragment(cur: &mut PlanCursor<'_>, capacity: usize) -> WcolResult<Self> {
        ingest_replay_reset();
        crate::exec::dict_traffic::reset();
        let batch_rows = cur.u32()? as usize;
        if batch_rows == 0 {
            return Err(WcolError::from("batch row limit is zero"));
        }
        let catalog = cur.catalog()?;
        let len = cur.u32()? as usize;
        let fragment = crate::program::decode_node_fragment(cur.bytes(len)?)
            .map_err(|e| WcolError::msg(format!("node fragment: {e}")))?;
        if !cur.exhausted() {
            return Err(WcolError::from("trailing bytes after node fragment"));
        }
        let graph = crate::exec::colocated_plan::open_fragment_graph(
            batch_rows,
            catalog,
            &fragment,
            crate::program::IngestMemoryProfile::default(),
        )
        .map_err(|e| WcolError::msg(format!("fragment graph open failed: {e}")))?;
        let mut host = Self::new(graph, GraphHostIngress::HostParquet, capacity)?;
        if let Some(crate::program::FragmentStep::Resident(config)) = fragment.steps.last() {
            host.core.resident = Some(
                crate::ops::iterative::resident::ResidentExec::new(config.clone())
                    .map_err(WcolError::msg)?,
            );
            host.core.row_egress = None;
        }
        if let Some(plan) = fanout_plan_of(&fragment)? {
            let partitions = plan.partitions() as usize;
            host.core.row_egress = None;
            host.core.hash_fanout = Some(HashFanout {
                plan,
                egress: (0..partitions).map(|_| RowBatchWcsEgress::new()).collect(),
                cursor: 0,
                ends_queued: false,
                opened: vec![false; partitions],
            });
        }
        Ok(host)
    }

    pub fn open_coordinator_fragment(
        cur: &mut PlanCursor<'_>,
        capacity: usize,
    ) -> WcolResult<Self> {
        let node_count = cur.u32()?;
        if node_count == 0 {
            return Err(WcolError::from("coordinator fragment has no node inputs"));
        }
        let len = cur.u32()? as usize;
        let steps = crate::program::decode_coordinator_steps(cur.bytes(len)?)
            .map_err(|e| WcolError::msg(format!("coordinator fragment: {e:?}")))?;
        if !cur.exhausted() {
            return Err(WcolError::from("trailing bytes after coordinator fragment"));
        }
        let graph = crate::exec::colocated_plan::open_coordinator_graph(&steps)
            .map_err(|e| WcolError::msg(format!("coordinator graph open failed: {e}")))?;
        Self::new(graph, wcs_row_batches(node_count), capacity)
    }

    pub fn open_wcs_partial_reduce(cur: &mut PlanCursor<'_>, capacity: usize) -> WcolResult<Self> {
        let producer_count = cur.u32()?;
        if producer_count == 0 {
            return Err(WcolError::from(
                "partial reduce requires at least one producer",
            ));
        }
        if !cur.exhausted() {
            return Err(WcolError::from(
                "trailing bytes after partial reduce header",
            ));
        }
        let graph = LinearColocatedGraph::from_plan_parts(None, vec![], vec![], false);
        let mut host = Self::new(graph, wcs_partial_reduce(producer_count), capacity)?;
        host.core.reduce = Some(ReduceOp::new());
        Ok(host)
    }

    /// Finalizer: reduce `producer_count` partial streams, then run the coordinator steps
    /// over the finalized groups.
    pub fn open_finalize_fragment(cur: &mut PlanCursor<'_>, capacity: usize) -> WcolResult<Self> {
        let producer_count = cur.u32()?;
        if producer_count == 0 {
            return Err(WcolError::from("finalizer requires at least one producer"));
        }
        let len = cur.u32()? as usize;
        let steps = crate::program::decode_coordinator_steps(cur.bytes(len)?)
            .map_err(|e| WcolError::msg(format!("finalize fragment: {e:?}")))?;
        if !cur.exhausted() {
            return Err(WcolError::from("trailing bytes after finalize fragment"));
        }
        let graph = crate::exec::colocated_plan::open_coordinator_graph(&steps)
            .map_err(|e| WcolError::msg(format!("finalize graph open failed: {e}")))?;
        let mut host = Self::new(graph, wcs_partial_reduce(producer_count), capacity)?;
        host.core.reduce = Some(ReduceOp::new());
        host.core.finalize = FinalizePhase::Pending;
        Ok(host)
    }

    /// Shard export: one reducer's final partial, written through the steps frame by frame.
    /// Nothing is merged, so memory holds a frame and the sink's row group, not the shard.
    pub fn open_shard_export(cur: &mut PlanCursor<'_>, capacity: usize) -> WcolResult<Self> {
        let len = cur.u32()? as usize;
        let steps = crate::program::decode_coordinator_steps(cur.bytes(len)?)
            .map_err(|e| WcolError::msg(format!("shard export: {e:?}")))?;
        if !cur.exhausted() {
            return Err(WcolError::from("trailing bytes after shard export"));
        }
        if steps.iter().any(|step| {
            matches!(
                step,
                crate::program::CoordinatorStep::Sort(_)
                    | crate::program::CoordinatorStep::Limit { .. }
            )
        }) {
            return Err(WcolError::from("a shard export cannot sort or limit"));
        }
        if !steps
            .iter()
            .any(|step| matches!(step, crate::program::CoordinatorStep::Parquet { .. }))
        {
            return Err(WcolError::from("a shard export writes Parquet"));
        }
        let graph = crate::exec::colocated_plan::open_coordinator_graph(&steps)
            .map_err(|e| WcolError::msg(format!("shard export graph open failed: {e}")))?;
        let mut host = Self::new(graph, wcs_partial_reduce(1), capacity)?;
        host.core.finalize = FinalizePhase::Streaming;
        Ok(host)
    }

    pub(super) fn new(
        graph: LinearColocatedGraph,
        ingress: GraphHostIngress,
        capacity: usize,
    ) -> WcolResult<Self> {
        if !(64..=1024 * 1024).contains(&capacity) {
            return Err(WcolError::from("invalid ring capacity"));
        }
        crate::exec::mem_growth::reset_baseline();
        let row_egress = graph.emits_rows().then(RowBatchWcsEgress::new);
        Ok(Self {
            core: GraphHostCore {
                resident: None,
                graph,
                ingress,
                emit: EmitPartial::new(),
                row_egress,
                hash_fanout: None,
                row_aggregate: None,
                row_end_queued: false,
                partial_payloads: VecDeque::new(),
                reduce: None,
                finalize: FinalizePhase::Inactive,
                spill_bridge: None,
                spill_drive: SpillDrive {
                    phase: SpillPhase::Idle,
                    chunks_finished: false,
                    distinct: DistinctFold::Merge(None),
                },
                direct_port_egress: false,
            },
            input: EdgeBuffer::with_capacity(capacity),
            output: EdgeBuffer::with_capacity(capacity),
            read_at_offset: 0,
            read_at_len: 0,
            read_at_source: 0,
            read_at_ticket: 0,
            ring_capacity: capacity,
            spill_op: 0,
            spill_append_buf: Vec::new(),
            spill_run_upload_mirror: Vec::new(),
            error: None,
        })
    }

    pub(crate) fn note_memory_growth(&mut self, force: bool) {
        if let Some(aggregate) = self.core.row_aggregate.as_ref() {
            aggregate.publish_growth_gauges();
        }
        let (wcs_pending, active_inputs) = self.ingress_gauges();
        let host_spill =
            (self.spill_append_buf.capacity() + self.spill_run_upload_mirror.capacity()) as u64;
        crate::exec::mem_growth::set_ingress(wcs_pending, active_inputs, host_spill);
        crate::exec::mem_growth::observe(force);
    }
}
