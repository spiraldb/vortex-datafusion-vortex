use std::sync::Arc;

use datafusion::{
    catalog::memory::DataSourceExec,
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::physical_plan::FileScanConfig,
    physical_expr::{aggregate::AggregateFunctionExpr, expressions::Column},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        aggregates::{AggregateExec, AggregateMode},
        coalesce_partitions::CoalescePartitionsExec,
    },
};
use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt, session::VortexSession};
use vortex_datafusion::{DefaultExpressionConvertor, ExpressionConvertor, VortexSource};

use crate::plan::{
    aggregate::VortexAggregateExec, export::VortexExportExec, is_vortex_plan, scan::VortexScanExec,
};

#[derive(Debug)]
pub struct VortexPlanRule {
    session: VortexSession,
}

impl VortexPlanRule {
    pub fn new() -> Self {
        Self::with_session(VortexSession::default().with_tokio())
    }

    pub fn with_session(session: VortexSession) -> Self {
        Self { session }
    }

    pub fn session(&self) -> &VortexSession {
        &self.session
    }
}

impl Default for VortexPlanRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for VortexPlanRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let session = self.session.clone();
        let mut changed = false;
        let result = plan.clone().transform_up(|plan| {
            if let Some(data_source) = plan.downcast_ref::<DataSourceExec>()
                && let Some(file_scan) = data_source.data_source().downcast_ref::<FileScanConfig>()
                && let Some(vortex_source) = file_scan.file_source().downcast_ref::<VortexSource>()
            {
                changed = true;

                let mut scan = VortexScanExec::try_new(
                    file_scan.object_store_url.clone(),
                    file_scan.file_groups.clone(),
                    file_scan.projected_schema()?.clone(),
                    file_scan
                        .file_source()
                        .projection()
                        .map(|projection| projection.column_indices()),
                    session.clone(),
                    32 * 1024,
                )?;

                if let Some(predicate) = vortex_source.predicate() {
                    let filter =
                        DefaultExpressionConvertor::default().convert(predicate.as_ref())?;
                    scan = scan.with_filter(filter);
                }

                return Ok(Transformed::yes(Arc::new(scan) as _));
            }

            if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
                && is_supported_vortex_aggregate(aggregate)
                && let Some(input) = vortex_input_for_aggregate(aggregate.input())
            {
                changed = true;
                return Ok(Transformed::yes(Arc::new(VortexAggregateExec::try_new(
                    input,
                    aggregate.aggr_expr().to_vec(),
                    aggregate.input_schema(),
                    aggregate.schema(),
                    session.clone(),
                )?) as _));
            }

            let plan_is_vortex = is_vortex_plan(&plan);
            let children = plan.children();
            let mut new_children = Vec::with_capacity(children.len());
            let mut node_changed = false;
            for child in children {
                let child_is_vortex = is_vortex_plan(child);
                if !plan_is_vortex && child_is_vortex && !child.is::<VortexExportExec>() {
                    new_children
                        .push(Arc::new(VortexExportExec::new(child.clone(), session.clone())) as _);
                    node_changed = true;
                } else {
                    new_children.push(child.clone());
                }
            }

            if node_changed {
                changed = true;
                assert_eq!(plan.children().len(), new_children.len());
                Ok(Transformed::yes(plan.with_new_children(new_children)?))
            } else {
                Ok(Transformed::no(plan))
            }
        });

        if changed {
            let optimized = result?.data;
            if is_vortex_native_plan(&optimized) {
                Ok(Arc::new(VortexExportExec::new(optimized, session)) as _)
            } else {
                Ok(optimized)
            }
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "VortexPlanRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn is_supported_vortex_aggregate(aggregate: &AggregateExec) -> bool {
    matches!(
        aggregate.mode(),
        AggregateMode::Single | AggregateMode::SinglePartitioned
    ) && aggregate.group_expr().is_true_no_grouping()
        && aggregate.filter_expr().iter().all(Option::is_none)
        && aggregate.aggr_expr().iter().all(is_supported_sum_aggregate)
}

fn is_supported_sum_aggregate(aggregate: &Arc<AggregateFunctionExpr>) -> bool {
    if !aggregate.fun().name().eq_ignore_ascii_case("sum")
        || aggregate.is_distinct()
        || !aggregate.order_bys().is_empty()
    {
        return false;
    }

    let expressions = aggregate.expressions();
    matches!(expressions.as_slice(), [expr] if expr.downcast_ref::<Column>().is_some())
}

fn vortex_input_for_aggregate(input: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if is_vortex_native_plan(input) {
        return Some(Arc::clone(input));
    }

    if let Some(coalesce) = input.downcast_ref::<CoalescePartitionsExec>() {
        return unwrap_vortex_export(coalesce.input()).filter(is_vortex_native_plan);
    }

    None
}

fn unwrap_vortex_export(input: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(export) = input.downcast_ref::<VortexExportExec>() {
        return Some(Arc::clone(export.input()));
    }

    Some(Arc::clone(input))
}

fn is_vortex_native_plan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    is_vortex_plan(plan) && !plan.is::<VortexExportExec>()
}
