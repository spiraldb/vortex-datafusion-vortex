use std::sync::Arc;

use arrow::datatypes::SchemaRef;
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
        repartition::RepartitionExec,
    },
};
use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt, session::VortexSession};
use vortex_datafusion::VortexSource;
use vortex_datafusion::convert::{DefaultExpressionConvertor, ExpressionConvertor};

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
                && let Some((input, aggr_expr, input_schema)) =
                    vortex_input_for_partial_final_aggregate(aggregate)
            {
                changed = true;
                return Ok(Transformed::yes(Arc::new(VortexAggregateExec::try_new(
                    input,
                    aggr_expr,
                    input_schema,
                    aggregate.schema(),
                    session.clone(),
                )?) as _));
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

fn vortex_input_for_partial_final_aggregate(
    aggregate: &AggregateExec,
) -> Option<(
    Arc<dyn ExecutionPlan>,
    Vec<Arc<AggregateFunctionExpr>>,
    SchemaRef,
)> {
    if !is_supported_vortex_final_aggregate(aggregate) {
        return None;
    }

    let partial = partial_aggregate_input(aggregate.input())?;
    if !is_supported_vortex_partial_aggregate(partial) {
        return None;
    }

    let input = vortex_raw_aggregate_input(partial.input())?;
    Some((input, partial.aggr_expr().to_vec(), partial.input_schema()))
}

fn vortex_raw_aggregate_input(input: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let input = if let Some(repartition) = input.downcast_ref::<RepartitionExec>() {
        repartition.input()
    } else {
        input
    };

    unwrap_vortex_export(input).filter(is_vortex_native_plan)
}

fn partial_aggregate_input(input: &Arc<dyn ExecutionPlan>) -> Option<&AggregateExec> {
    if let Some(coalesce) = input.downcast_ref::<CoalescePartitionsExec>() {
        return coalesce.input().downcast_ref::<AggregateExec>();
    }

    input.downcast_ref::<AggregateExec>()
}

fn is_supported_vortex_final_aggregate(aggregate: &AggregateExec) -> bool {
    matches!(aggregate.mode(), AggregateMode::Final)
        && aggregate.group_expr().is_true_no_grouping()
        && aggregate.filter_expr().iter().all(Option::is_none)
        && aggregate.aggr_expr().iter().all(is_sum_aggregate_function)
}

fn is_supported_vortex_partial_aggregate(aggregate: &AggregateExec) -> bool {
    matches!(aggregate.mode(), AggregateMode::Partial)
        && aggregate.group_expr().is_true_no_grouping()
        && aggregate.filter_expr().iter().all(Option::is_none)
        && aggregate.aggr_expr().iter().all(is_supported_sum_aggregate)
}

fn is_sum_aggregate_function(aggregate: &Arc<AggregateFunctionExpr>) -> bool {
    aggregate.fun().name().eq_ignore_ascii_case("sum")
        && !aggregate.is_distinct()
        && aggregate.order_bys().is_empty()
}

fn is_supported_sum_aggregate(aggregate: &Arc<AggregateFunctionExpr>) -> bool {
    if !is_sum_aggregate_function(aggregate) {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        datasource::provider::DefaultTableFactory,
        execution::SessionStateBuilder,
        physical_plan::{ExecutionPlan, displayable},
        prelude::{SessionConfig, SessionContext},
    };
    use vortex_datafusion::VortexFormatFactory;

    use super::*;

    #[tokio::test]
    async fn rewrites_partial_final_sum_over_vortex_scan() -> anyhow::Result<()> {
        let vortex_format = Arc::new(VortexFormatFactory::new());
        let mut state = SessionStateBuilder::new_with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_table_factory("VORTEX".to_string(), Arc::new(DefaultTableFactory::new()))
            .with_physical_optimizer_rule(Arc::new(VortexPlanRule::new()))
            .build();

        state.register_file_format(vortex_format, false)?;
        let ctx = SessionContext::new_with_state(state);

        let dir = tempfile::tempdir()?;
        let location = format!("file://{}/", dir.path().display());
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE events (id BIGINT) \
             STORED AS vortex LOCATION '{location}'"
        ))
        .await?
        .collect()
        .await?;

        for id in 1..=4 {
            ctx.sql(&format!("INSERT INTO events VALUES ({id})"))
                .await?
                .collect()
                .await?;
        }

        let plan = ctx
            .sql("SELECT SUM(id) AS total FROM events")
            .await?
            .create_physical_plan()
            .await?;
        let plan_display = displayable(plan.as_ref()).indent(true).to_string();

        assert!(
            contains_node::<VortexAggregateExec>(&plan),
            "expected VortexAggregateExec in plan:\n{plan_display}"
        );
        assert!(
            !contains_node::<AggregateExec>(&plan),
            "expected DataFusion AggregateExec chain to be replaced:\n{plan_display}"
        );

        Ok(())
    }

    fn contains_node<T: ExecutionPlan + 'static>(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.is::<T>() || plan.children().into_iter().any(contains_node::<T>)
    }
}
