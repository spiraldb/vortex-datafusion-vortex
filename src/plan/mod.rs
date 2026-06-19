use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::plan::{
    aggregate::VortexAggregateExec, export::VortexExportExec, filter::VortexFilterExec,
    scan::VortexScanExec,
};

pub mod aggregate;
pub mod export;
pub mod filter;
pub mod scan;
mod utils;

pub fn is_vortex_plan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<VortexAggregateExec>()
        || plan.is::<VortexExportExec>()
        || plan.is::<VortexScanExec>()
        || plan.is::<VortexFilterExec>()
}
