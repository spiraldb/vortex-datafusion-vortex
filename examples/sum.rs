use std::sync::Arc;

use datafusion::{
    datasource::provider::DefaultTableFactory, execution::SessionStateBuilder,
    physical_plan::displayable, prelude::SessionContext,
};
use vortex_datafusion::VortexFormatFactory;
use vortex_datafusion_vortex::optimizer::VortexPlanRule;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let vortex_format = Arc::new(VortexFormatFactory::new());
    let mut state = SessionStateBuilder::new_with_default_features()
        .with_table_factory("VORTEX".to_string(), Arc::new(DefaultTableFactory::new()))
        .with_physical_optimizer_rule(Arc::new(VortexPlanRule::new()))
        .build();

    state.register_file_format(vortex_format, false)?;

    let ctx = SessionContext::new_with_state(state);

    // Write the Vortex data into a tempdir so it is cleaned up when `dir` drops.
    let dir = tempfile::tempdir()?;
    let location = format!("file://{}/", dir.path().display());
    println!("writing Vortex data into {}", dir.path().display());

    // Define a Vortex-backed external table over the temp directory.
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE events (id BIGINT, name VARCHAR) \
         STORED AS vortex LOCATION '{location}'"
    ))
    .await?
    .collect()
    .await?;

    // Write some rows. Each INSERT lands a new Vortex file in the directory.
    ctx.sql("INSERT INTO events VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie'), (4, 'delta')")
        .await?
        .collect()
        .await?;

    let query = ctx.sql("SELECT sum(id) FROM events").await?;

    println!("logical plan:\n{}\n", query.logical_plan());
    let physical_plan = query.create_physical_plan().await?;
    println!(
        "physical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true)
    );

    // Read the data back through the Vortex scan to confirm the round-trip.
    println!("contents of the events table:");
    query.show().await?;

    Ok(())
}
