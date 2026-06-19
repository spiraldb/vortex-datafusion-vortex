use std::{fs, sync::Arc};

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

    let dir = tempfile::tempdir()?;
    let events_path = dir.path().join("events");
    let limits_path = dir.path().join("limits");
    fs::create_dir_all(&events_path)?;
    fs::create_dir_all(&limits_path)?;

    let events_location = format!("file://{}/", events_path.display());
    let limits_location = format!("file://{}/", limits_path.display());
    println!("writing Vortex data into {}", dir.path().display());

    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE events (id BIGINT, score BIGINT, name VARCHAR) \
         STORED AS vortex LOCATION '{events_location}'"
    ))
    .await?
    .collect()
    .await?;

    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE limits (bucket VARCHAR, min_score BIGINT, max_id BIGINT) \
         STORED AS vortex LOCATION '{limits_location}'"
    ))
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO events VALUES \
         (1, 10, 'alpha'), \
         (2, 25, 'bravo'), \
         (3, 35, 'charlie'), \
         (4, 60, 'delta')",
    )
    .await?
    .collect()
    .await?;

    ctx.sql(
        "INSERT INTO limits VALUES \
         ('low', 20, 3), \
         ('high', 40, 10)",
    )
    .await?
    .collect()
    .await?;

    let query = ctx
        .sql(
            "SELECT e.id, e.name, e.score, l.bucket, l.min_score \
             FROM events e CROSS JOIN limits l \
             WHERE e.score >= l.min_score AND e.id <= l.max_id \
             ORDER BY e.id, l.bucket",
        )
        .await?;

    println!("logical plan:\n{}\n", query.logical_plan());
    let physical_plan = query.create_physical_plan().await?;
    println!(
        "physical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true)
    );

    println!("joined rows where event columns satisfy limit columns:");
    query.show().await?;

    Ok(())
}
