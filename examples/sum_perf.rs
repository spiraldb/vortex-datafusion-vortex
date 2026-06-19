use std::{fmt, sync::Arc, time::Instant};

use arrow::array::RecordBatch;
use clap::{Parser, ValueEnum};
use datafusion::{
    common::ScalarValue,
    datasource::provider::DefaultTableFactory,
    execution::SessionStateBuilder,
    physical_plan::{collect, displayable},
    prelude::{SessionConfig, SessionContext},
};
use vortex::{
    VortexSessionDefault,
    array::{IntoArray, arrays::StructArray, stream::ArrayStreamExt, validity::Validity},
    buffer::Buffer,
    dtype::Nullability,
    encodings::sparse::Sparse,
    file::{OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::VortexWrite,
    scalar::Scalar,
    session::VortexSession,
};
use vortex_datafusion::VortexFormatFactory;
use vortex_datafusion_vortex::optimizer::VortexPlanRule;

const QUERY: &str = "SELECT SUM(col) AS total FROM vortex_data";

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Regular,
    Vortex,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Vortex => "vortex",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Parser)]
#[command(
    about = "Compare SUM(col) over Vortex files with regular vortex-datafusion vs this crate's optimizer",
    version
)]
struct Args {
    #[arg(long, default_value_t = 1_000_000)]
    rows: usize,
    #[arg(long, default_value_t = 1)]
    files: usize,
    #[arg(long, default_value_t = 1024)]
    sparse_stride: usize,
    #[arg(long, value_enum, default_value_t = Mode::Vortex)]
    mode: Mode,
    #[arg(long)]
    print_plan: bool,
    #[arg(long)]
    print_encoding: bool,
}

impl Args {
    fn validate(self) -> anyhow::Result<Self> {
        if self.rows == 0 {
            anyhow::bail!("--rows must be greater than zero");
        }
        if self.files == 0 {
            anyhow::bail!("--files must be greater than zero");
        }
        if self.sparse_stride == 0 {
            anyhow::bail!("--sparse-stride must be greater than zero");
        }
        if self.rows > i64::MAX as usize {
            anyhow::bail!("--rows must fit in i64 values");
        }

        Ok(self)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse().validate()?;
    let dir = tempfile::tempdir()?;
    let session = VortexSession::default();

    let write_start = Instant::now();
    let write_stats = write_vortex_files(
        dir.path(),
        args.rows,
        args.files,
        args.sparse_stride,
        &session,
    )
    .await?;
    let write_elapsed = write_start.elapsed();

    if args.print_encoding {
        print_first_file_encoding(dir.path(), &session).await?;
    }

    let ctx = session_context(args.mode)?;
    let location = format!("file://{}/", dir.path().display());
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE vortex_data (col BIGINT) \
         STORED AS vortex LOCATION '{location}'"
    ))
    .await?
    .collect()
    .await?;

    let query_start = Instant::now();
    let dataframe = ctx.sql(QUERY).await?;
    let logical_elapsed = query_start.elapsed();

    let physical_start = Instant::now();
    let physical_plan = dataframe.create_physical_plan().await?;
    let physical_elapsed = physical_start.elapsed();

    if args.print_plan {
        println!(
            "physical plan:\n{}",
            displayable(physical_plan.as_ref()).indent(true)
        );
    }

    let execution_start = Instant::now();
    let batches = collect(physical_plan, ctx.task_ctx()).await?;
    let execution_elapsed = execution_start.elapsed();
    let total_query_elapsed = query_start.elapsed();

    let result = scalar_result(&batches)?;
    let expected_sum = expected_sum(args.rows, args.sparse_stride);

    println!("mode: {}", args.mode);
    println!("rows: {}", args.rows);
    println!("files: {}", args.files);
    println!("sparse stride: {}", args.sparse_stride);
    println!("sparse patches: {}", write_stats.patch_count);
    println!("generated encoding: sparse(fill=0)");
    println!("data path: {}", dir.path().display());
    println!("write time (not query): {:.3?}", write_elapsed);
    println!("logical planning: {:.3?}", logical_elapsed);
    println!("physical planning: {:.3?}", physical_elapsed);
    println!("query execution: {:.3?}", execution_elapsed);
    println!("query total: {:.3?}", total_query_elapsed);
    println!("result: {result}");
    println!("expected: {expected_sum}");

    Ok(())
}

async fn write_vortex_files(
    dir: &std::path::Path,
    rows: usize,
    files: usize,
    sparse_stride: usize,
    session: &VortexSession,
) -> anyhow::Result<WriteStats> {
    let mut next_row = 0usize;
    let base_rows = rows / files;
    let extra_rows = rows % files;
    let mut stats = WriteStats::default();

    for file_idx in 0..files {
        let rows_in_file = base_rows + usize::from(file_idx < extra_rows);
        let end_row = next_row
            .checked_add(rows_in_file)
            .ok_or_else(|| anyhow::anyhow!("row count overflowed usize"))?;

        let (col, patch_count) = sparse_column(next_row, end_row, sparse_stride)?;
        stats.patch_count += patch_count;

        let vortex_array = StructArray::try_new(
            ["col"].into(),
            vec![col],
            rows_in_file,
            Validity::NonNullable,
        )?
        .into_array();

        let path = dir.join(format!("part-{file_idx:05}.vortex"));
        let mut file = tokio::fs::File::create(path).await?;
        session
            .write_options()
            .write(&mut file, vortex_array.to_array_stream())
            .await?;
        file.shutdown().await?;

        next_row = end_row;
    }

    Ok(stats)
}

async fn print_first_file_encoding(
    dir: &std::path::Path,
    session: &VortexSession,
) -> anyhow::Result<()> {
    let path = dir.join("part-00000.vortex");
    let array = session
        .open_options()
        .open_path(path)
        .await?
        .scan()?
        .into_array_stream()?
        .read_all()
        .await?;

    println!(
        "first file encoding tree:\n{}",
        array.display_tree_encodings_only()
    );
    Ok(())
}

fn sparse_column(
    global_start: usize,
    global_end: usize,
    sparse_stride: usize,
) -> anyhow::Result<(vortex::array::ArrayRef, usize)> {
    let first_patch = if global_start == 0 {
        sparse_stride
    } else {
        global_start.div_ceil(sparse_stride) * sparse_stride
    };

    let patch_count = if first_patch >= global_end {
        0
    } else {
        ((global_end - 1 - first_patch) / sparse_stride) + 1
    };

    let mut indices = Vec::with_capacity(patch_count);
    let mut values = Vec::with_capacity(patch_count);
    for global_row in (first_patch..global_end).step_by(sparse_stride) {
        indices.push((global_row - global_start) as u64);
        values.push(global_row as i64);
    }

    let indices: Buffer<u64> = indices.into_iter().collect();
    let values: Buffer<i64> = values.into_iter().collect();
    let sparse = Sparse::try_new(
        indices.into_array(),
        values.into_array(),
        global_end - global_start,
        Scalar::primitive(0i64, Nullability::NonNullable),
    )?;

    Ok((sparse.into_array(), patch_count))
}

#[derive(Default)]
struct WriteStats {
    patch_count: usize,
}

fn session_context(mode: Mode) -> anyhow::Result<SessionContext> {
    let vortex_format = Arc::new(VortexFormatFactory::new());
    let mut state_builder = SessionStateBuilder::new_with_default_features()
        // Keep the benchmark in the single-stage aggregate shape supported by VortexAggregateExec.
        // .with_config(SessionConfig::new().with_target_partitions(1))
        .with_table_factory("VORTEX".to_string(), Arc::new(DefaultTableFactory::new()));

    if mode == Mode::Vortex {
        state_builder = state_builder.with_physical_optimizer_rule(Arc::new(VortexPlanRule::new()));
    }

    let mut state = state_builder.build();
    state.register_file_format(vortex_format, false)?;

    Ok(SessionContext::new_with_state(state))
}

fn scalar_result(batches: &[RecordBatch]) -> anyhow::Result<ScalarValue> {
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("query returned no batches"))?;
    if batch.num_columns() != 1 || batch.num_rows() != 1 {
        anyhow::bail!(
            "expected one row and one column, got {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }

    Ok(ScalarValue::try_from_array(batch.column(0).as_ref(), 0)?)
}

fn expected_sum(rows: usize, sparse_stride: usize) -> i128 {
    let last_patch_multiple = ((rows - 1) / sparse_stride) as i128;
    sparse_stride as i128 * last_patch_multiple * (last_patch_multiple + 1) / 2
}
