use std::{fmt, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    common::{Result, Statistics},
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile, object_store::ObjectStoreUrl, physical_plan::FileGroup,
    },
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    object_store::ObjectStore,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
        execution_plan::{Boundedness, CardinalityEffect, EmissionType},
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
            PushedDown,
        },
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{
    StreamExt, TryStreamExt, future,
    stream::{self, BoxStream},
};
use vortex::{
    array::memory::MemorySessionExt,
    error::VortexError,
    expr::{Expression, and as vx_and, root, select},
    file::OpenOptionsSessionExt,
    io::{object_store::ObjectStoreReadAt, session::RuntimeSessionExt},
    session::VortexSession,
};
use vortex_datafusion::{DefaultExpressionConvertor, ExpressionConvertor};

use crate::plan::utils::chunk_to_record_batch;

#[derive(Clone)]
pub struct VortexScanExec {
    object_store_url: ObjectStoreUrl,
    file_groups: Vec<FileGroup>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    session: VortexSession,
    filter: Option<Expression>,
    batch_size: usize,
    properties: Arc<PlanProperties>,
}

impl VortexScanExec {
    pub fn try_new(
        object_store_url: ObjectStoreUrl,
        file_groups: Vec<FileGroup>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        session: VortexSession,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            return Err(DataFusionError::Plan(
                "VortexScanExec batch_size must be greater than zero".to_string(),
            ));
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            object_store_url,
            file_groups,
            schema,
            projection,
            session,
            filter: None,
            batch_size,
            properties,
        })
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn file_groups(&self) -> &[FileGroup] {
        &self.file_groups
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn with_filter(mut self, filter: Expression) -> Self {
        self.filter = Some(combine_vortex_filters(self.filter, Some(filter)));
        self
    }

    pub fn filter(&self) -> Option<&Expression> {
        self.filter.as_ref()
    }

    fn file_count(&self) -> usize {
        self.file_groups
            .iter()
            .map(|file_group| file_group.files().len())
            .sum()
    }

    fn file_group_sizes(&self) -> String {
        self.file_groups
            .iter()
            .map(|file_group| file_group.files().len().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn schema_display(&self) -> String {
        self.schema
            .fields()
            .iter()
            .map(|field| format!("{}:{}", field.name(), field.data_type()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn filter_display(&self) -> String {
        self.filter
            .as_ref()
            .map(|filter| format!(", filter={filter}"))
            .unwrap_or_default()
    }
}

impl fmt::Debug for VortexScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VortexScanExec")
            .field("object_store_url", &self.object_store_url)
            .field("file_groups", &self.file_groups)
            .field("schema", &self.schema)
            .field(
                "filter",
                &self.filter.as_ref().map(|filter| format!("{filter}")),
            )
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for VortexScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "VortexScanExec: store={}, file_groups={}, files={}, schema=[{}], batch_size={}",
                    self.object_store_url,
                    self.file_groups.len(),
                    self.file_count(),
                    self.schema_display(),
                    self.batch_size,
                )
                .and_then(|_| write!(f, "{}", self.filter_display()))
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VortexScanExec: store={}, file_groups={}, file_group_sizes=[{}], files={}, schema=[{}], batch_size={}",
                    self.object_store_url,
                    self.file_groups.len(),
                    self.file_group_sizes(),
                    self.file_count(),
                    self.schema_display(),
                    self.batch_size,
                )
                .and_then(|_| write!(f, "{}", self.filter_display()))
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "VortexScan")?;
                writeln!(f, "store={}", self.object_store_url)?;
                writeln!(f, "files={}", self.file_count())?;
                writeln!(f, "partitions={}", self.file_groups.len())?;
                if let Some(filter) = &self.filter {
                    writeln!(f, "filter={filter}")?;
                }
                write!(f, "batch_size={}", self.batch_size)
            }
        }
    }
}

impl ExecutionPlan for VortexScanExec {
    fn name(&self) -> &str {
        "VortexScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "VortexScanExec is a leaf plan but received {} children",
                children.len()
            )));
        }

        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let file_group = self.file_groups.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "partition {partition} out of bounds for {} file groups",
                self.file_groups.len()
            ))
        })?;

        let object_store = context.runtime_env().object_store(&self.object_store_url)?;
        let files = file_group.files().to_vec();
        let schema = Arc::clone(&self.schema);
        let session = self.session.clone();
        let filter = self.filter.clone();
        let batch_size = self.batch_size;
        let projection = self.projection.clone();

        let stream = stream::iter(files)
            .then(move |file| {
                scan_file(
                    Arc::clone(&object_store),
                    file,
                    Arc::clone(&schema),
                    projection.clone(),
                    session.clone(),
                    filter.clone(),
                    batch_size,
                )
            })
            .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if let Some(partition) = partition {
            let Some(file_group) = self.file_groups.get(partition) else {
                return Err(DataFusionError::Internal(format!(
                    "partition {partition} out of bounds for {} file groups",
                    self.file_groups.len()
                )));
            };

            if let Some(stats) = file_group.file_statistics(None) {
                return Ok(Arc::new(stats.clone()));
            }
        }

        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        if self.filter.is_some() {
            CardinalityEffect::LowerEqual
        } else {
            CardinalityEffect::Equal
        }
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        _parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::new())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|filter| filter.filter)
            .collect::<Vec<_>>();
        let (pushdown_result, filter) = convert_pushdown_filters(&parent_filters, &self.schema);

        let mut propagation: FilterPushdownPropagation<Arc<dyn ExecutionPlan>> =
            FilterPushdownPropagation::with_parent_pushdown_result(pushdown_result);

        if let Some(filter) = filter {
            let mut updated = self.clone();
            updated.filter = Some(combine_vortex_filters(updated.filter, Some(filter)));
            propagation =
                propagation.with_updated_node(Arc::new(updated) as Arc<dyn ExecutionPlan>);
        }

        Ok(propagation)
    }
}

async fn scan_file(
    object_store: Arc<dyn ObjectStore>,
    file: PartitionedFile,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    session: VortexSession,
    filter: Option<Expression>,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    if file.range.is_some() {
        return Err(DataFusionError::NotImplemented(
            "VortexScanExec does not support byte ranges yet".to_string(),
        ));
    }

    if !file.partition_values.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "VortexScanExec does not append partition values yet".to_string(),
        ));
    }

    let reader = ObjectStoreReadAt::new_with_allocator(
        object_store,
        file.object_meta.location,
        session.handle(),
        session.allocator(),
    );

    let vxf = session
        .open_options()
        .with_file_size(file.object_meta.size)
        .open(Arc::new(reader))
        .await
        .map_err(vortex_to_datafusion)?;

    let mut scanner = vxf.scan().map_err(vortex_to_datafusion)?;
    if let Some(filter) = filter {
        scanner = scanner.with_filter(filter);
    }

    if let Some(projection) = projection {
        let sf = vxf.dtype().as_struct_fields();
        let fields = projection
            .iter()
            .copied()
            .map(|idx| sf.field_name(idx).unwrap())
            .cloned()
            .collect::<Vec<_>>();
        scanner = scanner.with_projection(select(fields, root()));
    }

    let array_stream = scanner.into_stream().map_err(vortex_to_datafusion)?;

    let stream = array_stream
        .map(move |chunk| {
            let schema = Arc::clone(&schema);
            match chunk
                .map_err(vortex_to_datafusion)
                .and_then(|chunk| chunk_to_record_batch(chunk, schema))
            {
                Ok(batch) => {
                    stream::iter(split_record_batch(batch, batch_size).into_iter().map(Ok)).boxed()
                }
                Err(error) => stream::once(future::ready(Err(error))).boxed(),
            }
        })
        .flatten()
        .boxed();

    Ok(stream)
}

fn split_record_batch(batch: RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    if batch.num_rows() <= batch_size {
        return vec![batch];
    }

    let mut batches = Vec::with_capacity(batch.num_rows().div_ceil(batch_size));
    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, length));
        offset += length;
    }
    batches
}

fn vortex_to_datafusion(error: VortexError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn convert_pushdown_filters(
    filters: &[Arc<dyn PhysicalExpr>],
    schema: &SchemaRef,
) -> (Vec<PushedDown>, Option<Expression>) {
    let convertor = DefaultExpressionConvertor::default();
    let mut pushdown_result = Vec::with_capacity(filters.len());
    let mut converted = Vec::new();

    for filter in filters {
        if convertor.can_be_pushed_down(filter, schema.as_ref()) {
            match convertor.convert(filter.as_ref()) {
                Ok(filter) => {
                    pushdown_result.push(PushedDown::Yes);
                    converted.push(filter);
                }
                Err(_) => pushdown_result.push(PushedDown::No),
            }
        } else {
            pushdown_result.push(PushedDown::No);
        }
    }

    (pushdown_result, and_collect_vortex_filters(converted))
}

fn and_collect_vortex_filters(filters: impl IntoIterator<Item = Expression>) -> Option<Expression> {
    filters.into_iter().reduce(vx_and)
}

fn combine_vortex_filters(lhs: Option<Expression>, rhs: Option<Expression>) -> Expression {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => vx_and(lhs, rhs),
        (Some(filter), None) | (None, Some(filter)) => filter,
        (None, None) => unreachable!("at least one Vortex filter must be present"),
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        common::ScalarValue,
        config::ConfigOptions,
        logical_expr::Operator,
        object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, memory::InMemory, path::Path},
        physical_expr::expressions::{BinaryExpr, col, lit},
        physical_plan::filter_pushdown::{
            ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
        },
        physical_plan::{ExecutionPlan, common::collect as collect_stream},
        prelude::SessionContext,
    };
    use vortex::{
        VortexSessionDefault,
        array::arrow::ArrowSessionExt,
        file::WriteOptionsSessionExt,
        io::{VortexWrite, object_store::ObjectStoreWrite},
    };

    use super::*;
    use crate::array::VortexArray;

    #[tokio::test]
    async fn scan_returns_vortex_array_columns() -> Result<()> {
        let session = VortexSession::default();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
            ],
        )?;

        let location = Path::from("data.vortex");
        let object_meta = write_vortex_file(Arc::clone(&store), &location, batch, &session).await?;
        let object_store_url = ObjectStoreUrl::parse("memory://vortex-test").unwrap();
        let ctx = SessionContext::new();
        ctx.register_object_store(object_store_url.as_ref(), Arc::clone(&store));

        let exec = VortexScanExec::try_new(
            object_store_url,
            vec![FileGroup::new(vec![PartitionedFile::new_from_meta(
                object_meta,
            )])],
            Arc::clone(&schema),
            None,
            session,
            2,
        )?;

        let batches = collect_stream(exec.execute(0, ctx.task_ctx())?).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
        assert!(batches.iter().all(|batch| {
            batch
                .columns()
                .iter()
                .all(|column| column.as_any().is::<VortexArray>())
        }));

        Ok(())
    }

    #[test]
    fn pushdown_classifies_convertible_filters() -> Result<()> {
        let session = VortexSession::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "payload",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));
        let exec = VortexScanExec::try_new(
            ObjectStoreUrl::parse("memory://vortex-test").unwrap(),
            vec![FileGroup::new(vec![])],
            Arc::clone(&schema),
            None,
            session,
            1024,
        )?;

        let supported = Arc::new(BinaryExpr::new(
            col("id", schema.as_ref())?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(2))),
        )) as Arc<dyn PhysicalExpr>;
        let unsupported = col("payload", schema.as_ref())?;

        let filter_description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Pre,
            vec![Arc::clone(&supported), Arc::clone(&unsupported)],
            &ConfigOptions::new(),
        )?;
        assert!(filter_description.parent_filters().is_empty());

        let propagation = exec.handle_child_pushdown_result(
            FilterPushdownPhase::Pre,
            ChildPushdownResult {
                parent_filters: vec![
                    ChildFilterPushdownResult {
                        filter: supported,
                        child_results: vec![],
                    },
                    ChildFilterPushdownResult {
                        filter: unsupported,
                        child_results: vec![],
                    },
                ],
                self_filters: vec![],
            },
            &ConfigOptions::new(),
        )?;

        assert_eq!(propagation.filters.len(), 2);
        assert!(matches!(propagation.filters[0], PushedDown::Yes));
        assert!(matches!(propagation.filters[1], PushedDown::No));

        let updated = propagation
            .updated_node
            .expect("supported filter updates scan");
        let updated = updated
            .downcast_ref::<VortexScanExec>()
            .expect("updated node is a VortexScanExec");
        assert!(updated.filter().is_some());

        Ok(())
    }

    #[tokio::test]
    async fn scan_applies_pushed_filter() -> Result<()> {
        let session = VortexSession::default();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
            ],
        )?;

        let location = Path::from("filtered.vortex");
        let object_meta = write_vortex_file(Arc::clone(&store), &location, batch, &session).await?;
        let object_store_url = ObjectStoreUrl::parse("memory://vortex-test").unwrap();
        let ctx = SessionContext::new();
        ctx.register_object_store(object_store_url.as_ref(), Arc::clone(&store));

        let filter = Arc::new(BinaryExpr::new(
            col("id", schema.as_ref())?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(3))),
        )) as Arc<dyn PhysicalExpr>;
        let (pushdown, vortex_filter) = convert_pushdown_filters(&[filter], &schema);
        assert!(matches!(pushdown[0], PushedDown::Yes));

        let exec = VortexScanExec::try_new(
            object_store_url,
            vec![FileGroup::new(vec![PartitionedFile::new_from_meta(
                object_meta,
            )])],
            Arc::clone(&schema),
            None,
            session,
            1024,
        )?
        .with_filter(vortex_filter.expect("filter should convert"));

        let batches = collect_stream(exec.execute(0, ctx.task_ctx())?).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_file_ranges_and_partition_values() -> Result<()> {
        let session = VortexSession::default();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let object_store_url = ObjectStoreUrl::parse("memory://vortex-test").unwrap();
        let ctx = SessionContext::new();
        ctx.register_object_store(object_store_url.as_ref(), store);

        let ranged_exec = VortexScanExec::try_new(
            object_store_url.clone(),
            vec![FileGroup::new(vec![
                PartitionedFile::new("data.vortex", 1).with_range(0, 1),
            ])],
            Arc::clone(&schema),
            None,
            session.clone(),
            1024,
        )?;
        let error = collect_stream(ranged_exec.execute(0, ctx.task_ctx())?)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("byte ranges"));

        let partitioned_exec = VortexScanExec::try_new(
            object_store_url,
            vec![FileGroup::new(vec![
                PartitionedFile::new("data.vortex", 1)
                    .with_partition_values(vec![ScalarValue::Utf8(Some("p".to_string()))]),
            ])],
            schema,
            None,
            session,
            1024,
        )?;
        let error = collect_stream(partitioned_exec.execute(0, ctx.task_ctx())?)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("partition values"));

        Ok(())
    }

    async fn write_vortex_file(
        store: Arc<dyn ObjectStore>,
        location: &Path,
        batch: RecordBatch,
        session: &VortexSession,
    ) -> Result<ObjectMeta> {
        let schema = batch.schema();
        let vortex_array = session
            .arrow()
            .from_arrow_record_batch(batch, schema.as_ref())
            .map_err(vortex_to_datafusion)?;
        let mut writer = ObjectStoreWrite::new(Arc::clone(&store), location)
            .await
            .map_err(vortex_to_datafusion)?;

        session
            .write_options()
            .write(&mut writer, vortex_array.to_array_stream())
            .await
            .map_err(vortex_to_datafusion)?;
        writer
            .shutdown()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        store
            .head(location)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}
