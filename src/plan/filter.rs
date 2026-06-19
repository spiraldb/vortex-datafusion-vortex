#![allow(dead_code)]
#![allow(unused)]

use std::{
    fmt,
    sync::Arc,
    task::{Poll, ready},
};

use arrow::array::RecordBatch;
use datafusion::{
    common::Statistics,
    config::ConfigOptions,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::projection::ProjectionRef,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
        execution_plan::CardinalityEffect,
        filter::FilterExec,
        filter_pushdown::{FilterDescription, FilterPushdownPhase},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use delegate;
use futures::{Stream, StreamExt};
use vortex::{
    array::{IntoArray, arrays::StructArray, validity::Validity},
    dtype::{DType, StructFields, arrow::FromArrowType},
    session::VortexSession,
};
use vortex_datafusion::{DefaultExpressionConvertor, ExpressionConvertor};

use crate::{array::VortexArray, plan::utils::chunk_to_record_batch};

#[derive(Debug)]
pub struct VortexFilterExec {
    original: FilterExec,
    predicate: Arc<dyn PhysicalExpr>,
    projection: Option<ProjectionRef>,
    input: Arc<dyn ExecutionPlan>,
    session: VortexSession,
    metrics: ExecutionPlanMetricsSet,
}

impl VortexFilterExec {
    pub fn new(
        original: FilterExec,
        predicate: Arc<dyn PhysicalExpr>,
        projection: Option<ProjectionRef>,
        input: Arc<dyn ExecutionPlan>,
        session: VortexSession,
    ) -> Self {
        Self {
            original,
            predicate,
            projection,
            input,
            session,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    fn projection_display(&self) -> String {
        let Some(projection) = self.projection.as_ref() else {
            return String::new();
        };

        let fields = self.input.schema().fields().clone();
        let columns = projection
            .iter()
            .map(|index| {
                let name = fields
                    .get(*index)
                    .map(|field| field.name().as_str())
                    .unwrap_or("<unknown>");
                format!("{name}@{index}")
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(", projection=[{columns}]")
    }
}

struct VortexFilterExecStream {
    predicate: Arc<dyn PhysicalExpr>,
    projection: Option<ProjectionRef>,
    input: SendableRecordBatchStream,
    session: VortexSession,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for VortexFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VortexFilterExec: predicate={}{}",
                    self.predicate,
                    self.projection_display()
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "VortexFilter")?;
                write!(f, "predicate={}", self.predicate)
            }
        }
    }
}

impl ExecutionPlan for VortexFilterExec {
    fn name(&self) -> &str {
        "VortexFilterExec"
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = VortexFilterExecStream {
            predicate: self.predicate.clone(),
            projection: self.projection.clone(),
            input: self.input.execute(partition, context)?,
            session: self.session.clone(),
            metrics: Default::default(),
        };

        let schema = self
            .projection
            .as_ref()
            .map(|indices| self.input.schema().project(indices).map(Arc::new))
            .transpose()?
            .unwrap_or_else(|| self.input.schema().clone());

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    delegate::delegate! {
        to self.original {
            fn properties(&self) -> &Arc<PlanProperties>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn supports_limit_pushdown(&self) -> bool;
            fn gather_filters_for_pushdown(&self, phase: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, config: &ConfigOptions) -> Result<FilterDescription, DataFusionError>;
            fn partition_statistics(&self, partition: Option<usize>) -> datafusion::common::Result<Arc<Statistics>>;
        }
    }
}

impl Stream for VortexFilterExecStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let batch = apply_filter(&batch, &self.predicate, self.projection.as_ref())?;
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    return Poll::Ready(Some(Ok(batch)));
                }
                other => {
                    return Poll::Ready(other);
                }
            }
        }
    }
}

fn apply_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projection: Option<&ProjectionRef>,
) -> Result<RecordBatch, DataFusionError> {
    let fields = StructFields::from_arrow(batch.schema().fields());
    let children = batch
        .columns()
        .iter()
        .map(|arr| {
            let vx_array = arr.as_any().downcast_ref::<VortexArray>().unwrap();
            vx_array.inner.clone()
        })
        .collect::<Vec<_>>();
    let vx_struct = unsafe {
        StructArray::new_unchecked(children, fields, batch.num_rows(), Validity::AllValid)
            .into_array()
    };

    let expr = DefaultExpressionConvertor::default()
        .convert(predicate.as_ref())
        .unwrap();
    let chunk = vx_struct.apply(&expr).unwrap();

    chunk_to_record_batch(chunk, batch.schema())
}
