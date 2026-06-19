use std::{
    fmt,
    sync::Arc,
    task::{Poll, ready},
};

use arrow::array::RecordBatch;
use datafusion::{
    common::exec_datafusion_err,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    },
};
use futures::{Stream, StreamExt};
use vortex::{
    array::{VortexSessionExecute, arrow::ArrowSessionExt},
    session::VortexSession,
};

use crate::array::VortexArray;

#[derive(Debug)]
pub struct VortexExportExec {
    input: Arc<dyn ExecutionPlan>,
    session: VortexSession,
}

impl VortexExportExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, session: VortexSession) -> Self {
        Self { input, session }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for VortexExportExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VortexExportExec: materialize=VortexArray->Arrow, input={}",
                    self.input.name()
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "VortexExport")?;
                write!(f, "materialize=Arrow arrays")
            }
        }
    }
}

impl ExecutionPlan for VortexExportExec {
    fn name(&self) -> &str {
        "VortexExportExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "VortexExportExec expects one child, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.session.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        Ok(Box::pin(VortexExportExecStream {
            input: self.input.execute(partition, context)?,
            session: self.session.clone(),
        }))
    }
}

pub struct VortexExportExecStream {
    input: SendableRecordBatchStream,
    session: VortexSession,
}

impl Stream for VortexExportExecStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let batch = export_array(batch, &self.session)?;
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

impl RecordBatchStream for VortexExportExecStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

fn export_array(
    batch: RecordBatch,
    session: &VortexSession,
) -> datafusion::common::Result<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.num_columns());

    for (idx, col) in batch.columns().iter().enumerate() {
        let Some(vx_array) = col.as_any().downcast_ref::<VortexArray>() else {
            return Err(exec_datafusion_err!("Column expected to be Vortex"));
        };

        let arrow_array = session
            .arrow()
            .execute_arrow(
                vx_array.inner().clone(),
                Some(batch.schema().field(idx)),
                &mut session.create_execution_ctx(),
            )
            .map_err(|_| exec_datafusion_err!("Column expected to be Vortex"))?;
        columns.push(arrow_array);
    }

    Ok(RecordBatch::try_new(batch.schema().clone(), columns)?)
}
