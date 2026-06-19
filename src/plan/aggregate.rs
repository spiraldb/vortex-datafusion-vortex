use std::{fmt, sync::Arc};

use arrow::{
    array::{Array, ArrayRef as ArrowArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use datafusion::{
    common::{Result, exec_datafusion_err},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, aggregate::AggregateFunctionExpr, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, CardinalityEffect, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{TryStreamExt, stream};
use vortex::{
    aggregate_fn::{
        Accumulator, DynAccumulator, EmptyOptions,
        fns::{null_count::NullCount, sum::Sum},
    },
    array::{IntoArray, VortexSessionExecute, arrays::ConstantArray},
    dtype::{DType, arrow::FromArrowType},
    error::VortexError,
    scalar::Scalar,
    session::VortexSession,
};

use crate::array::VortexArray;

#[derive(Debug, Clone)]
pub struct VortexAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    aggregates: Vec<SumAggregate>,
    schema: SchemaRef,
    session: VortexSession,
    properties: Arc<PlanProperties>,
}

impl VortexAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        input_schema: SchemaRef,
        schema: SchemaRef,
        session: VortexSession,
    ) -> Result<Self> {
        if aggr_expr.len() != schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "VortexAggregateExec expected {} output fields for {} aggregate expressions",
                aggr_expr.len(),
                schema.fields().len()
            )));
        }

        let aggregates = aggr_expr
            .iter()
            .map(|aggregate| SumAggregate::try_new(aggregate, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        let properties = aggregate_properties(Arc::clone(&schema));

        Ok(Self {
            input,
            aggregates,
            schema,
            session,
            properties,
        })
    }

    fn aggregates_display(&self) -> String {
        self.aggregates
            .iter()
            .map(SumAggregate::display)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl DisplayAs for VortexAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VortexAggregateExec: aggr=[{}]",
                    self.aggregates_display()
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "VortexAggregate")?;
                write!(f, "aggr=[{}]", self.aggregates_display())
            }
        }
    }
}

impl ExecutionPlan for VortexAggregateExec {
    fn name(&self) -> &str {
        "VortexAggregateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "VortexAggregateExec expects one child, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            aggregates: self.aggregates.clone(),
            schema: Arc::clone(&self.schema),
            session: self.session.clone(),
            properties: aggregate_properties(Arc::clone(&self.schema)),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "VortexAggregateExec has one output partition, got partition {partition}"
            )));
        }

        let input = Arc::clone(&self.input);
        let aggregates = self.aggregates.clone();
        let schema = Arc::clone(&self.schema);
        let session = self.session.clone();

        let stream = stream::once(async move {
            execute_global_sum(input, aggregates, Arc::clone(&schema), session, context).await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

async fn execute_global_sum(
    input: Arc<dyn ExecutionPlan>,
    aggregates: Vec<SumAggregate>,
    schema: SchemaRef,
    session: VortexSession,
    context: Arc<TaskContext>,
) -> Result<RecordBatch> {
    let mut states = aggregates
        .iter()
        .map(SumAggregateState::try_new)
        .collect::<Result<Vec<_>>>()?;
    let mut vortex_ctx = session.create_execution_ctx();

    for partition in 0..input.properties().partitioning.partition_count() {
        let mut stream = input.execute(partition, Arc::clone(&context))?;
        while let Some(batch) = stream.try_next().await? {
            for (aggregate, state) in aggregates.iter().zip(states.iter_mut()) {
                let column = batch.column(aggregate.column_index);
                let vortex_array = column.as_any().downcast_ref::<VortexArray>().ok_or_else(
                    || {
                        DataFusionError::Internal(format!(
                            "VortexAggregateExec expected VortexArray for input column {}, got {}",
                            aggregate.column_index,
                            column.data_type()
                        ))
                    },
                )?;
                state.accumulate(vortex_array, &mut vortex_ctx)?;
            }
        }
    }

    let columns = aggregates
        .iter()
        .zip(states.into_iter())
        .zip(schema.fields().iter())
        .map(|((aggregate, state), field)| aggregate.finish(state, field.data_type().clone()))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

#[derive(Debug, Clone)]
struct SumAggregate {
    output_name: String,
    column_index: usize,
    column_name: String,
    input_dtype: DType,
}

impl SumAggregate {
    fn try_new(aggregate: &AggregateFunctionExpr, input_schema: &SchemaRef) -> Result<Self> {
        if !aggregate.fun().name().eq_ignore_ascii_case("sum") {
            return Err(DataFusionError::Plan(format!(
                "VortexAggregateExec only supports sum, got {}",
                aggregate.fun().name()
            )));
        }

        if aggregate.is_distinct() {
            return Err(DataFusionError::Plan(
                "VortexAggregateExec does not support DISTINCT aggregates".to_string(),
            ));
        }

        if !aggregate.order_bys().is_empty() {
            return Err(DataFusionError::Plan(
                "VortexAggregateExec does not support ordered aggregates".to_string(),
            ));
        }

        let expressions = aggregate.expressions();
        let [expr] = expressions.as_slice() else {
            return Err(DataFusionError::Plan(format!(
                "VortexAggregateExec expected one sum argument, got {}",
                expressions.len()
            )));
        };

        let column = expr.downcast_ref::<Column>().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "VortexAggregateExec only supports sum over input columns, got {expr}"
            ))
        })?;
        let field = input_schema.field(column.index());

        Ok(Self {
            output_name: aggregate.name().to_string(),
            column_index: column.index(),
            column_name: column.name().to_string(),
            input_dtype: DType::from_arrow(field.as_ref()),
        })
    }

    fn display(&self) -> String {
        format!(
            "sum({}@{}) AS {}",
            self.column_name, self.column_index, self.output_name
        )
    }

    fn finish(
        &self,
        mut state: SumAggregateState,
        output_data_type: arrow::datatypes::DataType,
    ) -> Result<ArrowArrayRef> {
        let (Some(mut sum), Some(mut null_count)) = (state.sum.take(), state.null_count.take())
        else {
            let scalar = Scalar::null(self.input_dtype.as_nullable());
            let vortex_result = ConstantArray::new(scalar, 1).into_array();
            return Ok(Arc::new(VortexArray::new(output_data_type, vortex_result)) as ArrowArrayRef);
        };

        let null_count = null_count
            .finish()
            .map_err(vortex_to_datafusion)?
            .as_primitive()
            .typed_value::<u64>()
            .ok_or_else(|| exec_datafusion_err!("Vortex null_count returned NULL"))?
            as usize;
        let sum = sum.finish().map_err(vortex_to_datafusion)?;

        let scalar = if state.row_count == null_count {
            Scalar::null(sum.dtype().as_nullable())
        } else {
            sum
        };
        let vortex_result = ConstantArray::new(scalar, 1).into_array();

        Ok(Arc::new(VortexArray::new(output_data_type, vortex_result)) as ArrowArrayRef)
    }
}

struct SumAggregateState {
    sum: Option<Accumulator<Sum>>,
    null_count: Option<Accumulator<NullCount>>,
    row_count: usize,
}

impl SumAggregateState {
    fn try_new(_aggregate: &SumAggregate) -> Result<Self> {
        Ok(Self {
            sum: None,
            null_count: None,
            row_count: 0,
        })
    }

    fn accumulate(
        &mut self,
        array: &VortexArray,
        ctx: &mut vortex::array::ExecutionCtx,
    ) -> Result<()> {
        if self.sum.is_none() {
            let input_dtype = array.inner().dtype().clone();
            self.sum = Some(
                Accumulator::try_new(Sum, EmptyOptions, input_dtype.clone())
                    .map_err(vortex_to_datafusion)?,
            );
            self.null_count = Some(
                Accumulator::try_new(NullCount, EmptyOptions, input_dtype)
                    .map_err(vortex_to_datafusion)?,
            );
        }

        self.row_count += array.len();
        self.sum
            .as_mut()
            .expect("sum accumulator must be initialized")
            .accumulate(array.inner(), ctx)
            .map_err(vortex_to_datafusion)?;
        self.null_count
            .as_mut()
            .expect("null_count accumulator must be initialized")
            .accumulate(array.inner(), ctx)
            .map_err(vortex_to_datafusion)?;
        Ok(())
    }
}

fn aggregate_properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

fn vortex_to_datafusion(error: VortexError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
