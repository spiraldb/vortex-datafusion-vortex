use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use vortex::array::{
    ArrayRef as VortexArrayRef,
    arrays::{Struct, struct_::StructArrayExt},
};

use crate::array::VortexArray;

pub(crate) fn chunk_to_record_batch(
    chunk: VortexArrayRef,
    schema: SchemaRef,
) -> datafusion::common::Result<RecordBatch> {
    let row_count = chunk.len();
    let struct_array = chunk.as_typed::<Struct>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "VortexScanExec expected struct chunks, got {}",
            chunk.dtype()
        ))
    })?;
    let fields: Vec<_> = struct_array.iter_unmasked_fields().cloned().collect();

    if fields.len() != schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "Vortex chunk has {} fields but output schema has {} fields",
            fields.len(),
            schema.fields().len()
        )));
    }

    let columns = fields
        .into_iter()
        .zip(schema.fields().iter())
        .map(|(field_array, field)| {
            Arc::new(VortexArray::new(field.data_type().clone(), field_array))
                as arrow::array::ArrayRef
        })
        .collect();

    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}
