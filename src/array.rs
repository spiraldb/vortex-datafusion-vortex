use std::{any::Any, sync::Arc};
use vortex::array::ArrayRef as VortexArrayRef;

use arrow::{
    array::{Array, ArrayData, ArrayRef},
    buffer::NullBuffer,
    datatypes::DataType,
};

#[derive(Debug)]
pub struct VortexArray {
    pub data_type: DataType,
    pub inner: VortexArrayRef,
}

impl VortexArray {
    pub fn new(data_type: DataType, inner: VortexArrayRef) -> Self {
        Self { data_type, inner }
    }

    pub fn inner(&self) -> &VortexArrayRef {
        &self.inner
    }
}

unsafe impl Array for VortexArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        panic!("Don't want to implicitly do this")
    }

    fn into_data(self) -> ArrayData {
        self.to_data()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        let arr = self
            .inner
            .slice(offset..offset + length)
            .expect("must slice");

        Arc::new(VortexArray {
            inner: arr,
            data_type: self.data_type.clone(),
        })
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        // TODO: Probably want something like a OnceLock to cache the conversion
        None
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner.nbytes() as usize
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner.nbytes() as usize
    }
}
