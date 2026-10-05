use chrono_tz::Tz;
use std::{mem, sync::Arc};

use crate::{
    binary::{Encoder, ReadEx},
    errors::{DriverError, Error, FromSqlError, Result},
    types::{
        column::{
            array::ArrayColumnData,
            column_data::{ArcColumnData, BoxColumnData, ColumnData, LowCardinalityAccessor},
            list::List,
            nullable::NullableColumnData,
            numeric::save_data,
            ArcColumnWrapper, ColumnFrom, ColumnWrapper,
        },
        Date32, SqlType, Time64, Time64Precision, Value, ValueRef,
    },
};

#[derive(Default)]
pub(crate) struct TemporalInternals {
    pub(crate) begin: *const (),
    pub(crate) len: usize,
}

fn initialized_coefficients<T: Copy + Default>(size: usize) -> Result<Vec<T>> {
    size.checked_mul(mem::size_of::<T>()).ok_or_else(|| {
        Error::Driver(DriverError::Deserialize(
            "Native temporal column size exceeds platform capacity.".into(),
        ))
    })?;
    let mut data = Vec::new();
    data.try_reserve_exact(size).map_err(|_| {
        Error::Driver(DriverError::Deserialize(
            "Native temporal column allocation failed.".into(),
        ))
    })?;
    data.resize(size, T::default());
    Ok(data)
}

pub(crate) struct Date32ColumnData {
    data: List<i32>,
}

impl Date32ColumnData {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            data: List::with_capacity(capacity),
        }
    }

    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<Self> {
        let mut data = List::from_vec(initialized_coefficients::<i32>(size)?);
        reader.read_bytes(data.as_mut())?;
        Ok(Self { data })
    }
}

impl ColumnFrom for Vec<Date32> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut column = Date32ColumnData::with_capacity(source.len());
        for value in source {
            column.data.push(value.days());
        }
        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Option<Date32>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let inner = Vec::column_from::<ArcColumnWrapper>(Vec::<Date32>::new());
        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };
        for value in source {
            data.push(value.into());
        }
        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Vec<Date32>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let inner = Vec::column_from::<ArcColumnWrapper>(Vec::<Date32>::new());
        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };
        for values in source {
            data.push(Value::Array(
                SqlType::Date32.into(),
                Arc::new(values.into_iter().map(Value::from).collect()),
            ));
        }
        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Vec<Option<Date32>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let inner = Vec::column_from::<ArcColumnWrapper>(Vec::<Option<Date32>>::new());
        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };
        for values in source {
            data.push(values.into());
        }
        W::wrap(data)
    }
}

impl LowCardinalityAccessor for Date32ColumnData {}

impl ColumnData for Date32ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Date32
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        save_data::<i32>(self.data.as_ref(), encoder, start, end);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        let Value::Date32(value) = value else {
            unreachable!("Date32 column values are checked before insertion")
        };
        self.data.push(value.days());
    }

    fn at(&self, index: usize) -> ValueRef<'_> {
        ValueRef::Date32(Date32::new(self.data.at(index)))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        _props: u32,
    ) -> Result<()> {
        if level != 0 {
            return Err(Error::FromSql(FromSqlError::UnsupportedOperation));
        }
        *pointers[0] = self.data.as_ptr().cast();
        *(pointers[1] as *mut usize) = self.len();
        Ok(())
    }

    unsafe fn get_internals(&self, data_ptr: *mut (), level: u8, _props: u32) -> Result<()> {
        if level != 0 {
            return Err(Error::FromSql(FromSqlError::UnsupportedOperation));
        }
        let internals = &mut *(data_ptr as *mut TemporalInternals);
        internals.begin = self.data.as_ptr().cast();
        internals.len = self.len();
        Ok(())
    }

    fn get_timezone(&self) -> Option<Tz> {
        None
    }

    fn get_low_cardinality_accessor(&self) -> Option<&dyn LowCardinalityAccessor> {
        Some(self)
    }
}

pub(crate) struct Time64ColumnData {
    data: List<i64>,
    precision: Time64Precision,
}

impl Time64ColumnData {
    pub(crate) fn with_capacity(capacity: usize, precision: u8) -> Result<Self> {
        let precision = Time64Precision::new(precision)?;
        Ok(Self {
            data: List::with_capacity(capacity),
            precision,
        })
    }

    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize, precision: u8) -> Result<Self> {
        let precision = Time64Precision::new(precision)?;
        let mut column = Self {
            data: List::from_vec(initialized_coefficients::<i64>(size)?),
            precision,
        };
        reader.read_bytes(column.data.as_mut())?;
        Ok(column)
    }

    pub(crate) fn from_coefficients(precision: u8, coefficients: Vec<i64>) -> Result<Self> {
        let precision = Time64Precision::new(precision)?;
        Ok(Self {
            data: List::from_vec(coefficients),
            precision,
        })
    }
}

impl LowCardinalityAccessor for Time64ColumnData {}

impl ColumnData for Time64ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Time64(self.precision)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        save_data::<i64>(self.data.as_ref(), encoder, start, end);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        let Value::Time64(value) = value else {
            unreachable!("Time64 column values are checked before insertion")
        };
        let value = value
            .rescale(self.precision.get())
            .expect("Time64 precision is checked before insertion");
        self.data.push(value.coefficient());
    }

    fn at(&self, index: usize) -> ValueRef<'_> {
        ValueRef::Time64(Time64::from_validated(self.data.at(index), self.precision))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
            precision: self.precision,
        })
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        let SqlType::Time64(precision) = target else {
            return None;
        };
        let mut result = Self::with_capacity(self.len(), precision.get()).ok()?;
        for index in 0..self.len() {
            let value = Time64::from_validated(self.data.at(index), self.precision);
            result
                .data
                .push(value.rescale(precision.get()).ok()?.coefficient());
        }
        Some(Arc::new(result))
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        _props: u32,
    ) -> Result<()> {
        if level != 0 {
            return Err(Error::FromSql(FromSqlError::UnsupportedOperation));
        }
        *pointers[0] = self.data.as_ptr().cast();
        *(pointers[1] as *mut usize) = self.len();
        Ok(())
    }

    unsafe fn get_internals(&self, data_ptr: *mut (), level: u8, _props: u32) -> Result<()> {
        if level != 0 {
            return Err(Error::FromSql(FromSqlError::UnsupportedOperation));
        }
        let internals = &mut *(data_ptr as *mut TemporalInternals);
        internals.begin = self.data.as_ptr().cast();
        internals.len = self.len();
        Ok(())
    }

    fn get_timezone(&self) -> Option<Tz> {
        None
    }

    fn get_low_cardinality_accessor(&self) -> Option<&dyn LowCardinalityAccessor> {
        Some(self)
    }
}
