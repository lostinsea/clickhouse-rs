use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    marker,
};

use chrono_tz::Tz;
use either::Either;

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{
        block::ColumnIdx,
        column::{datetime64::DEFAULT_TZ, ArcColumnWrapper, ColumnData, LowCardinalityColumnData},
        Column, ColumnType, SqlType, Value,
    },
    Block,
};

#[doc(hidden)]
pub struct ValidatedRow(());

#[inline(always)]
pub(super) fn apply_validated<K: ColumnType, B: RowBuilder>(
    row: B,
    block: &mut Block<K>,
) -> Result<()> {
    block.ensure_duplicate_names();
    block.next_column_hint = 0;
    row.apply_prevalidated(block, &ValidatedRow(()))
}

pub trait RowBuilder {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()>;

    #[doc(hidden)]
    fn apply_prevalidated<K: ColumnType>(
        self,
        block: &mut Block<K>,
        _validated: &ValidatedRow,
    ) -> Result<()>
    where
        Self: Sized,
    {
        self.apply(block)
    }

    #[inline(always)]
    fn contains_native_temporal(&self) -> bool {
        false
    }

    fn validate_native_temporal<K: ColumnType>(&self, _block: &Block<K>) -> Result<()> {
        Ok(())
    }

    #[doc(hidden)]
    fn validate_native_temporal_with_targets<K: ColumnType>(
        &self,
        block: &Block<K>,
        _targets: &mut HashMap<String, SqlType>,
    ) -> Result<()> {
        self.validate_native_temporal(block)
    }
}

pub struct RNil;

pub struct RCons<T>
where
    T: RowBuilder,
{
    key: Cow<'static, str>,
    value: Value,
    tail: T,
    has_native_temporal: bool,
}

impl RNil {
    #[inline(always)]
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        let has_native_temporal = value.contains_native_temporal();
        RCons {
            key,
            value,
            tail: RNil,
            has_native_temporal,
        }
    }
}

impl<T> RCons<T>
where
    T: RowBuilder,
{
    #[inline(always)]
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        let has_native_temporal = value.contains_native_temporal() || self.has_native_temporal;
        RCons {
            key,
            value,
            tail: self,
            has_native_temporal,
        }
    }
}

impl RowBuilder for RNil {
    #[inline(always)]
    fn apply<K: ColumnType>(self, _block: &mut Block<K>) -> Result<()> {
        Ok(())
    }
}

impl<T> RowBuilder for RCons<T>
where
    T: RowBuilder,
{
    #[inline(always)]
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        if block.has_native_temporal || self.has_native_temporal {
            self.validate_native_temporal(block)?;
        }
        apply_validated(self, block)
    }

    #[inline(always)]
    fn apply_prevalidated<K: ColumnType>(
        self,
        block: &mut Block<K>,
        validated: &ValidatedRow,
    ) -> Result<()> {
        put_param(self.key, self.value, block)?;
        self.tail.apply_prevalidated(block, validated)
    }

    #[inline(always)]
    fn contains_native_temporal(&self) -> bool {
        self.has_native_temporal
    }

    fn validate_native_temporal<K: ColumnType>(&self, block: &Block<K>) -> Result<()> {
        self.validate_native_temporal_with_targets(block, &mut HashMap::new())
    }

    fn validate_native_temporal_with_targets<K: ColumnType>(
        &self,
        block: &Block<K>,
        targets: &mut HashMap<String, SqlType>,
    ) -> Result<()> {
        validate_param(&self.key, &self.value, block, targets)?;
        self.tail
            .validate_native_temporal_with_targets(block, targets)
    }
}

impl RowBuilder for Vec<(String, Value)> {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        if block.has_native_temporal || self.contains_native_temporal() {
            self.validate_native_temporal(block)?;
        }
        apply_validated(self, block)
    }

    fn apply_prevalidated<K: ColumnType>(
        self,
        block: &mut Block<K>,
        _validated: &ValidatedRow,
    ) -> Result<()> {
        for (k, v) in self {
            put_param(k.into(), v, block)?;
        }
        Ok(())
    }

    fn contains_native_temporal(&self) -> bool {
        self.iter()
            .any(|(_, value)| value.contains_native_temporal())
    }

    fn validate_native_temporal<K: ColumnType>(&self, block: &Block<K>) -> Result<()> {
        self.validate_native_temporal_with_targets(block, &mut HashMap::new())
    }

    fn validate_native_temporal_with_targets<K: ColumnType>(
        &self,
        block: &Block<K>,
        targets: &mut HashMap<String, SqlType>,
    ) -> Result<()> {
        for (key, value) in self {
            validate_param(key, value, block, targets)?;
        }
        Ok(())
    }
}

fn validate_param<K: ColumnType>(
    key: &str,
    value: &Value,
    block: &Block<K>,
    targets: &mut HashMap<String, SqlType>,
) -> Result<()> {
    let sql_type = match key.get_index(&block.columns) {
        Ok(index) => block.columns[index].sql_type(),
        Err(Error::FromSql(FromSqlError::OutOfRange)) if block.row_count() <= 1 => targets
            .entry(key.to_owned())
            .or_insert_with(|| SqlType::from(value.clone()))
            .clone(),
        Err(error) => return Err(error),
    };
    if sql_type.contains_native_temporal() || value.contains_native_temporal() {
        LowCardinalityColumnData::ensure_writable_type(&sql_type)?;
        validate_native_value(&sql_type, value)?;
    }
    Ok(())
}

fn validate_declared_type(target: &SqlType, source: &SqlType) -> Result<()> {
    match (target, source) {
        (SqlType::Time64(_), SqlType::Time64(_)) => Ok(()),
        (SqlType::Array(target), SqlType::Array(source))
        | (SqlType::Nullable(target), SqlType::Nullable(source))
        | (SqlType::LowCardinality(target), SqlType::LowCardinality(source)) => {
            validate_declared_type(target, source)
        }
        (SqlType::Nullable(target), source) => validate_declared_type(target, source),
        (SqlType::Map(target_key, target_value), SqlType::Map(source_key, source_value)) => {
            validate_declared_type(target_key, source_key)?;
            validate_declared_type(target_value, source_value)
        }
        _ if target == source => Ok(()),
        _ => Err(Error::FromSql(FromSqlError::InvalidType {
            src: source.to_string(),
            dst: target.to_string(),
        })),
    }
}

fn validate_native_value(sql_type: &SqlType, value: &Value) -> Result<()> {
    match (sql_type, value) {
        (SqlType::LowCardinality(inner) | SqlType::SimpleAggregateFunction(_, inner), value) => {
            validate_native_value(inner, value)
        }
        (SqlType::Date32, Value::Date32(_)) => Ok(()),
        (SqlType::Time64(precision), Value::Time64(time)) => {
            time.rescale(precision.get()).map(|_| ())
        }
        (SqlType::Nullable(inner), Value::Nullable(Either::Left(source))) => {
            validate_declared_type(inner, source)
        }
        (SqlType::Nullable(inner), Value::Nullable(Either::Right(value))) => {
            validate_native_value(inner, value)
        }
        (SqlType::Nullable(inner), value) => validate_native_value(inner, value),
        (SqlType::Array(inner), Value::Array(source, values)) => {
            validate_declared_type(inner, source)?;
            for value in values.iter() {
                validate_native_value(inner, value)?;
            }
            Ok(())
        }
        (SqlType::Map(key, value_type), Value::Map(source_key, source_value, entries)) => {
            validate_declared_type(key, source_key)?;
            validate_declared_type(value_type, source_value)?;
            let mut normalized_keys = if matches!(*key, SqlType::Time64(_)) {
                Some(HashSet::with_capacity(entries.len()))
            } else {
                None
            };
            for (entry_key, entry_value) in entries.iter() {
                validate_native_value(key, entry_key)?;
                validate_native_value(value_type, entry_value)?;
                if let (SqlType::Time64(precision), Value::Time64(time), Some(keys)) =
                    (*key, entry_key, &mut normalized_keys)
                {
                    let normalized = time.rescale(precision.get())?.coefficient();
                    if !keys.insert(normalized) {
                        return Err(Error::Other(
                            "Time64 map keys collide after precision conversion".into(),
                        ));
                    }
                }
            }
            Ok(())
        }
        _ if *sql_type == SqlType::from(value.clone()) => Ok(()),
        _ => Err(Error::FromSql(FromSqlError::InvalidType {
            src: SqlType::from(value.clone()).to_string(),
            dst: sql_type.to_string(),
        })),
    }
}

fn put_param<K: ColumnType>(
    key: Cow<'static, str>,
    value: Value,
    block: &mut Block<K>,
) -> Result<()> {
    let column_index = if block.has_duplicate_names == Some(false)
        && block
            .columns
            .get(block.next_column_hint)
            .is_some_and(|column| column.name() == key.as_ref())
    {
        Ok(block.next_column_hint)
    } else {
        key.as_ref().get_index(&block.columns)
    };
    let col_index = match column_index {
        Ok(col_index) => col_index,
        Err(Error::FromSql(FromSqlError::OutOfRange)) => {
            if block.row_count() <= 1 {
                let sql_type = From::from(value.clone());

                let timezone = extract_timezone(&value);

                let column = Column {
                    name: key.clone().into(),
                    data: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                        sql_type,
                        timezone,
                        block.capacity,
                    )?,
                    _marker: marker::PhantomData,
                };

                block.has_native_temporal |= column.sql_type().contains_native_temporal();
                block.columns.push(column);
                return put_param(key, value, block);
            } else {
                return Err(Error::FromSql(FromSqlError::OutOfRange));
            }
        }
        Err(err) => return Err(err),
    };

    block.next_column_hint = col_index + 1;
    block.columns[col_index].push(value);
    Ok(())
}

fn extract_timezone(value: &Value) -> Tz {
    match value {
        Value::Date(_) => *DEFAULT_TZ,
        Value::DateTime(_, tz) => *tz,
        Value::Nullable(Either::Right(d)) => extract_timezone(d),
        Value::Array(_, data) => {
            if let Some(v) = data.first() {
                extract_timezone(v)
            } else {
                *DEFAULT_TZ
            }
        }
        _ => *DEFAULT_TZ,
    }
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;
    use chrono_tz::Tz::{self, UTC};

    use crate::{
        row,
        types::{DateTimeType, Decimal, Simple, SqlType},
    };

    use super::*;

    #[test]
    fn test_push_row() {
        let date_value: NaiveDate = NaiveDate::from_ymd_opt(2016, 10, 22).unwrap();
        let date_time_value: DateTime<Tz> = UTC.with_ymd_and_hms(2014, 7, 8, 14, 0, 0).unwrap();

        let decimal = Decimal::of(2.0_f64, 4);

        let mut block = Block::<Simple>::new();
        block
            .push(row! {
                i8_field: 1_i8,
                i16_field: 1_i16,
                i32_field: 1_i32,
                i64_field: 1_i64,

                u8_field: 1_u8,
                u16_field: 1_u16,
                u32_field: 1_u32,
                u64_field: 1_u64,

                f32_field: 4.66_f32,
                f64_field: 2.71_f64,

                str_field: "text",
                opt_filed: Some("text"),
                nil_filed: Option::<&str>::None,

                date_field: date_value,
                date_time_field: date_time_value,

                decimal_field: decimal
            })
            .unwrap();

        assert_eq!(block.row_count(), 1);

        assert_eq!(block.columns[0].sql_type(), SqlType::Int8);
        assert_eq!(block.columns[1].sql_type(), SqlType::Int16);
        assert_eq!(block.columns[2].sql_type(), SqlType::Int32);
        assert_eq!(block.columns[3].sql_type(), SqlType::Int64);

        assert_eq!(block.columns[4].sql_type(), SqlType::UInt8);
        assert_eq!(block.columns[5].sql_type(), SqlType::UInt16);
        assert_eq!(block.columns[6].sql_type(), SqlType::UInt32);
        assert_eq!(block.columns[7].sql_type(), SqlType::UInt64);

        assert_eq!(block.columns[8].sql_type(), SqlType::Float32);
        assert_eq!(block.columns[9].sql_type(), SqlType::Float64);

        assert_eq!(block.columns[10].sql_type(), SqlType::String);
        assert_eq!(
            block.columns[11].sql_type(),
            SqlType::Nullable(SqlType::String.into())
        );
        assert_eq!(
            block.columns[12].sql_type(),
            SqlType::Nullable(SqlType::String.into())
        );

        assert_eq!(block.columns[13].sql_type(), SqlType::Date);
        assert_eq!(
            block.columns[14].sql_type(),
            SqlType::DateTime(DateTimeType::Chrono)
        );
        assert_eq!(block.columns[15].sql_type(), SqlType::Decimal(18, 4));
    }
}
