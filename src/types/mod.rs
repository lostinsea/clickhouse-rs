use std::{borrow::Cow, collections::HashMap, fmt, mem, pin::Pin, str::FromStr, sync::Mutex};

use chrono::prelude::*;
use chrono_tz::Tz;
use hostname::get;

use lazy_static::lazy_static;

use crate::{
    errors::{Error, ServerError},
    types::column::datetime64::DEFAULT_TZ,
};

pub use self::{
    block::{Block, RCons, RNil, Row, RowBuilder, Rows},
    column::{Column, ColumnType, Complex, Simple},
    decimal::Decimal,
    enums::{Enum16, Enum8},
    from_sql::{FromSql, FromSqlResult},
    options::Options,
    options::{SettingType, SettingValue},
    query::{Query, QueryParameterValue},
    query_result::QueryResult,
    temporal::{Date32, Time64},
    value::Value,
    value_ref::ValueRef,
};

pub(crate) use self::{
    cmd::Cmd,
    date_converter::DateConverter,
    marshal::Marshal,
    options::{IntoOptions, OptionsSource},
    stat_buffer::StatBuffer,
    unmarshal::Unmarshal,
};

pub mod column;
mod marshal;
mod stat_buffer;
mod unmarshal;

mod from_sql;
mod value;
mod value_ref;

pub(crate) mod block;
mod cmd;

mod date_converter;
mod query;
pub(crate) mod query_result;

mod decimal;
mod enums;
mod options;
mod temporal;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub(crate) struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
}

#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub(crate) struct ProfileInfo {
    pub rows: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub(crate) struct TableColumns {
    pub table_name: String,
    pub columns: String,
}

#[derive(Clone, PartialEq)]
pub(crate) struct ServerInfo {
    pub name: String,
    pub revision: u64,
    pub minor_version: u64,
    pub major_version: u64,
    pub timezone: Tz,
    pub display_name: String,
    pub patch_version: u64,
}

impl fmt::Debug for ServerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}.{}.{}.{} ({:?})",
            self.name,
            self.major_version,
            self.minor_version,
            self.revision,
            self.patch_version,
            self.timezone
        )
    }
}

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) server_info: ServerInfo,
    pub(crate) hostname: String,
    pub(crate) options: OptionsSource,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            revision: 0,
            minor_version: 0,
            major_version: 0,
            timezone: *DEFAULT_TZ,
            display_name: "".into(),
            patch_version: 0,
        }
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context")
            .field("options", &self.options)
            .field("hostname", &self.hostname)
            .finish()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            server_info: ServerInfo::default(),
            hostname: get().unwrap().into_string().unwrap(),
            options: OptionsSource::default(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Packet<S> {
    Hello(S, ServerInfo),
    Pong(S),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    ProfileEvents(Block),
    TableColumns(TableColumns),
    Exception(ServerError),
    Block(Block),
    Eof(S),
}

impl<S> fmt::Debug for Packet<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Packet::Hello(_, info) => write!(f, "Hello({info:?})"),
            Packet::Pong(_) => write!(f, "Pong"),
            Packet::Progress(p) => write!(f, "Progress({p:?})"),
            Packet::ProfileInfo(info) => write!(f, "ProfileInfo({info:?})"),
            Packet::ProfileEvents(b) => write!(f, "ProfileEvents({b:?})"),
            Packet::TableColumns(info) => write!(f, "TableColumns({info:?})"),
            Packet::Exception(e) => write!(f, "Exception({e:?})"),
            Packet::Block(b) => write!(f, "Block({b:?})"),
            Packet::Eof(_) => write!(f, "Eof"),
        }
    }
}

impl<S> Packet<S> {
    pub fn bind<N>(self, transport: &mut Option<N>) -> Packet<N> {
        match self {
            Packet::Hello(_, server_info) => Packet::Hello(transport.take().unwrap(), server_info),
            Packet::Pong(_) => Packet::Pong(transport.take().unwrap()),
            Packet::Progress(progress) => Packet::Progress(progress),
            Packet::ProfileInfo(profile_info) => Packet::ProfileInfo(profile_info),
            Packet::ProfileEvents(block) => Packet::ProfileEvents(block),
            Packet::TableColumns(table_columns) => Packet::TableColumns(table_columns),
            Packet::Exception(exception) => Packet::Exception(exception),
            Packet::Block(block) => Packet::Block(block),
            Packet::Eof(_) => Packet::Eof(transport.take().unwrap()),
        }
    }
}

pub trait HasSqlType {
    fn get_sql_type() -> SqlType;
}

macro_rules! has_sql_type {
    ( $( $t:ty : $k:expr ),* ) => {
        $(
            impl HasSqlType for $t {
                fn get_sql_type() -> SqlType {
                    $k
                }
            }
        )*
    };
}

has_sql_type! {
    bool: SqlType::Bool,
    u8: SqlType::UInt8,
    u16: SqlType::UInt16,
    u32: SqlType::UInt32,
    u64: SqlType::UInt64,
    u128: SqlType::UInt128,
    i8: SqlType::Int8,
    i16: SqlType::Int16,
    i32: SqlType::Int32,
    i64: SqlType::Int64,
    i128: SqlType::Int128,
    &str: SqlType::String,
    String: SqlType::String,
    f32: SqlType::Float32,
    f64: SqlType::Float64,
    NaiveDate: SqlType::Date,
    Date32: SqlType::Date32,
    DateTime<Tz>: SqlType::DateTime(DateTimeType::DateTime32)
}

impl<K, V> HasSqlType for HashMap<K, V>
where
    K: HasSqlType,
    V: HasSqlType,
{
    fn get_sql_type() -> SqlType {
        SqlType::Map(K::get_sql_type().into(), V::get_sql_type().into())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeType {
    DateTime32,
    DateTime64(u32, Tz),
    Chrono,
}

#[derive(Debug, Copy, Clone, PartialOrd, Eq, PartialEq, Hash)]
pub enum SimpleAggFunc {
    Any,
    AnyLast,
    Min,
    Max,
    Sum,
    SumWithOverflow,
    GroupBitAnd,
    GroupBitOr,
    GroupBitXor,
    GroupArrayArray,
    GroupUniqArrayArray,
    SumMap,
    MinMap,
    MaxMap,
    ArgMin,
    ArgMax,
}

impl From<SimpleAggFunc> for &str {
    fn from(source: SimpleAggFunc) -> &'static str {
        match source {
            SimpleAggFunc::Any => "any",
            SimpleAggFunc::AnyLast => "anyLast",
            SimpleAggFunc::Min => "min",
            SimpleAggFunc::Max => "max",
            SimpleAggFunc::Sum => "sum",
            SimpleAggFunc::SumWithOverflow => "sumWithOverflow",
            SimpleAggFunc::GroupBitAnd => "groupBitAnd",
            SimpleAggFunc::GroupBitOr => "groupBitOr",
            SimpleAggFunc::GroupBitXor => "groupBitXor",
            SimpleAggFunc::GroupArrayArray => "groupArrayArray",
            SimpleAggFunc::GroupUniqArrayArray => "groupUniqArrayArray",
            SimpleAggFunc::SumMap => "sumMap",
            SimpleAggFunc::MinMap => "minMap",
            SimpleAggFunc::MaxMap => "maxMap",
            SimpleAggFunc::ArgMin => "argMin",
            SimpleAggFunc::ArgMax => "argMax",
        }
    }
}

impl FromStr for SimpleAggFunc {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(SimpleAggFunc::Any),
            "anyLast" => Ok(SimpleAggFunc::AnyLast),
            "min" => Ok(SimpleAggFunc::Min),
            "max" => Ok(SimpleAggFunc::Max),
            "sum" => Ok(SimpleAggFunc::Sum),
            "sumWithOverflow" => Ok(SimpleAggFunc::SumWithOverflow),
            "groupBitAnd" => Ok(SimpleAggFunc::GroupBitAnd),
            "groupBitOr" => Ok(SimpleAggFunc::GroupBitOr),
            "groupBitXor" => Ok(SimpleAggFunc::GroupBitXor),
            "groupArrayArray" => Ok(SimpleAggFunc::GroupArrayArray),
            "groupUniqArrayArray" => Ok(SimpleAggFunc::GroupUniqArrayArray),
            "sumMap" => Ok(SimpleAggFunc::SumMap),
            "minMap" => Ok(SimpleAggFunc::MinMap),
            "maxMap" => Ok(SimpleAggFunc::MaxMap),
            "argMin" => Ok(SimpleAggFunc::ArgMin),
            "argMax" => Ok(SimpleAggFunc::ArgMax),
            _ => Err(()),
        }
    }
}

/// The validated decimal precision of a Time64 coefficient.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Time64Precision(u8);

impl Time64Precision {
    pub fn new(precision: u8) -> crate::errors::Result<Self> {
        Self::try_from(precision)
    }

    pub fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for Time64Precision {
    type Error = Error;

    fn try_from(precision: u8) -> crate::errors::Result<Self> {
        if precision > 9 {
            return Err(Error::Other(
                format!("Time64 precision {precision} is outside 0..=9").into(),
            ));
        }
        Ok(Self(precision))
    }
}

impl fmt::Display for Time64Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SqlType {
    Bool,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    String,
    FixedString(usize),
    Float32,
    Float64,
    Date,
    DateTime(DateTimeType),
    Ipv4,
    Ipv6,
    Uuid,
    Nullable(&'static SqlType),
    Array(&'static SqlType),
    LowCardinality(&'static SqlType),
    Decimal(u8, u8),
    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),
    SimpleAggregateFunction(SimpleAggFunc, &'static SqlType),
    Map(&'static SqlType, &'static SqlType),
    Date32,
    Time64(Time64Precision),
}

lazy_static! {
    static ref TYPES_CACHE: Mutex<HashMap<SqlType, Pin<Box<SqlType>>>> = Mutex::new(HashMap::new());
}

static TIME64_SQL_TYPES: [SqlType; 10] = [
    SqlType::Time64(Time64Precision(0)),
    SqlType::Time64(Time64Precision(1)),
    SqlType::Time64(Time64Precision(2)),
    SqlType::Time64(Time64Precision(3)),
    SqlType::Time64(Time64Precision(4)),
    SqlType::Time64(Time64Precision(5)),
    SqlType::Time64(Time64Precision(6)),
    SqlType::Time64(Time64Precision(7)),
    SqlType::Time64(Time64Precision(8)),
    SqlType::Time64(Time64Precision(9)),
];

impl From<SqlType> for &'static SqlType {
    fn from(value: SqlType) -> Self {
        match value {
            SqlType::UInt8 => &SqlType::UInt8,
            SqlType::UInt16 => &SqlType::UInt16,
            SqlType::UInt32 => &SqlType::UInt32,
            SqlType::UInt64 => &SqlType::UInt64,
            SqlType::Int8 => &SqlType::Int8,
            SqlType::Int16 => &SqlType::Int16,
            SqlType::Int32 => &SqlType::Int32,
            SqlType::Int64 => &SqlType::Int64,
            SqlType::String => &SqlType::String,
            SqlType::Float32 => &SqlType::Float32,
            SqlType::Float64 => &SqlType::Float64,
            SqlType::Date => &SqlType::Date,
            SqlType::Date32 => &SqlType::Date32,
            SqlType::Time64(precision) => &TIME64_SQL_TYPES[usize::from(precision.get())],
            _ => {
                let mut guard = TYPES_CACHE.lock().unwrap();
                loop {
                    if let Some(value_ref) = guard.get(&value.clone()) {
                        return unsafe { mem::transmute(value_ref.as_ref()) };
                    }
                    guard.insert(value.clone(), Box::pin(value.clone()));
                }
            }
        }
    }
}

impl SqlType {
    pub fn time64(precision: u8) -> crate::errors::Result<Self> {
        Time64Precision::new(precision).map(Self::Time64)
    }

    #[inline]
    pub(crate) fn contains_native_temporal(&self) -> bool {
        match self {
            SqlType::Date32 | SqlType::Time64(_) => true,
            SqlType::Nullable(inner)
            | SqlType::Array(inner)
            | SqlType::LowCardinality(inner)
            | SqlType::SimpleAggregateFunction(_, inner) => inner.contains_native_temporal(),
            SqlType::Map(key, value) => {
                key.contains_native_temporal() || value.contains_native_temporal()
            }
            _ => false,
        }
    }

    pub(crate) fn is_datetime(&self) -> bool {
        matches!(self, SqlType::DateTime(_))
    }

    pub(crate) fn is_inner_low_cardinality(&self) -> bool {
        matches!(
            self,
            SqlType::String
                | SqlType::FixedString(_)
                | SqlType::Date
                | SqlType::Date32
                | SqlType::DateTime(_)
                | SqlType::Time64(_)
                | SqlType::UInt8
                | SqlType::UInt16
                | SqlType::UInt32
                | SqlType::UInt64
                | SqlType::Int8
                | SqlType::Int16
                | SqlType::Int32
                | SqlType::Int64
        )
    }

    pub fn to_string(&self) -> Cow<'static, str> {
        match self.clone() {
            SqlType::Bool => "Bool".into(),
            SqlType::UInt8 => "UInt8".into(),
            SqlType::UInt16 => "UInt16".into(),
            SqlType::UInt32 => "UInt32".into(),
            SqlType::UInt64 => "UInt64".into(),
            SqlType::UInt128 => "UInt128".into(),
            SqlType::Int8 => "Int8".into(),
            SqlType::Int16 => "Int16".into(),
            SqlType::Int32 => "Int32".into(),
            SqlType::Int64 => "Int64".into(),
            SqlType::Int128 => "Int128".into(),
            SqlType::String => "String".into(),
            SqlType::FixedString(str_len) => format!("FixedString({str_len})").into(),
            SqlType::LowCardinality(inner) => format!("LowCardinality({})", &inner).into(),
            SqlType::Float32 => "Float32".into(),
            SqlType::Float64 => "Float64".into(),
            SqlType::Date => "Date".into(),
            SqlType::Date32 => "Date32".into(),
            SqlType::Time64(precision) => format!("Time64({precision})").into(),
            SqlType::DateTime(DateTimeType::DateTime64(precision, tz)) => {
                format!("DateTime64({precision}, '{tz:?}')").into()
            }
            SqlType::DateTime(_) => "DateTime".into(),
            SqlType::Ipv4 => "IPv4".into(),
            SqlType::Ipv6 => "IPv6".into(),
            SqlType::Uuid => "UUID".into(),
            SqlType::Nullable(nested) => format!("Nullable({})", &nested).into(),
            SqlType::SimpleAggregateFunction(func, nested) => {
                let func_str: &str = func.into();
                format!("SimpleAggregateFunction({}, {})", func_str, &nested).into()
            }
            SqlType::Array(nested) => format!("Array({})", &nested).into(),
            SqlType::Decimal(precision, scale) => format!("Decimal({precision}, {scale})").into(),
            SqlType::Enum8(values) => {
                let a: Vec<String> = values
                    .iter()
                    .map(|(name, value)| format!("'{name}' = {value}"))
                    .collect();
                format!("Enum8({})", a.join(",")).into()
            }
            SqlType::Enum16(values) => {
                let a: Vec<String> = values
                    .iter()
                    .map(|(name, value)| format!("'{name}' = {value}"))
                    .collect();
                format!("Enum16({})", a.join(",")).into()
            }
            SqlType::Map(k, v) => format!("Map({}, {})", &k, &v).into(),
        }
    }

    pub(crate) fn level(&self) -> u8 {
        match self {
            SqlType::Nullable(inner) => 1 + inner.level(),
            SqlType::Array(inner) => 1 + inner.level(),
            SqlType::Map(_, value) => 1 + value.level(),
            SqlType::LowCardinality(_) => 1,
            _ => 0,
        }
    }

    pub(crate) fn map_level(&self) -> u8 {
        match self {
            SqlType::Nullable(inner) => inner.level(),
            SqlType::Array(inner) => inner.level(),
            SqlType::Map(_, value) => 1 + value.level(),
            _ => 0,
        }
    }
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::to_string(self))
    }
}

#[test]
fn test_display() {
    let expected = "UInt8".to_string();
    let actual = format!("{}", SqlType::UInt8);
    assert_eq!(expected, actual);
}

#[test]
fn test_to_string() {
    let expected: Cow<'static, str> = "Nullable(UInt8)".into();
    let actual = SqlType::Nullable(&SqlType::UInt8).to_string();
    assert_eq!(expected, actual)
}
