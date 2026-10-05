use std::fmt;

use chrono::{Datelike, NaiveDate};

use crate::errors::{Error, FromSqlError, Result};
use crate::types::Time64Precision;

const UNIX_EPOCH_DAY: i64 = 719_163;

/// A ClickHouse Date32 value, in signed days since 1970-01-01.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Date32(i32);

impl Date32 {
    pub fn new(days: i32) -> Self {
        Self(days)
    }

    pub fn from_days(days: i32) -> Self {
        Self(days)
    }

    pub fn days(self) -> i32 {
        self.0
    }

    pub fn to_naive_date(self) -> Result<NaiveDate> {
        let day = i32::try_from(UNIX_EPOCH_DAY + i64::from(self.0))
            .map_err(|_| Error::FromSql(FromSqlError::OutOfRange))?;
        NaiveDate::from_num_days_from_ce_opt(day).ok_or(Error::FromSql(FromSqlError::OutOfRange))
    }
}

impl From<NaiveDate> for Date32 {
    fn from(date: NaiveDate) -> Self {
        Self(date.num_days_from_ce() - UNIX_EPOCH_DAY as i32)
    }
}

impl fmt::Display for Date32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_naive_date() {
            Ok(date) => date.fmt(f),
            Err(_) => write!(f, "Date32({} days)", self.0),
        }
    }
}

/// A ClickHouse Time64 coefficient and its decimal precision, without a timezone or day limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Time64 {
    coefficient: i64,
    precision: Time64Precision,
}

impl Time64 {
    pub fn new(coefficient: i64, precision: u8) -> Result<Self> {
        Ok(Self {
            coefficient,
            precision: Time64Precision::new(precision)?,
        })
    }

    pub fn coefficient(self) -> i64 {
        self.coefficient
    }

    pub fn precision(self) -> u8 {
        self.precision.get()
    }

    pub(crate) fn precision_type(self) -> Time64Precision {
        self.precision
    }

    pub(crate) fn from_validated(coefficient: i64, precision: Time64Precision) -> Self {
        Self {
            coefficient,
            precision,
        }
    }

    pub fn rescale(self, precision: u8) -> Result<Self> {
        let precision = Time64Precision::new(precision)?;
        if self.precision == precision {
            return Ok(self);
        }
        let factor = 10_i128.pow(u32::from(self.precision.get().abs_diff(precision.get())));
        let coefficient = i128::from(self.coefficient);
        let overflow = || {
            Error::Other(
                format!(
                    "Time64 rescaling from precision {} to {precision} overflows i64",
                    self.precision
                )
                .into(),
            )
        };
        let rescaled = if precision.get() > self.precision.get() {
            coefficient.checked_mul(factor).ok_or_else(overflow)?
        } else {
            if coefficient % factor != 0 {
                return Err(Error::Other(
                    format!(
                        "Time64 coefficient {} cannot be rescaled from precision {} to {precision} without loss",
                        self.coefficient, self.precision
                    )
                    .into(),
                ));
            }
            coefficient / factor
        };
        let rescaled = i64::try_from(rescaled).map_err(|_| overflow())?;
        Ok(Self {
            coefficient: rescaled,
            precision,
        })
    }
}

impl fmt::Display for Time64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let factor = 10_u128.pow(u32::from(self.precision.get()));
        let magnitude = u128::from(self.coefficient.unsigned_abs());
        let seconds = magnitude / factor;
        let fraction = magnitude % factor;
        let sign = if self.coefficient < 0 { "-" } else { "" };
        if self.precision.get() == 0 {
            write!(
                f,
                "{sign}{:02}:{:02}:{:02}",
                seconds / 3600,
                seconds / 60 % 60,
                seconds % 60
            )
        } else {
            write!(
                f,
                "{sign}{:02}:{:02}:{:02}.{:0width$}",
                seconds / 3600,
                seconds / 60 % 60,
                seconds % 60,
                fraction,
                width = usize::from(self.precision.get())
            )
        }
    }
}
