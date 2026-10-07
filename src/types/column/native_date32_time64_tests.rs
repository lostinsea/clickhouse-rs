use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    hint::black_box,
    io::Cursor,
    sync::Arc,
    time::Instant,
};

use chrono::NaiveDate;
use chrono_tz::Tz;
use either::Either;

use crate::{
    binary::Encoder,
    errors::{DriverError, Error, FromSqlError},
    row,
    types::{
        column::{
            column_data::{ArcColumnData, ColumnData},
            new_column,
            simple_agg_func::SimpleAggregateFunctionColumnData,
            temporal::{Date32ColumnData, Time64ColumnData},
            ArcColumnWrapper,
        },
        Date32, FromSql, Marshal, RNil, RowBuilder, Simple, SimpleAggFunc, SqlType, StatBuffer,
        Time64, Time64Precision, Value, ValueRef,
    },
    Block,
};

fn time64(coefficient: i64, precision: u8) -> Time64 {
    Time64::new(coefficient, precision).unwrap()
}

fn date32(days: i32) -> Date32 {
    Date32::from_days(days)
}

fn sql_time64(precision: u8) -> SqlType {
    SqlType::time64(precision).unwrap()
}

fn load_empty_column(type_name: &str) -> crate::errors::Result<ArcColumnData> {
    <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(&mut Cursor::new([]), type_name, 0, Tz::UTC)
}

fn load_column(
    type_name: &str,
    bytes: Vec<u8>,
    rows: usize,
) -> crate::errors::Result<ArcColumnData> {
    <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(
        &mut Cursor::new(bytes),
        type_name,
        rows,
        Tz::UTC,
    )
}

fn low_cardinality_payload<T: Copy + Marshal + StatBuffer>(
    flags: u64,
    dictionary_value: T,
    keys_rows: u64,
    key: u8,
) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write(1_u64);
    encoder.write(flags);
    encoder.write(1_u64);
    encoder.write(dictionary_value);
    encoder.write(keys_rows);
    encoder.write(key);
    encoder.get_buffer()
}

fn decoded_low_cardinality_nullable_time64_block() -> Block<Simple> {
    const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);

    let mut encoder = Encoder::new();
    encoder.uvarint(1);
    encoder.write(false);
    encoder.uvarint(2);
    encoder.write(-1_i32);
    encoder.uvarint(0);
    encoder.uvarint(2);
    encoder.uvarint(2);

    encoder.string("ordinary");
    encoder.string("UInt8");
    encoder.write(7_u8);
    encoder.write(8_u8);

    encoder.string("clock");
    encoder.string("LowCardinality(Nullable(Time64(9)))");
    encoder.write(1_u64);
    encoder.write(UINT8_FLAGS);
    encoder.write(2_u64);
    encoder.write_bytes(&[0, 1]);
    encoder.write(-1_i64);
    encoder.write(0_i64);
    encoder.write(2_u64);
    encoder.write(0_u8);
    encoder.write(1_u8);

    Block::load(&mut Cursor::new(encoder.get_buffer()), Tz::UTC, false, 0).unwrap()
}

fn low_cardinality_nullable_time64_payload() -> Vec<u8> {
    const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);

    let mut encoder = Encoder::new();
    encoder.write(1_u64);
    encoder.write(UINT8_FLAGS);
    encoder.write(2_u64);
    encoder.write_bytes(&[0, 1]);
    encoder.write(-1_i64);
    encoder.write(0_i64);
    encoder.write(2_u64);
    encoder.write(0_u8);
    encoder.write(1_u8);
    encoder.get_buffer()
}

fn low_cardinality_nullable_date32_payload() -> Vec<u8> {
    const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);

    let mut encoder = Encoder::new();
    encoder.write(1_u64);
    encoder.write(UINT8_FLAGS);
    encoder.write(2_u64);
    encoder.write_bytes(&[0, 1]);
    encoder.write(-1_i32);
    encoder.write(0_i32);
    encoder.write(2_u64);
    encoder.write(0_u8);
    encoder.write(1_u8);
    encoder.get_buffer()
}

#[test]
fn native_date32_time64_date32_raw_codec_preserves_signed_epoch_days() {
    let values = [i32::MIN, -1, 0, 1, i32::MAX];
    let mut column = Date32ColumnData::with_capacity(values.len());

    for days in values {
        column.push(Value::Date32(Date32::from_days(days)));
    }

    let mut encoder = Encoder::new();
    column.save(&mut encoder, 0, values.len());

    let expected: Vec<u8> = values.iter().flat_map(|days| days.to_le_bytes()).collect();
    assert_eq!(encoder.get_buffer_ref(), expected);

    let mut reader = Cursor::new(expected);
    let decoded = Date32ColumnData::load(&mut reader, values.len()).unwrap();
    assert_eq!(decoded.sql_type(), SqlType::Date32);

    for (index, days) in values.into_iter().enumerate() {
        let value = decoded.at(index);
        assert_eq!(Date32::from_sql(value).unwrap().days(), days);
    }
}

#[test]
fn native_date32_time64_time64_raw_codec_preserves_all_precisions_and_signed_coefficients() {
    for precision in 0..=9 {
        let values = [i64::MIN, -3, -1, 0, 1, 3, i64::MAX];
        let mut column = Time64ColumnData::with_capacity(values.len(), precision).unwrap();

        for coefficient in values {
            column.push(Value::Time64(time64(coefficient, precision)));
        }

        let mut encoder = Encoder::new();
        column.save(&mut encoder, 0, values.len());

        let expected: Vec<u8> = values
            .iter()
            .flat_map(|coefficient| coefficient.to_le_bytes())
            .collect();
        assert_eq!(encoder.get_buffer_ref(), expected, "precision {precision}");

        let mut reader = Cursor::new(expected);
        let decoded = Time64ColumnData::load(&mut reader, values.len(), precision).unwrap();
        assert_eq!(decoded.sql_type(), sql_time64(precision));

        for (index, coefficient) in values.into_iter().enumerate() {
            let value = Time64::from_sql(decoded.at(index)).unwrap();
            assert_eq!(value.coefficient(), coefficient);
            assert_eq!(value.precision(), precision);
        }
    }
}

#[test]
fn native_date32_time64_temporal_value_contract_is_fallible_and_scale_aware() {
    let epoch = Date32::from_days(0);
    assert_eq!(
        epoch.to_naive_date().unwrap(),
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
    );
    assert!(Date32::from_days(i32::MIN).to_naive_date().is_err());
    assert!(Date32::from_days(i32::MAX).to_naive_date().is_err());

    let exact = time64(-1_234_000, 6).rescale(3).unwrap();
    assert_eq!(exact, time64(-1_234, 3));
    assert!(time64(1_234_001, 6).rescale(3).is_err());
    assert!(time64(i64::MAX, 0).rescale(9).is_err());
    assert!(time64(i64::MIN, 0).rescale(9).is_err());

    assert_eq!(time64(0, 9), time64(0, 9));
    assert_ne!(time64(0, 0), time64(0, 9));

    assert_eq!(format!("{}", time64(-1, 0)), "-00:00:01");
    assert!(!format!("{}", time64(i64::MIN, 0)).is_empty());

    let mut zero_nanoseconds_hasher = DefaultHasher::new();
    time64(0, 9).hash(&mut zero_nanoseconds_hasher);
    let mut equal_zero_nanoseconds_hasher = DefaultHasher::new();
    time64(0, 9).hash(&mut equal_zero_nanoseconds_hasher);
    assert_eq!(
        zero_nanoseconds_hasher.finish(),
        equal_zero_nanoseconds_hasher.finish()
    );
}

#[test]
fn native_date32_time64_precision_is_validated_before_sql_type_construction() {
    assert_eq!(Time64Precision::new(0).unwrap().get(), 0);
    assert_eq!(Time64Precision::try_from(9).unwrap().get(), 9);
    assert_eq!(Time64Precision::new(6).unwrap().to_string(), "6");
    assert!(Time64Precision::new(10).is_err());
    assert!(Time64Precision::try_from(10).is_err());
    assert!(SqlType::time64(10).is_err());
    assert!(Time64::new(0, 10).is_err());
}

#[test]
fn native_date32_time64_factory_parses_only_valid_native_type_headers() {
    assert!(load_empty_column("Date32").is_ok());
    assert!(load_empty_column("Time64(0)").is_ok());
    assert!(load_empty_column("Time64(9)").is_ok());
    assert!(load_empty_column("LowCardinality(Time64(9))").is_ok());

    for type_name in [
        "Time64",
        "Time64()",
        "Time64(-1)",
        "Time64(abc)",
        "Time64(9, 'UTC')",
    ] {
        assert!(
            load_empty_column(type_name).is_err(),
            "malformed Time64 header {type_name} must return an error"
        );
    }

    let error = match load_empty_column("Time64(10)") {
        Err(error) => error,
        Ok(_) => panic!("invalid precision must be rejected"),
    };
    assert!(
        error.to_string().contains("outside 0..=9"),
        "invalid precision must not fall through to an unsupported-type error: {error}"
    );
    let low_cardinality_error = match load_empty_column("LowCardinality(Time64(10))") {
        Err(error) => error,
        Ok(_) => panic!("invalid LowCardinality Time64 precision must be rejected"),
    };
    assert!(
        low_cardinality_error.to_string().contains("outside 0..=9"),
        "invalid LowCardinality Time64 precision must preserve the inner parse error: {low_cardinality_error}"
    );
}

#[test]
fn native_date32_time64_low_cardinality_time64_is_readable_but_not_writable() {
    const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);
    let readable = load_column(
        "LowCardinality(Time64(9))",
        low_cardinality_payload(UINT8_FLAGS, -1_i64, 1, 0),
        1,
    )
    .unwrap();
    assert_eq!(
        readable.sql_type(),
        SqlType::LowCardinality(sql_time64(9).into())
    );
    assert_eq!(readable.at(0), ValueRef::Time64(time64(-1, 9)));

    let error = <dyn ColumnData>::from_type::<ArcColumnWrapper>(
        SqlType::LowCardinality(sql_time64(9).into()),
        Tz::UTC,
        0,
    )
    .err()
    .expect("writable LowCardinality(Time64) construction must fail");
    match error {
        Error::FromSql(FromSqlError::InvalidType { src, dst }) => {
            assert_eq!(src, "LowCardinality(Time64(9))");
            assert_eq!(dst, "a writable LowCardinality column in this client");
        }
        other => panic!("expected typed unsupported construction error, got {other}"),
    }
}

#[test]
fn native_date32_time64_low_cardinality_nullable_time64_is_readable_but_not_writable() {
    let nullable_time64 = SqlType::Nullable(sql_time64(9).into());
    let target = SqlType::LowCardinality(nullable_time64.clone().into());

    let readable = load_empty_column("LowCardinality(Nullable(Time64(9)))").unwrap();
    assert_eq!(readable.sql_type(), target);

    let construction_error =
        <dyn ColumnData>::from_type::<ArcColumnWrapper>(target.clone(), Tz::UTC, 0)
            .err()
            .expect("writable LowCardinality(Nullable(Time64)) construction must fail");
    let source = new_column::<Simple>("clock", load_empty_column("Nullable(Time64(9))").unwrap());
    let cast_error = source
        .cast_to(target)
        .err()
        .expect("LowCardinality(Nullable(Time64)) cast must fail");

    for error in [construction_error, cast_error] {
        assert!(matches!(
            error,
            Error::FromSql(FromSqlError::InvalidType { .. })
        ));
    }
}

#[test]
fn native_date32_time64_decoded_low_cardinality_nullable_time64_rejects_rows_atomically() {
    let mut block = decoded_low_cardinality_nullable_time64_block();
    let before = block.clone();
    assert_eq!(block.row_count(), 2);
    assert_eq!(block.get::<u8, _>(0, "ordinary").unwrap(), 7);
    assert_eq!(block.get::<u8, _>(1, "ordinary").unwrap(), 8);
    assert_eq!(
        block.get::<Option<Time64>, _>(0, "clock").unwrap(),
        Some(time64(-1, 9))
    );
    assert_eq!(block.get::<Option<Time64>, _>(1, "clock").unwrap(), None);

    let precision_mismatch = row! {
        ordinary: 9_u8,
        clock: Value::Nullable(Either::Right(Box::new(Value::Time64(time64(1, 6)))))
    };
    let null_value = row! {
        ordinary: 10_u8,
        clock: Value::Nullable(Either::Left(sql_time64(9).into()))
    };

    for row in [precision_mismatch, null_value] {
        let error = block
            .push(row)
            .expect_err("decoded LowCardinality(Nullable(Time64)) rows must be rejected");
        assert!(matches!(
            error,
            Error::FromSql(FromSqlError::InvalidType { .. })
        ));
        assert_eq!(block, before);
        assert_eq!(block.row_count(), 2);
        assert_eq!(block.get::<u8, _>(0, "ordinary").unwrap(), 7);
        assert_eq!(block.get::<u8, _>(1, "ordinary").unwrap(), 8);
        assert_eq!(
            block.get::<Option<Time64>, _>(0, "clock").unwrap(),
            Some(time64(-1, 9))
        );
        assert_eq!(block.get::<Option<Time64>, _>(1, "clock").unwrap(), None);
    }
}

#[test]
fn native_date32_time64_low_cardinality_rejects_out_of_bounds_dictionary_indices() {
    const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);

    let error = load_column(
        "LowCardinality(Date32)",
        low_cardinality_payload(UINT8_FLAGS, 0_i32, 1, 1),
        1,
    )
    .err()
    .expect("out-of-range LowCardinality dictionary index must be rejected");
    match error {
        Error::Driver(DriverError::Deserialize(message)) => {
            assert_eq!(message, "LowCardinality dictionary index is out of bounds.");
        }
        other => panic!("expected dictionary decode error, got {other}"),
    }
}

#[test]
fn native_date32_time64_low_cardinality_rejects_malformed_key_metadata() {
    const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);

    let key_count_error = match load_column(
        "LowCardinality(Date32)",
        low_cardinality_payload(UINT8_FLAGS, 0_i32, 0, 0),
        1,
    ) {
        Err(Error::Driver(DriverError::Deserialize(message))) => message,
        Err(other) => panic!("expected key-count decode error, got {other}"),
        Ok(_) => panic!("mismatched LowCardinality key count must be rejected"),
    };
    assert_eq!(
        key_count_error,
        "LowCardinality key count 0 does not match row count 1."
    );

    let flags_error = match load_column(
        "LowCardinality(Date32)",
        low_cardinality_payload(1 << 9, 0_i32, 1, 0),
        1,
    ) {
        Err(Error::Driver(DriverError::Deserialize(message))) => message,
        Err(other) => panic!("expected flags decode error, got {other}"),
        Ok(_) => panic!("mismatched LowCardinality index flags must be rejected"),
    };
    assert_eq!(flags_error, "Invalid LowCardinality index flags.");

    let mut truncated_keys = Encoder::new();
    truncated_keys.write(1_u64);
    truncated_keys.write(UINT8_FLAGS);
    truncated_keys.write(1_u64);
    truncated_keys.write(0_i32);
    truncated_keys.write(1_u64);
    assert!(matches!(
        load_column("LowCardinality(Date32)", truncated_keys.get_buffer(), 1),
        Err(Error::Io(_))
    ));

    let mut overflowing_keys = Encoder::new();
    overflowing_keys.write(1_u64);
    overflowing_keys.write((1 << 9) | (1 << 10) | 3);
    overflowing_keys.write(1_u64);
    overflowing_keys.write(0_i32);
    overflowing_keys.write(u64::MAX);
    assert!(matches!(
        load_column("LowCardinality(Date32)", overflowing_keys.get_buffer(), 1),
        Err(Error::Driver(DriverError::Deserialize(_)))
    ));
}

#[test]
fn native_date32_time64_raw_coefficient_columns_preserve_empty_and_spare_capacity() {
    let empty = Vec::with_capacity(8);
    let empty_block = Block::<Simple>::new()
        .try_time64_column("clock", 9, empty)
        .unwrap();
    assert_eq!(empty_block.row_count(), 0);
    assert_eq!(empty_block.columns()[0].len(), 0);

    let exact = vec![-1_i64, 0, 1];
    let exact_block = Block::<Simple>::new()
        .try_time64_column("clock", 9, exact)
        .unwrap();
    assert_eq!(
        (0..3)
            .map(|index| {
                exact_block
                    .get::<Time64, _>(index, "clock")
                    .unwrap()
                    .coefficient()
            })
            .collect::<Vec<_>>(),
        vec![-1, 0, 1]
    );

    let mut spare_capacity = Vec::with_capacity(64);
    spare_capacity.extend([-9_i64, 0, 9]);
    let spare_capacity_block = Block::<Simple>::new()
        .try_time64_column("clock", 9, spare_capacity)
        .unwrap();
    assert_eq!(spare_capacity_block.row_count(), 3);
    assert_eq!(
        (0..3)
            .map(|index| {
                spare_capacity_block
                    .get::<Time64, _>(index, "clock")
                    .unwrap()
                    .coefficient()
            })
            .collect::<Vec<_>>(),
        vec![-9, 0, 9]
    );
}

#[test]
fn native_date32_time64_row_builders_preflight_before_mutating_ordinary_columns(
) -> crate::errors::Result<()> {
    fn destination() -> crate::errors::Result<Block<Simple>> {
        Block::<Simple>::new()
            .column("ordinary", vec![7_u8])
            .try_time64_column("clock", 3, vec![0])
    }

    fn assert_atomic(
        apply: impl FnOnce(&mut Block<Simple>) -> crate::errors::Result<()>,
    ) -> crate::errors::Result<()> {
        let mut block = destination()?;
        let before = block.clone();
        assert!(matches!(apply(&mut block), Err(Error::Other(_))));
        assert_eq!(block, before);
        Ok(())
    }

    struct DelegatingBuilder(Vec<(String, Value)>);

    impl RowBuilder for DelegatingBuilder {
        fn apply<K: crate::types::ColumnType>(
            self,
            block: &mut Block<K>,
        ) -> crate::errors::Result<()> {
            self.0.apply(block)
        }
    }

    assert_atomic(|block| {
        RNil.put("clock".into(), Value::Time64(time64(1, 6)))
            .put("ordinary".into(), Value::UInt8(8))
            .apply(block)
    })?;
    assert_atomic(|block| {
        vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("clock".to_string(), Value::Time64(time64(1, 6))),
        ]
        .apply(block)
    })?;
    assert_atomic(|block| {
        DelegatingBuilder(vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("clock".to_string(), Value::Time64(time64(1, 6))),
        ])
        .apply(block)
    })?;

    let mut duplicate = Block::<Simple>::new();
    let duplicate_before = duplicate.clone();
    assert!(duplicate
        .push(vec![
            ("clock".to_string(), Value::Time64(time64(0, 3))),
            ("clock".to_string(), Value::Time64(time64(1, 6))),
        ])
        .is_err());
    assert_eq!(duplicate, duplicate_before);

    let mut legal_duplicate = Block::<Simple>::new();
    legal_duplicate.push(vec![
        ("ordinary".to_string(), Value::UInt8(1)),
        ("ordinary".to_string(), Value::UInt8(2)),
    ])?;
    assert_eq!(legal_duplicate.get::<u8, _>(0, "ordinary")?, 1);
    assert_eq!(legal_duplicate.get::<u8, _>(1, "ordinary")?, 2);
    Ok(())
}

#[test]
fn native_date32_time64_duplicate_names_preserve_first_match_and_preflight_atomically(
) -> crate::errors::Result<()> {
    fn duplicate_date32_block() -> Block<Simple> {
        Block::<Simple>::new()
            .column("a", vec![date32(0)])
            .column("a", vec![7_u8])
    }

    fn assert_date32_first_match<K: crate::types::ColumnType>(
        mut block: Block<K>,
    ) -> crate::errors::Result<()> {
        let first_len = block.columns()[0].len();
        let second = block.columns()[1].clone();
        block.push(vec![
            ("a".to_string(), Value::Date32(date32(1))),
            ("a".to_string(), Value::Date32(date32(2))),
        ])?;
        assert_eq!(
            Date32::from_sql(block.columns()[0].at(first_len)).unwrap(),
            date32(1)
        );
        assert_eq!(
            Date32::from_sql(block.columns()[0].at(first_len + 1)).unwrap(),
            date32(2)
        );
        assert!(block.columns()[1] == second);
        Ok(())
    }

    let mut ordinary = Block::<Simple>::new()
        .column("a", vec![7_u8])
        .column("a", vec!["untouched".to_string()]);
    let ordinary_second = ordinary.columns()[1].clone();
    ordinary.push(vec![
        ("a".to_string(), Value::UInt8(8)),
        ("a".to_string(), Value::UInt8(9)),
    ])?;
    assert_eq!(ordinary.get::<u8, _>(1, "a")?, 8);
    assert_eq!(ordinary.get::<u8, _>(2, "a")?, 9);
    assert!(ordinary.columns()[1] == ordinary_second);

    let original = duplicate_date32_block();
    assert_date32_first_match(original.clone())?;
    let header = original.clone();
    assert_date32_first_match(original.cast_to(&header)?)?;
    let mut encoded = Encoder::new();
    original.write(&mut encoded, false, 0);
    assert_date32_first_match(Block::load(
        &mut Cursor::new(encoded.get_buffer()),
        Tz::UTC,
        false,
        0,
    )?)?;

    let mut row_macro = duplicate_date32_block();
    row_macro.push(row! {
        a: Value::Date32(date32(3)),
        a: Value::Date32(date32(4))
    })?;
    assert_eq!(row_macro.get::<Date32, _>(1, "a")?, date32(3));
    assert_eq!(row_macro.get::<Date32, _>(2, "a")?, date32(4));

    for invalid in [
        vec![
            ("a".to_string(), Value::Date32(date32(1))),
            ("a".to_string(), Value::Time64(time64(1, 6))),
        ],
        vec![
            ("a".to_string(), Value::Date32(date32(1))),
            ("a".to_string(), Value::Time64(time64(1, 9))),
        ],
    ] {
        let mut block = duplicate_date32_block();
        let before = block.clone();
        assert!(matches!(
            block.push(invalid),
            Err(Error::FromSql(FromSqlError::InvalidType { .. }))
        ));
        assert_eq!(block, before);
    }
    Ok(())
}

#[test]
fn native_date32_time64_duplicate_time64_names_use_the_first_target_atomically(
) -> crate::errors::Result<()> {
    fn duplicate_time64_block() -> crate::errors::Result<Block<Simple>> {
        Block::<Simple>::new()
            .try_time64_column("clock", 6, vec![0])?
            .column("ordinary", vec![7_u8])
            .try_time64_column("clock", 3, vec![0])
    }

    fn assert_first_time64_target<B: RowBuilder>(
        mut block: Block<Simple>,
        row: B,
    ) -> crate::errors::Result<()> {
        let later_clock = block.columns()[2].clone();
        block.push(row)?;
        assert_eq!(
            Time64::from_sql(block.columns()[0].at(1)).unwrap(),
            time64(1, 6)
        );
        assert_eq!(block.get::<u8, _>(1, "ordinary")?, 8);
        assert!(block.columns()[2] == later_clock);
        Ok(())
    }

    assert_first_time64_target(
        duplicate_time64_block()?,
        vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("clock".to_string(), Value::Time64(time64(1, 6))),
        ],
    )?;
    assert_first_time64_target(
        duplicate_time64_block()?,
        row! {
            ordinary: Value::UInt8(8),
            clock: Value::Time64(time64(1, 6))
        },
    )?;

    let mut invalid = duplicate_time64_block()?;
    let before = invalid.clone();
    assert!(matches!(
        invalid.push(vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("clock".to_string(), Value::Time64(time64(1, 9))),
        ]),
        Err(Error::Other(_))
    ));
    assert_eq!(invalid, before);
    Ok(())
}

#[test]
fn native_date32_time64_duplicate_cache_is_lazy_and_invalidates_on_public_appends(
) -> crate::errors::Result<()> {
    fn time64_schema() -> crate::errors::Result<Block<Simple>> {
        Block::<Simple>::new()
            .try_time64_column("clock", 6, vec![0])?
            .column("ordinary", vec![7_u8])
            .try_time64_column("clock", 3, vec![0])
    }

    let mut exact = time64_schema()?;
    let later = exact.columns()[2].clone();
    exact.push(vec![
        ("ordinary".to_string(), Value::UInt8(8)),
        ("clock".to_string(), Value::Time64(time64(1, 3))),
    ])?;
    assert_eq!(
        Time64::from_sql(exact.columns()[0].at(1)).unwrap(),
        time64(1_000, 6)
    );
    assert!(exact.columns()[2] == later);

    let mut warmed = Block::<Simple>::new()
        .try_time64_column("clock", 6, vec![0])?
        .column("ordinary", vec![7_u8]);
    warmed.push(row! {
        ordinary: Value::UInt8(8),
        clock: Value::Time64(time64(1, 6))
    })?;
    warmed = warmed.try_time64_column("clock", 3, vec![0, 0])?;
    let later = warmed.columns()[2].clone();
    warmed.push(vec![
        ("ordinary".to_string(), Value::UInt8(9)),
        ("clock".to_string(), Value::Time64(time64(1, 3))),
    ])?;
    assert_eq!(
        Time64::from_sql(warmed.columns()[0].at(2)).unwrap(),
        time64(1_000, 6)
    );
    assert!(warmed.columns()[2] == later);

    let mut inferred = Block::<Simple>::new().column("ordinary", vec![7_u8]);
    inferred.push(vec![("date".to_string(), Value::Date32(date32(0)))])?;
    inferred.push(vec![("date".to_string(), Value::Date32(date32(1)))])?;
    assert_eq!(inferred.column_count(), 2);
    assert_eq!(inferred.get::<Date32, _>(1, "date")?, date32(1));
    Ok(())
}

#[test]
fn native_date32_time64_concatenated_blocks_remain_readable_without_mutation() {
    let first = Block::<Simple>::new().column("date", vec![date32(-1), date32(0)]);
    let second = Block::<Simple>::new().column("date", vec![date32(1)]);
    let concatenated = Block::concat(&[first, second]);

    assert_eq!(
        concatenated.get::<Date32, _>(0, "date").unwrap(),
        date32(-1)
    );
    assert_eq!(concatenated.get::<Date32, _>(2, "date").unwrap(), date32(1));
    assert_eq!(
        concatenated.columns()[0]
            .iter::<Date32>()
            .unwrap()
            .collect::<Vec<_>>(),
        vec![date32(-1), date32(0), date32(1)]
    );
}

#[test]
fn native_date32_time64_casts_reject_legacy_and_recursive_low_cardinality_writes() {
    let low_cardinality_date32 = SqlType::LowCardinality(SqlType::Date32.into());
    for source_type in ["Date", "UInt32"] {
        let source = new_column::<Simple>("legacy", load_empty_column(source_type).unwrap());
        let error = source
            .cast_to(low_cardinality_date32.clone())
            .err()
            .expect("legacy cast to LowCardinality(Date32) must return an error");
        assert!(matches!(
            error,
            Error::FromSql(FromSqlError::InvalidType { .. })
        ));
    }

    for type_name in [
        "Array(LowCardinality(Time64(9)))",
        "Array(LowCardinality(Nullable(Time64(9))))",
    ] {
        let target = load_empty_column(type_name).unwrap().sql_type();
        let source = new_column::<Simple>("clock", load_empty_column(type_name).unwrap());
        let error = source
            .cast_to(target)
            .err()
            .expect("recursive LowCardinality Time64 write must be rejected");
        assert!(matches!(
            error,
            Error::FromSql(FromSqlError::InvalidType { .. })
        ));
    }
}

#[test]
fn native_date32_time64_nullable_casts_ignore_hidden_null_coefficients() {
    let mut nullable = Encoder::new();
    nullable.write_bytes(&[1, 0]);
    nullable.write(i64::MAX);
    nullable.write(-1_234_000_i64);
    let source = new_column::<Simple>(
        "clock",
        load_column("Nullable(Time64(9))", nullable.get_buffer(), 2).unwrap(),
    );
    let cast = source
        .cast_to(SqlType::Nullable(sql_time64(6).into()))
        .unwrap();
    assert_eq!(Option::<Time64>::from_sql(cast.at(0)).unwrap(), None);
    assert_eq!(
        Option::<Time64>::from_sql(cast.at(1)).unwrap(),
        Some(time64(-1_234, 6))
    );

    let mut overflow_nullable = Encoder::new();
    overflow_nullable.write_bytes(&[1, 0]);
    overflow_nullable.write(i64::MAX);
    overflow_nullable.write(2_i64);
    let overflow_source = new_column::<Simple>(
        "clock",
        load_column("Nullable(Time64(0))", overflow_nullable.get_buffer(), 2).unwrap(),
    );
    let overflow_cast = overflow_source
        .cast_to(SqlType::Nullable(sql_time64(9).into()))
        .unwrap();
    assert_eq!(
        Option::<Time64>::from_sql(overflow_cast.at(0)).unwrap(),
        None
    );
    assert_eq!(
        Option::<Time64>::from_sql(overflow_cast.at(1)).unwrap(),
        Some(time64(2_000_000_000, 9))
    );

    let mut array = Encoder::new();
    array.write(2_u64);
    array.write(4_u64);
    array.write_bytes(&[1, 0, 1, 0]);
    array.write(i64::MAX);
    array.write(-1_234_000_i64);
    array.write(i64::MIN);
    array.write(2_000_i64);
    let source = new_column::<Simple>(
        "clock",
        load_column("Array(Nullable(Time64(9)))", array.get_buffer(), 2).unwrap(),
    );
    let cast = source
        .cast_to(SqlType::Array(
            SqlType::Nullable(sql_time64(6).into()).into(),
        ))
        .unwrap();
    assert_eq!(
        Vec::<Option<Time64>>::from_sql(cast.at(0)).unwrap(),
        vec![None, Some(time64(-1_234, 6))]
    );
    assert_eq!(
        Vec::<Option<Time64>>::from_sql(cast.at(1)).unwrap(),
        vec![None, Some(time64(2, 6))]
    );

    let mut overflow_array = Encoder::new();
    overflow_array.write(2_u64);
    overflow_array.write_bytes(&[1, 0]);
    overflow_array.write(i64::MIN);
    overflow_array.write(3_i64);
    let overflow_source = new_column::<Simple>(
        "clock",
        load_column("Array(Nullable(Time64(0)))", overflow_array.get_buffer(), 1).unwrap(),
    );
    let overflow_cast = overflow_source
        .cast_to(SqlType::Array(
            SqlType::Nullable(sql_time64(9).into()).into(),
        ))
        .unwrap();
    assert_eq!(
        Vec::<Option<Time64>>::from_sql(overflow_cast.at(0)).unwrap(),
        vec![None, Some(time64(3_000_000_000, 9))]
    );
}

#[test]
fn native_date32_time64_load_errors_and_nullable_low_cardinality_iterate_safely() {
    for (case, result, expected_deserialize) in [
        (
            "truncated Date32",
            Date32ColumnData::load(&mut Cursor::new([0_u8; 3]), 1).map(|_| ()),
            false,
        ),
        (
            "truncated Time64",
            Time64ColumnData::load(&mut Cursor::new([0_u8; 7]), 1, 9).map(|_| ()),
            false,
        ),
        (
            "oversized Date32",
            Date32ColumnData::load(&mut Cursor::new([]), usize::MAX).map(|_| ()),
            true,
        ),
        (
            "oversized Time64",
            Time64ColumnData::load(&mut Cursor::new([]), usize::MAX, 9).map(|_| ()),
            true,
        ),
    ] {
        if expected_deserialize {
            assert!(
                matches!(result, Err(Error::Driver(DriverError::Deserialize(_)))),
                "{case}: {result:?}"
            );
        } else {
            assert!(matches!(result, Err(Error::Io(_))), "{case}: {result:?}");
        }
    }

    let time64_column = new_column::<Simple>(
        "clock",
        load_column(
            "LowCardinality(Nullable(Time64(9)))",
            low_cardinality_nullable_time64_payload(),
            2,
        )
        .unwrap(),
    );
    assert_eq!(
        time64_column
            .iter::<Option<Time64>>()
            .unwrap()
            .collect::<Vec<_>>(),
        vec![Some(time64(-1, 9)), None]
    );

    let date32_column = new_column::<Simple>(
        "date",
        load_column(
            "LowCardinality(Nullable(Date32))",
            low_cardinality_nullable_date32_payload(),
            2,
        )
        .unwrap(),
    );
    assert_eq!(
        date32_column
            .iter::<Option<Date32>>()
            .unwrap()
            .collect::<Vec<_>>(),
        vec![Some(date32(-1)), None]
    );

    let mut array = Encoder::new();
    array.write(2_u64);
    array.write_bytes(&low_cardinality_nullable_date32_payload());
    let array = new_column::<Simple>(
        "dates",
        load_column(
            "Array(LowCardinality(Nullable(Date32)))",
            array.get_buffer(),
            1,
        )
        .unwrap(),
    );
    assert_eq!(
        array
            .iter::<Vec<Option<Date32>>>()
            .unwrap()
            .collect::<Vec<_>>(),
        vec![vec![Some(date32(-1)), None]]
    );
}

#[test]
fn native_date32_time64_leaf_and_nested_unsupported_levels_return_errors() {
    let date32 = Date32ColumnData::with_capacity(0);
    let time64 = Time64ColumnData::with_capacity(0, 9).unwrap();

    for result in [
        unsafe { date32.get_internal(&[], 1, 0) },
        unsafe { date32.get_internals(std::ptr::null_mut(), 1, 0) },
        unsafe { time64.get_internal(&[], 1, 0) },
        unsafe { time64.get_internals(std::ptr::null_mut(), 1, 0) },
    ] {
        assert!(matches!(
            result,
            Err(Error::FromSql(FromSqlError::UnsupportedOperation))
        ));
    }

    for sql_type in [
        SqlType::Array(SqlType::Date32.into()),
        SqlType::Nullable(sql_time64(9).into()),
    ] {
        let column = <dyn ColumnData>::from_type::<ArcColumnWrapper>(sql_type, Tz::UTC, 0).unwrap();
        let result = unsafe { column.get_internal(&[], 2, 0) };
        assert!(matches!(
            result,
            Err(Error::FromSql(FromSqlError::UnsupportedOperation))
        ));
    }
}

#[test]
fn native_date32_time64_row_insertion_preflights_before_mutating_any_column(
) -> crate::errors::Result<()> {
    let mut block = Block::<Simple>::new()
        .column("date32", vec![date32(0)])
        .try_time64_values_column("time64", 3, vec![time64(0, 3)])?;
    let before = block.clone();

    let result = block.push(row! {
        date32: date32(1),
        time64: time64(1, 6)
    });

    assert!(result.is_err());
    assert_eq!(block, before);

    let mut typed_destination = Block::<Simple>::new()
        .column("date32", Vec::<Date32>::new())
        .try_time64_column("time64", 3, Vec::new())?;
    let empty_before = typed_destination.clone();

    let wrong_time64_value = row! {
        date32: date32(0),
        time64: date32(0)
    };

    assert!(typed_destination.push(wrong_time64_value).is_err());
    assert_eq!(typed_destination, empty_before);

    let mut legacy_vec_destination = Block::<Simple>::new()
        .column("date32", vec![date32(0)])
        .try_time64_column("time64", 3, vec![0])?;
    let legacy_vec_before = legacy_vec_destination.clone();
    let wrong_native_value = vec![
        ("time64".to_string(), Value::Time64(time64(0, 3))),
        ("date32".to_string(), Value::Time64(time64(0, 3))),
    ];

    assert!(legacy_vec_destination.push(wrong_native_value).is_err());
    assert_eq!(legacy_vec_destination, legacy_vec_before);
    Ok(())
}

#[test]
fn native_date32_time64_column_length_mismatch_is_precise() {
    let block = Block::<Simple>::new().column("date32", vec![date32(0), date32(1)]);

    let error = block
        .try_time64_column("time64", 3, vec![0_i64])
        .expect_err("mismatched Time64 column length must fail");
    assert_eq!(
        error.to_string(),
        "Other error: `Time64 column \"time64\" expects 2 rows, got 1.`"
    );
}

#[test]
#[ignore = "release-only codec measurement"]
fn native_date32_time64_release_codec_benchmark() {
    const ROWS: usize = 1_000_000;
    const WARMUPS: usize = 2;
    const SAMPLES: usize = 7;

    let mut date32 = Date32ColumnData::with_capacity(ROWS);
    let mut time64_column = Time64ColumnData::with_capacity(ROWS, 9).unwrap();
    for index in 0..ROWS {
        date32.push(Value::Date32(Date32::from_days(
            index as i32 - (ROWS as i32 / 2),
        )));
        time64_column.push(Value::Time64(time64(index as i64 - (ROWS as i64 / 2), 9)));
    }

    let mut date32_encode = Vec::with_capacity(SAMPLES);
    let mut date32_decode = Vec::with_capacity(SAMPLES);
    let mut time64_encode = Vec::with_capacity(SAMPLES);
    let mut time64_decode = Vec::with_capacity(SAMPLES);

    for run in 0..(WARMUPS + SAMPLES) {
        let start = Instant::now();
        let mut encoder = Encoder::new();
        date32.save(&mut encoder, 0, ROWS);
        let encoded = encoder.get_buffer();
        let encode_elapsed = start.elapsed();
        black_box(encoded.len());

        let start = Instant::now();
        let decoded = Date32ColumnData::load(&mut Cursor::new(&encoded), ROWS).unwrap();
        let decode_elapsed = start.elapsed();
        black_box(decoded.len());

        if run >= WARMUPS {
            date32_encode.push(encode_elapsed);
            date32_decode.push(decode_elapsed);
        }
    }

    for run in 0..(WARMUPS + SAMPLES) {
        let start = Instant::now();
        let mut encoder = Encoder::new();
        time64_column.save(&mut encoder, 0, ROWS);
        let encoded = encoder.get_buffer();
        let encode_elapsed = start.elapsed();
        black_box(encoded.len());

        let start = Instant::now();
        let decoded = Time64ColumnData::load(&mut Cursor::new(&encoded), ROWS, 9).unwrap();
        let decode_elapsed = start.elapsed();
        black_box(decoded.len());

        if run >= WARMUPS {
            time64_encode.push(encode_elapsed);
            time64_decode.push(decode_elapsed);
        }
    }

    let median = |samples: &mut Vec<std::time::Duration>| {
        samples.sort_unstable();
        samples[SAMPLES / 2]
    };

    println!(
        "date32_encode_ns={},date32_decode_ns={},time64_encode_ns={},time64_decode_ns={}",
        median(&mut date32_encode).as_nanos(),
        median(&mut date32_decode).as_nanos(),
        median(&mut time64_encode).as_nanos(),
        median(&mut time64_decode).as_nanos()
    );
}

#[test]
fn native_date32_time64_wrapped_date32_wire_column_iterates_like_scalar_access() {
    let expected = vec![date32(-10_957), date32(0), date32(19_723)];

    for (type_name, func) in [
        ("SimpleAggregateFunction(any, Date32)", SimpleAggFunc::Any),
        (
            "SimpleAggregateFunction(anyLast, Date32)",
            SimpleAggFunc::AnyLast,
        ),
    ] {
        let mut encoder = Encoder::new();
        for value in &expected {
            encoder.write(value.days());
        }
        let wrapped = load_column(type_name, encoder.get_buffer(), expected.len())
            .unwrap_or_else(|error| panic!("{type_name} must load from the wire: {error}"));
        assert_eq!(
            wrapped.sql_type(),
            SqlType::SimpleAggregateFunction(func, SqlType::Date32.into())
        );

        let column = new_column::<Simple>("date", wrapped);
        let scalar = (0..expected.len())
            .map(|index| Date32::from_sql(column.at(index)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(scalar, expected, "{type_name} scalar access");

        let iterated = column
            .iter::<Date32>()
            .map(|values| values.collect::<Vec<_>>())
            .unwrap_or_else(|error| {
                panic!(
                    "{type_name} loaded and read back {scalar:?} through scalar access, but \
                     typed iteration was refused with: {error}"
                )
            });
        assert_eq!(iterated, expected, "{type_name} typed iteration");
    }
}

// Negative control for the forthcoming temporal iterator fix. It deliberately
// shares no state with the baseline-failing wrapper iteration test above, so it
// passes on the untouched baseline and keeps reporting on the dictionary shapes
// even while the wrapper iteration test is still red.
#[test]
fn native_date32_time64_low_cardinality_simple_agg_chains_reject_date32_iteration() {
    fn dictionary_payload() -> Vec<u8> {
        const UINT8_FLAGS: u64 = (1 << 9) | (1 << 10);

        low_cardinality_payload(UINT8_FLAGS, -10_957_i32, 1, 0)
    }

    // Baseline: a direct dictionary of Date32 iterates today, and the wrapper
    // fix must not disturb it.
    let direct = new_column::<Simple>(
        "date",
        load_column("LowCardinality(Date32)", dictionary_payload(), 1)
            .expect("LowCardinality(Date32) must load from the wire"),
    );
    assert_eq!(
        direct.sql_type(),
        SqlType::LowCardinality(SqlType::Date32.into())
    );
    assert_eq!(Date32::from_sql(direct.at(0)).unwrap(), date32(-10_957));
    assert_eq!(
        direct.iter::<Date32>().unwrap().collect::<Vec<_>>(),
        vec![date32(-10_957)],
        "LowCardinality(Date32) typed iteration"
    );

    // The wrapper forwards `get_internal` but not `get_internals`, and a
    // dictionary needs `get_internals`. Only a SimpleAggregateFunction chain
    // terminating in a direct native scalar leaf may become iterable, so both
    // orderings of a wrapper and a dictionary must keep failing with
    // `InvalidType` naming the full source type, never with the
    // `UnsupportedOperation` that `temporal_iter` would raise and never with a
    // success.
    //
    // `SimpleAggregateFunction(any, LowCardinality(Date32))` cannot be loaded
    // from the wire on this baseline because `parse_simple_agg_fun` strips the
    // inner closing parenthesis, so it is assembled directly instead.
    let wrapper_over_dictionary: ArcColumnData = Arc::new(SimpleAggregateFunctionColumnData {
        inner: load_column("LowCardinality(Date32)", dictionary_payload(), 1)
            .expect("LowCardinality(Date32) must load from the wire"),
        func: SimpleAggFunc::Any,
    });
    let dictionary_over_wrapper = load_column(
        "LowCardinality(SimpleAggregateFunction(any, Date32))",
        dictionary_payload(),
        1,
    )
    .expect("LowCardinality(SimpleAggregateFunction(any, Date32)) must load from the wire");

    for (expected_src, data) in [
        (
            "SimpleAggregateFunction(any, LowCardinality(Date32))",
            wrapper_over_dictionary,
        ),
        (
            "LowCardinality(SimpleAggregateFunction(any, Date32))",
            dictionary_over_wrapper,
        ),
    ] {
        let column = new_column::<Simple>("date", data);
        assert_eq!(
            column.sql_type().to_string(),
            expected_src,
            "the assembled column must declare the source type this control pins"
        );
        assert_eq!(
            Date32::from_sql(column.at(0)).unwrap(),
            date32(-10_957),
            "{expected_src} scalar access"
        );

        let error = column
            .iter::<Date32>()
            .err()
            .unwrap_or_else(|| panic!("{expected_src} typed Date32 iteration must stay rejected"));
        match error {
            Error::FromSql(FromSqlError::InvalidType { src, dst }) => {
                assert_eq!(src, expected_src, "rejected source type spelling");
                assert_eq!(dst, "Date32", "{expected_src} rejected target type");
            }
            other => panic!("{expected_src}: expected a typed InvalidType rejection, got {other}"),
        }
    }
}

#[test]
fn native_date32_time64_wrapped_time64_memory_column_iterates_with_declared_precision() {
    let wrapper_type =
        SqlType::SimpleAggregateFunction(SimpleAggFunc::AnyLast, sql_time64(6).into());
    let data = <dyn ColumnData>::from_type::<ArcColumnWrapper>(wrapper_type.clone(), Tz::UTC, 4)
        .expect("in-memory SimpleAggregateFunction(anyLast, Time64(6)) must be constructible");
    let mut column = new_column::<Simple>("clock", data);
    assert_eq!(column.sql_type(), wrapper_type);

    for value in [
        time64(-1, 6),
        time64(0, 6),
        time64(86_399_999_999, 6),
        time64(1, 0),
    ] {
        column.push(Value::Time64(value));
    }

    let expected = vec![
        time64(-1, 6),
        time64(0, 6),
        time64(86_399_999_999, 6),
        time64(1_000_000, 6),
    ];
    let scalar = (0..expected.len())
        .map(|index| Time64::from_sql(column.at(index)).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(scalar, expected, "wrapped Time64 scalar access");

    let iterated = column
        .iter::<Time64>()
        .map(|values| values.collect::<Vec<_>>())
        .unwrap_or_else(|error| {
            panic!(
                "SimpleAggregateFunction(anyLast, Time64(6)) was constructed in memory and read \
                 back {scalar:?} through scalar access, but typed iteration was refused with: \
                 {error}"
            )
        });
    assert_eq!(iterated, expected, "wrapped Time64 typed iteration");
    assert!(
        iterated.iter().all(|value| value.precision() == 6),
        "typed iteration must carry the declared precision 6, got {iterated:?}"
    );

    let wrapped_low_cardinality = SqlType::SimpleAggregateFunction(
        SimpleAggFunc::AnyLast,
        SqlType::LowCardinality(sql_time64(6).into()).into(),
    );
    let error =
        <dyn ColumnData>::from_type::<ArcColumnWrapper>(wrapped_low_cardinality, Tz::UTC, 0)
            .err()
            .expect("a writable LowCardinality(Time64) under the wrapper must stay rejected");
    assert!(matches!(
        error,
        Error::FromSql(FromSqlError::InvalidType { .. })
    ));
}

// A rejected push must leave the block exactly as it was, and a length check
// alone would pass even if an already-present ordinary or map value had been
// overwritten in place. Cloning the block to snapshot it is not an option:
// `Block`/`Column`/`MapColumnData` clones only bump the `ArcColumnData`
// refcounts, so a live clone turns the very next accepted map push into an
// `Arc::get_mut(...).unwrap()` panic inside `MapColumnData::push`. `snapshot`
// therefore reads every cell into owned plain data and keeps no column handle.
#[derive(Debug, PartialEq)]
enum Cell {
    Scalar(String),
    MapUInt8(Vec<(i64, u8, u8)>),
    MapTime64(Vec<(i64, u8, i64, u8)>),
    Unreadable(String),
}

#[derive(Debug, PartialEq)]
struct ColumnSnapshot {
    name: String,
    sql_type: SqlType,
    len: usize,
    cells: Vec<Cell>,
}

fn snapshot(block: &Block<Simple>) -> Vec<ColumnSnapshot> {
    let mut columns = Vec::with_capacity(block.column_count());
    for column in block.columns() {
        let is_map = matches!(column.sql_type(), SqlType::Map(..));
        let mut cells = Vec::with_capacity(column.len());
        for row in 0..column.len() {
            if !is_map {
                cells.push(Cell::Scalar(format!("{:?}", column.at(row))));
                continue;
            }
            match <HashMap<Time64, u8>>::from_sql(column.at(row)) {
                Ok(entries) => {
                    let mut entries: Vec<(i64, u8, u8)> = entries
                        .into_iter()
                        .map(|(key, value)| (key.coefficient(), key.precision(), value))
                        .collect();
                    entries.sort_unstable();
                    cells.push(Cell::MapUInt8(entries));
                }
                Err(uint8_error) => match <HashMap<Time64, Time64>>::from_sql(column.at(row)) {
                    Ok(entries) => {
                        let mut entries: Vec<(i64, u8, i64, u8)> = entries
                            .into_iter()
                            .map(|(key, value)| {
                                (
                                    key.coefficient(),
                                    key.precision(),
                                    value.coefficient(),
                                    value.precision(),
                                )
                            })
                            .collect();
                        entries.sort_unstable();
                        cells.push(Cell::MapTime64(entries));
                    }
                    Err(_) => cells.push(Cell::Unreadable(uint8_error.to_string())),
                },
            }
        }
        columns.push(ColumnSnapshot {
            name: column.name().to_string(),
            sql_type: column.sql_type(),
            len: column.len(),
            cells,
        });
    }
    columns
}

fn shape(columns: &[ColumnSnapshot]) -> Vec<(&str, &SqlType, usize)> {
    columns
        .iter()
        .map(|column| (column.name.as_str(), &column.sql_type, column.len))
        .collect()
}

fn assert_unmutated(block: &Block<Simple>, before: &[ColumnSnapshot], case: &str) {
    let after = snapshot(block);
    assert_eq!(
        block.column_count(),
        before.len(),
        "{case}: the rejected push changed the column count"
    );
    assert_eq!(
        block.row_count(),
        before.first().map_or(0, |column| column.len),
        "{case}: the rejected push changed the row count"
    );
    assert_eq!(
        shape(&after),
        shape(before),
        "{case}: the rejected push changed the column names, types or lengths"
    );
    for (after_column, before_column) in after.iter().zip(before) {
        assert_eq!(
            after_column.cells, before_column.cells,
            "{case}: the rejected push mutated values in column `{}`",
            before_column.name
        );
    }
    assert_eq!(
        after.as_slice(),
        before,
        "{case}: the rejected push mutated existing ordinary or map column values"
    );
}

#[test]
fn native_date32_time64_wrapped_time64_map_keys_reject_normalized_collisions() {
    fn map_value(key_type: &SqlType, entries: &[(Time64, u8)]) -> Value {
        let mut map = HashMap::with_capacity(entries.len());
        for (key, value) in entries {
            map.insert(Value::Time64(*key), Value::UInt8(*value));
        }
        assert_eq!(
            map.len(),
            entries.len(),
            "the source keys must be distinct before any client-side normalization"
        );
        Value::Map(
            key_type.clone().into(),
            SqlType::UInt8.into(),
            Arc::new(map),
        )
    }

    fn seeded_block(key_type: &SqlType) -> Block<Simple> {
        let mut block = Block::<Simple>::new();
        block
            .push(vec![
                ("ordinary".to_string(), Value::UInt8(7)),
                ("tm".to_string(), map_value(key_type, &[(time64(0, 3), 1)])),
            ])
            .expect("the seed row must be accepted");
        assert_eq!(
            block.get_column("tm").unwrap().sql_type(),
            SqlType::Map(key_type.clone().into(), SqlType::UInt8.into())
        );
        block
    }

    fn second_row(key_type: &SqlType, entries: &[(Time64, u8)]) -> Vec<(String, Value)> {
        vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("tm".to_string(), map_value(key_type, entries)),
        ]
    }

    fn assert_collision_rejection(error: &Error, case: &str) {
        assert!(
            matches!(error, Error::Other(_)),
            "{case}: expected the Time64 map key collision guard to reject the row, \
             got {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("Time64") && message.contains("collide"),
            "{case}: expected a Time64 map key collision diagnostic, got `{message}`"
        );
    }

    let wrapped_key = SqlType::SimpleAggregateFunction(SimpleAggFunc::Any, sql_time64(3).into());
    let direct_key = sql_time64(3);
    let colliding = [(time64(1, 0), 10), (time64(1_000, 3), 20)];
    let distinct = [(time64(1, 0), 10), (time64(2_000, 3), 20)];

    // Control: the direct key guard rejects the same pair of keys.
    let mut direct = seeded_block(&direct_key);
    let before_direct = snapshot(&direct);
    let direct_error = direct
        .push(second_row(&direct_key, &colliding))
        .expect_err("Map(Time64(3), UInt8) must reject keys that collide after normalization");
    assert_collision_rejection(&direct_error, "direct collision");
    assert_unmutated(&direct, &before_direct, "direct collision");

    // Control: mixed precision keys that stay distinct must still be accepted,
    // through the direct key and through the wrapped key alike, and must read
    // back normalized to the declared target precision.
    for (case, key_type) in [("direct", &direct_key), ("wrapped", &wrapped_key)] {
        let mut block = seeded_block(key_type);
        block
            .push(second_row(key_type, &distinct))
            .unwrap_or_else(|error| panic!("{case} non-colliding keys must be accepted: {error}"));
        let readback: HashMap<Time64, u8> = block.get(1, "tm").unwrap();
        assert_eq!(
            readback,
            HashMap::from([(time64(1_000, 3), 10), (time64(2_000, 3), 20)]),
            "{case} non-colliding keys must preserve both entries at precision 3"
        );
    }

    // Control: a wrapped key that cannot be rescaled losslessly is still
    // rejected, so the wrapper is already transparent to per-key validation.
    let mut lossy = seeded_block(&wrapped_key);
    let before_lossy = snapshot(&lossy);
    assert!(lossy
        .push(second_row(&wrapped_key, &[(time64(1, 6), 10)]))
        .is_err());
    assert_unmutated(&lossy, &before_lossy, "wrapped lossy key");

    // Control: declared source metadata cannot disagree with the declared
    // target key in either direction, so this is not a metadata-only bypass.
    for (case, target, source) in [
        ("wrapped target, direct source", &wrapped_key, &direct_key),
        ("direct target, wrapped source", &direct_key, &wrapped_key),
    ] {
        let mut block = seeded_block(target);
        let before = snapshot(&block);
        let error = block
            .push(second_row(source, &distinct))
            .err()
            .unwrap_or_else(|| panic!("{case} must be rejected"));
        assert!(
            matches!(error, Error::FromSql(FromSqlError::InvalidType { .. })),
            "{case}: {error}"
        );
        assert_unmutated(&block, &before, case);
    }

    let mut wrapped = seeded_block(&wrapped_key);
    let before_wrapped = snapshot(&wrapped);
    let result = wrapped.push(second_row(&wrapped_key, &colliding));
    if result.is_ok() {
        let readback: HashMap<Time64, u8> = wrapped
            .get(1, "tm")
            .expect("the accepted map row must be readable");
        panic!(
            "Map(SimpleAggregateFunction(any, Time64(3)), UInt8) accepted the two distinct source \
             keys Time64(1, precision 0) and Time64(1000, precision 3), which both normalize to \
             coefficient 1000 at precision 3: push returned Ok and row 1 read back {} entry/entries \
             from 2 distinct input keys (map column length {}, row count {}); the direct form \
             rejects the same pair with: {direct_error}",
            readback.len(),
            wrapped.get_column("tm").unwrap().len(),
            wrapped.row_count()
        );
    }
    let error = result.unwrap_err();
    assert_collision_rejection(&error, "wrapped collision");
    assert_unmutated(&wrapped, &before_wrapped, "wrapped collision");
}

#[test]
fn native_date32_time64_wrapped_time64_map_guard_preserves_validation_precedence() {
    fn map_value(key_type: &SqlType, entries: &[(Time64, Time64)], reverse: bool) -> Value {
        let mut map = HashMap::with_capacity(entries.len());
        let ordered_entries: Box<dyn Iterator<Item = &(Time64, Time64)>> = if reverse {
            Box::new(entries.iter().rev())
        } else {
            Box::new(entries.iter())
        };
        for (key, value) in ordered_entries {
            map.insert(Value::Time64(*key), Value::Time64(*value));
        }
        assert_eq!(
            map.len(),
            entries.len(),
            "the source keys must be distinct before any client-side normalization"
        );
        Value::Map(key_type.clone().into(), sql_time64(3).into(), Arc::new(map))
    }

    fn seeded_block(key_type: &SqlType) -> Block<Simple> {
        let mut block = Block::<Simple>::new();
        block
            .push(vec![
                ("ordinary".to_string(), Value::UInt8(7)),
                (
                    "tm".to_string(),
                    map_value(key_type, &[(time64(0, 3), time64(0, 3))], false),
                ),
            ])
            .expect("the seed row must be accepted");
        assert_eq!(
            block.get_column("tm").unwrap().sql_type(),
            SqlType::Map(key_type.clone().into(), sql_time64(3).into())
        );
        block
    }

    fn second_row(
        key_type: &SqlType,
        entries: &[(Time64, Time64)],
        reverse: bool,
    ) -> Vec<(String, Value)> {
        vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("tm".to_string(), map_value(key_type, entries, reverse)),
        ]
    }

    fn wrong_key_row(key_type: &SqlType, value: Time64) -> Vec<(String, Value)> {
        vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            (
                "tm".to_string(),
                Value::Map(
                    key_type.clone().into(),
                    sql_time64(3).into(),
                    Arc::new(HashMap::from([(
                        Value::Date32(date32(0)),
                        Value::Time64(value),
                    )])),
                ),
            ),
        ]
    }

    let wrapped_key = SqlType::SimpleAggregateFunction(SimpleAggFunc::Any, sql_time64(3).into());
    let valid_value = time64(0, 3);
    let lossy_value = time64(1, 6);

    let mut value_control = seeded_block(&wrapped_key);
    let before_value_control = snapshot(&value_control);
    let value_error = value_control
        .push(second_row(
            &wrapped_key,
            &[(time64(2, 0), lossy_value)],
            false,
        ))
        .expect_err("a Time64(6) value with coefficient 1 must not rescale to Time64(3)");
    assert!(
        value_error
            .to_string()
            .contains("cannot be rescaled from precision 6 to 3 without loss"),
        "unexpected native value error: {value_error}"
    );
    assert_unmutated(
        &value_control,
        &before_value_control,
        "non-colliding lossy native value",
    );

    for (association, entries) in [
        (
            "lossy value on precision-0 collision key",
            [(time64(1, 0), lossy_value), (time64(1_000, 3), valid_value)],
        ),
        (
            "lossy value on precision-3 collision key",
            [(time64(1, 0), valid_value), (time64(1_000, 3), lossy_value)],
        ),
    ] {
        for (construction, reverse) in [("forward insertion", false), ("reverse insertion", true)] {
            let case = format!("{association}, {construction}");
            let mut block = seeded_block(&wrapped_key);
            let before = snapshot(&block);
            let error = block
                .push(second_row(&wrapped_key, &entries, reverse))
                .expect_err("{case}: value validation must run before collision rejection");
            assert_eq!(
                error.to_string(),
                value_error.to_string(),
                "{case}: collision detection must not mask the native value error"
            );
            assert_unmutated(&block, &before, &case);
        }
    }

    let overflow_key = time64(i64::MAX, 0);
    let mut overflow_control = seeded_block(&wrapped_key);
    let before_overflow_control = snapshot(&overflow_control);
    let overflow_error = overflow_control
        .push(second_row(
            &wrapped_key,
            &[(overflow_key, valid_value)],
            false,
        ))
        .expect_err("overflowing Time64 key must be rejected");
    assert!(
        overflow_error.to_string().contains("overflows i64"),
        "unexpected native key overflow error: {overflow_error}"
    );
    assert_unmutated(
        &overflow_control,
        &before_overflow_control,
        "overflowing native key control",
    );

    let mut overflow_with_lossy_value = seeded_block(&wrapped_key);
    let before_overflow_with_lossy_value = snapshot(&overflow_with_lossy_value);
    let error = overflow_with_lossy_value
        .push(second_row(
            &wrapped_key,
            &[(overflow_key, lossy_value)],
            false,
        ))
        .expect_err("overflowing Time64 key must be rejected before its invalid value");
    assert_eq!(
        error.to_string(),
        overflow_error.to_string(),
        "key overflow must precede native value validation"
    );
    assert_unmutated(
        &overflow_with_lossy_value,
        &before_overflow_with_lossy_value,
        "overflowing native key with lossy native value",
    );

    let mut wrong_key_control = seeded_block(&wrapped_key);
    let before_wrong_key_control = snapshot(&wrong_key_control);
    let wrong_key_error = wrong_key_control
        .push(wrong_key_row(&wrapped_key, valid_value))
        .expect_err("Date32 key must not satisfy a declared Time64 key type");
    assert!(
        matches!(
            wrong_key_error,
            Error::FromSql(FromSqlError::InvalidType { .. })
        ),
        "unexpected native key variant error: {wrong_key_error}"
    );
    assert_unmutated(
        &wrong_key_control,
        &before_wrong_key_control,
        "wrong native key variant control",
    );

    let mut wrong_key_with_lossy_value = seeded_block(&wrapped_key);
    let before_wrong_key_with_lossy_value = snapshot(&wrong_key_with_lossy_value);
    let error = wrong_key_with_lossy_value
        .push(wrong_key_row(&wrapped_key, lossy_value))
        .expect_err("wrong native key variant must be rejected before its invalid value");
    assert_eq!(
        error.to_string(),
        wrong_key_error.to_string(),
        "wrong native key validation must precede native value validation"
    );
    assert_unmutated(
        &wrong_key_with_lossy_value,
        &before_wrong_key_with_lossy_value,
        "wrong native key variant with lossy native value",
    );
}
