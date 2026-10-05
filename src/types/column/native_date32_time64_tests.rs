use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    hint::black_box,
    io::Cursor,
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
            temporal::{Date32ColumnData, Time64ColumnData},
            ArcColumnWrapper,
        },
        Date32, FromSql, Marshal, RNil, RowBuilder, Simple, SqlType, StatBuffer, Time64,
        Time64Precision, Value, ValueRef,
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
