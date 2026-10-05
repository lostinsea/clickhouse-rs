#[cfg(feature = "tokio_io")]
use clickhouse_rs::{
    errors::Error,
    row,
    types::{ColumnType, Date32, RCons, RNil, RowBuilder, SqlType, Time64, Value},
    Block, Pool,
};
#[cfg(feature = "tokio_io")]
use either::Either;
#[cfg(feature = "tokio_io")]
use std::env;
#[cfg(feature = "tokio_io")]
use std::sync::Arc;

#[cfg(all(feature = "tokio_io", feature = "_tls"))]
fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9440?secure=true&skip_verify=true&compression=lz4&ping_timeout=2s&retry_timeout=3s".into()
    })
}

#[cfg(all(feature = "tokio_io", not(feature = "_tls")))]
fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9000?compression=lz4&ping_timeout=2s&retry_timeout=3s".into()
    })
}

#[cfg(feature = "tokio_io")]
fn date32(days: i32) -> Date32 {
    Date32::from_days(days)
}

#[cfg(feature = "tokio_io")]
fn time64(coefficient: i64, precision: u8) -> Time64 {
    Time64::new(coefficient, precision).unwrap()
}

#[cfg(feature = "tokio_io")]
fn sql_time64(precision: u8) -> SqlType {
    SqlType::time64(precision).unwrap()
}

#[cfg(feature = "tokio_io")]
fn nullable_date32_array(values: Vec<Option<Date32>>) -> Value {
    Value::Array(
        SqlType::Nullable(SqlType::Date32.into()).into(),
        Arc::new(
            values
                .into_iter()
                .map(|value| match value {
                    Some(value) => Value::Nullable(Either::Right(Box::new(Value::Date32(value)))),
                    None => Value::Nullable(Either::Left(SqlType::Date32.into())),
                })
                .collect(),
        ),
    )
}

#[cfg(feature = "tokio_io")]
fn nullable_time64(value: Option<Time64>, precision: u8) -> Value {
    match value {
        Some(value) => Value::Nullable(Either::Right(Box::new(Value::Time64(value)))),
        None => Value::Nullable(Either::Left(sql_time64(precision).into())),
    }
}

#[cfg(feature = "tokio_io")]
fn nullable_time64_array(values: Vec<Option<Time64>>, precision: u8) -> Value {
    Value::Array(
        SqlType::Nullable(sql_time64(precision).into()).into(),
        Arc::new(
            values
                .into_iter()
                .map(|value| nullable_time64(value, precision))
                .collect(),
        ),
    )
}

#[cfg(feature = "tokio_io")]
#[test]
fn native_date32_time64_external_row_builder_api_remains_checked_and_compatible(
) -> Result<(), Error> {
    struct ApplyOnly(Vec<(String, Value)>);

    impl RowBuilder for ApplyOnly {
        fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<(), Error> {
            self.0.apply(block)
        }
    }

    fn apply_generic<T: RowBuilder>(row: RCons<T>, block: &mut Block) -> Result<(), Error> {
        row.apply(block)
    }

    fn destination() -> Result<Block, Error> {
        Block::new()
            .column("ordinary", vec![7_u8])
            .try_time64_column("clock", 3, vec![0])
    }

    let mut generic_block = destination()?;
    let generic_before = generic_block.clone();
    assert!(apply_generic(
        RNil.put("clock".into(), Value::Time64(time64(1, 6)))
            .put("ordinary".into(), Value::UInt8(8)),
        &mut generic_block,
    )
    .is_err());
    assert_eq!(generic_block, generic_before);

    let mut custom_block = destination()?;
    let custom_before = custom_block.clone();
    assert!(custom_block
        .push(ApplyOnly(vec![
            ("ordinary".to_string(), Value::UInt8(8)),
            ("clock".to_string(), Value::Time64(time64(1, 6))),
        ]))
        .is_err());
    assert_eq!(custom_block, custom_before);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn native_date32_time64_round_trip_preserves_signed_values_and_headers() -> Result<(), Error>
{
    let table = "clickhouse_test_native_date32_time64_round_trip";
    let date_values = vec![date32(-1), date32(0), date32(1)];

    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;

    for precision in 0..=9 {
        let scale = 10_i64.pow(u32::from(precision));
        let time_values = vec![
            time64(-3, precision),
            time64(0, precision),
            time64((72 * 60 * 60 + 1) * scale + scale - 1, precision),
        ];

        client
            .execute(format!("DROP TABLE IF EXISTS {table}"))
            .await?;
        client
            .execute(format!(
                "CREATE TABLE {table} (
                    date32 Date32,
                    time64 Time64({precision})
                ) Engine=Memory"
            ))
            .await?;

        client
            .insert(
                table,
                Block::new()
                    .column("date32", date_values.clone())
                    .try_time64_values_column("time64", precision, time_values.clone())?,
            )
            .await?;

        let block = client
            .query(format!(
                "SELECT date32, time64 FROM {table} ORDER BY date32"
            ))
            .fetch_all()
            .await?;

        assert_eq!(block.get::<Date32, _>(0, "date32")?, date32(-1));
        assert_eq!(block.get::<Date32, _>(1, "date32")?, date32(0));
        assert_eq!(block.get::<Date32, _>(2, "date32")?, date32(1));
        assert_eq!(block.get::<Time64, _>(0, "time64")?, time_values[0]);
        assert_eq!(block.get::<Time64, _>(1, "time64")?, time_values[1]);
        assert_eq!(block.get::<Time64, _>(2, "time64")?, time_values[2]);
        assert_eq!(
            block.columns()[0].iter::<Date32>()?.collect::<Vec<_>>(),
            date_values
        );
        assert_eq!(
            block.columns()[1].iter::<Time64>()?.collect::<Vec<_>>(),
            time_values
        );

        let row = block.rows().next().unwrap();
        assert_eq!(row.sql_type("date32")?, SqlType::Date32);
        assert_eq!(row.sql_type("time64")?, sql_time64(precision));
    }

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn native_date32_time64_nested_nullable_array_and_low_cardinality_round_trip(
) -> Result<(), Error> {
    let table = "clickhouse_test_native_date32_time64_nested";
    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;

    client
        .execute("SET allow_suspicious_low_cardinality_types = 1")
        .await?;
    client
        .execute(format!("DROP TABLE IF EXISTS {table}"))
        .await?;
    client
        .execute(format!(
            "CREATE TABLE {table} (
                dates Array(Nullable(Date32)),
                nullable_time Nullable(Time64(6)),
                nullable_times Array(Nullable(Time64(6))),
                repeated_date LowCardinality(Date32),
                repeated_time Time64(6)
            ) Engine=Memory"
        ))
        .await?;

    let mut insert = Block::new();
    insert.push(row! {
        dates: nullable_date32_array(vec![Some(date32(-1)), None, Some(date32(0))]),
        nullable_time: nullable_time64(Some(time64(-1_234_567, 6)), 6),
        nullable_times: nullable_time64_array(
            vec![Some(time64(-1_234_567, 6)), None, Some(time64(0, 6))],
            6
        ),
        repeated_date: Value::Date32(date32(-1)),
        repeated_time: Value::Time64(time64(-1_234_567, 6))
    })?;
    insert.push(row! {
        dates: nullable_date32_array(vec![Some(date32(1))]),
        nullable_time: nullable_time64(None, 6),
        nullable_times: nullable_time64_array(vec![None, Some(time64(-2, 6))], 6),
        repeated_date: Value::Date32(date32(1)),
        repeated_time: Value::Time64(time64(-1_234_567, 6))
    })?;
    insert.push(row! {
        dates: nullable_date32_array(vec![Some(date32(-2)), None]),
        nullable_time: nullable_time64(None, 6),
        nullable_times: nullable_time64_array(vec![None, Some(time64(-2, 6))], 6),
        repeated_date: Value::Date32(date32(-1)),
        repeated_time: Value::Time64(time64(-1_234_567, 6))
    })?;
    client.insert(table, insert).await?;

    let block = client
        .query(format!(
            "SELECT dates, nullable_time, nullable_times, repeated_date, repeated_time FROM {table} ORDER BY repeated_time, length(dates) DESC"
        ))
        .fetch_all()
        .await?;

    assert_eq!(
        block.get::<Vec<Option<Date32>>, _>(0, "dates")?,
        vec![Some(date32(-1)), None, Some(date32(0))]
    );
    assert_eq!(
        block.get::<Option<Time64>, _>(0, "nullable_time")?,
        Some(time64(-1_234_567, 6))
    );
    assert_eq!(block.get::<Option<Time64>, _>(1, "nullable_time")?, None);
    assert_eq!(
        block.get::<Vec<Option<Time64>>, _>(0, "nullable_times")?,
        vec![Some(time64(-1_234_567, 6)), None, Some(time64(0, 6))]
    );
    assert_eq!(
        block.get::<Vec<Option<Time64>>, _>(1, "nullable_times")?,
        vec![None, Some(time64(-2, 6))]
    );
    assert_eq!(block.get::<Date32, _>(0, "repeated_date")?, date32(-1));
    assert_eq!(block.get::<Date32, _>(1, "repeated_date")?, date32(-1));
    assert_eq!(
        block.get::<Time64, _>(0, "repeated_time")?,
        time64(-1_234_567, 6)
    );
    let iterated: Result<Vec<Date32>, Error> =
        block.rows().map(|row| row.get("repeated_date")).collect();
    assert_eq!(iterated?, vec![date32(-1), date32(-1), date32(1)]);
    assert_eq!(
        block.columns()[3].iter::<Date32>()?.collect::<Vec<_>>(),
        vec![date32(-1), date32(-1), date32(1)]
    );
    assert_eq!(
        block.columns()[2]
            .iter::<Vec<Option<Time64>>>()?
            .collect::<Vec<_>>(),
        vec![
            vec![Some(time64(-1_234_567, 6)), None, Some(time64(0, 6))],
            vec![None, Some(time64(-2, 6))],
            vec![None, Some(time64(-2, 6))],
        ]
    );
    assert_eq!(
        block.columns()[0]
            .iter::<Vec<Option<Date32>>>()?
            .collect::<Vec<_>>(),
        vec![
            vec![Some(date32(-1)), None, Some(date32(0))],
            vec![Some(date32(-2)), None],
            vec![Some(date32(1))],
        ]
    );

    let row = block.rows().next().unwrap();
    assert_eq!(
        row.sql_type("dates")?,
        SqlType::Array(SqlType::Nullable(SqlType::Date32.into()).into())
    );
    assert_eq!(
        row.sql_type("nullable_time")?,
        SqlType::Nullable(sql_time64(6).into())
    );
    assert_eq!(
        row.sql_type("nullable_times")?,
        SqlType::Array(SqlType::Nullable(sql_time64(6).into()).into())
    );
    assert_eq!(
        row.sql_type("repeated_date")?,
        SqlType::LowCardinality(SqlType::Date32.into())
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn native_date32_time64_time64_low_cardinality_is_a_normal_server_error() -> Result<(), Error>
{
    let table = "clickhouse_test_native_date32_time64_unsupported_low_cardinality";
    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;

    client
        .execute("SET allow_suspicious_low_cardinality_types = 1")
        .await?;
    client
        .execute(format!("DROP TABLE IF EXISTS {table}"))
        .await?;

    let error = client
        .execute(format!(
            "CREATE TABLE {table} (
                time64 LowCardinality(Time64(9))
            ) Engine=Memory"
        ))
        .await
        .expect_err("ClickHouse 26.5 does not support LowCardinality(Time64)");

    match error {
        Error::Server(server) => {
            assert_eq!(server.code, 43);
            assert!(server.message.contains("DataTypeLowCardinality"));
            assert!(server.message.contains("Time64(9)"));
        }
        other => panic!("expected ClickHouse type error, got {other}"),
    }

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn native_date32_time64_nullable_array_is_a_normal_server_error() -> Result<(), Error> {
    let table = "clickhouse_test_native_date32_time64_unsupported_nullable_array";
    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;

    client
        .execute(format!("DROP TABLE IF EXISTS {table}"))
        .await?;

    let error = client
        .execute(format!(
            "CREATE TABLE {table} (
                nullable_times Nullable(Array(Time64(6)))
            ) Engine=Memory"
        ))
        .await
        .expect_err("ClickHouse 26.5 does not support Nullable(Array(Time64))");

    match error {
        Error::Server(server) => {
            assert_eq!(server.code, 43);
            assert!(server.message.contains("Nested type"));
            assert!(server.message.contains("Array(Time64(6))"));
        }
        other => panic!("expected ClickHouse type error, got {other}"),
    }

    Ok(())
}
