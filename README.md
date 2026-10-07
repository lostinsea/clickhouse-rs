# Async ClickHouse Client 

[![Build Status](https://travis-ci.com/suharev7/clickhouse-rs.svg?branch=master)](https://travis-ci.com/suharev7/clickhouse-rs)
[![Crate info](https://img.shields.io/crates/v/clickhouse-rs.svg)](https://crates.io/crates/clickhouse-rs)
[![Documentation](https://docs.rs/clickhouse-rs/badge.svg)](https://docs.rs/clickhouse-rs)
[![dependency status](https://deps.rs/repo/github/suharev7/clickhouse-rs/status.svg)](https://deps.rs/repo/github/suharev7/clickhouse-rs)
[![Coverage Status](https://coveralls.io/repos/github/suharev7/clickhouse-rs/badge.svg)](https://coveralls.io/github/suharev7/clickhouse-rs)

Asynchronous [Yandex ClickHouse](https://clickhouse.yandex/) client library for rust programming language. 

## Installation
Library hosted on [crates.io](https://crates.io/crates/clickhouse-rs/).
```toml
[dependencies]
clickhouse-rs = "*"
```

## Supported data types

* Date
* Date32 (signed 32-bit days since 1970-01-01)
* DateTime
* Time64(P) (signed 64-bit coefficients, precision 0-9; no timezone or 24-hour limit)
* Decimal(P, S)
* Float32, Float64
* String, FixedString(N)
* UInt8, UInt16, UInt32, UInt64, UInt128, Int8, Int16, Int32, Int64, Int128
* Nullable(T)
* Array(UInt/Int/Float/String/Date/DateTime/Date32/Time64)
* SimpleAggregateFunction(F, T)
* IPv4/IPv6
* UUID
* Bool

`Date32` and `Time64` retain their native signed values. Use
`clickhouse_rs::types::{Date32, Time64}` with `Block::get` or `Row::get`.
`Date32::to_naive_date()` returns an error when a native day is outside
chrono's range. `Time64::new(coefficient, precision)` validates precision,
and `Time64::rescale(precision)` accepts exact conversions while rejecting
loss or overflow. `SqlType::time64(precision)` constructs a validated type;
`SqlType::Time64` carries a `Time64Precision` rather than an unchecked `u8`.
Time64 equality and hashing compare the raw coefficient **and** precision:
equivalent durations at different precisions are distinct values. For
inserts, `Block::column("date", Vec<Date32>)` creates
a Date32 column; `Block::try_time64_column("time", precision, coefficients)`
creates a Time64 column, including an empty column with an explicit
precision. `Block::try_time64_values_column("time", precision, values)`
accepts `Vec<Time64>` at mixed source precisions when every coefficient
rescales exactly to the explicit target precision. When `Block::push` infers
a Time64 column from the first row, that row's precision becomes the target;
later rows must rescale exactly without overflow. Coefficients, including
those returned by
`Time64::coefficient()`, are in units of `10^-precision` seconds, not
nanoseconds or wall-clock
timestamps. Nullable and array values can be supplied through the existing
`Value::Nullable` and `Value::Array` row APIs with explicit `SqlType` metadata
where the server accepts the schema. All-null and empty Time64 containers
need an explicit precision; there is no implicit precision for
`Option<Time64>` or `Vec<Time64>`.

`SimpleAggregateFunction(F, Date32)` and `SimpleAggregateFunction(F, Time64(P))`
support `Block::get`, `Row::get`, and typed column iteration, preserving native
values, precision, and declared wrapper metadata. Headers retain inner type
names and parameters. Combining `SimpleAggregateFunction` and `LowCardinality`
in either nesting order remains unsupported for native typed iteration; no
generic aggregate-wrapped container iterator dispatch is added.

`Time64` map keys normalize to the declared key precision, so distinct source
precisions can produce colliding keys. Built-in row APIs reject such collisions
before any column mutation, including for `SimpleAggregateFunction`-wrapped
keys, after strict declared metadata validation. Supply exact wrapped key
metadata through `Value::Map`; the convenience `HashMap` conversion does not
declare a `SimpleAggregateFunction` wrapper.

ClickHouse 26.5 accepts `LowCardinality(Date32)` with
`allow_suspicious_low_cardinality_types=1`, but rejects
`LowCardinality(Time64(P))` and `LowCardinality(Nullable(Time64(P)))`
with server error 43 even with that setting.
The client reports a type error when explicitly constructing or inserting a
column of either type; its generic reader is not restricted by this write
policy.

## DNS

```url
schema://user:password@host[:port]/database?param1=value1&...&paramN=valueN
```

parameters:

- `compression` - Whether or not use compression (defaults to `none`). Possible choices:
    * `none`
    * `lz4`

- `connection_timeout` - Timeout for connection (defaults to `500 ms`). This default is tuned for low-latency LANs; increase it (e.g., to a few seconds) if you see `[timeout] operation=connect` warnings or run over TLS/slow networks.
- `query_timeout` - Timeout for queries (defaults to `180 sec`).
- `insert_timeout` - Timeout for inserts (defaults to `180 sec`).
- `execute_timeout` - Timeout for execute (defaults to `180 sec`).
- `keepalive` - TCP keep alive timeout in milliseconds.
- `nodelay` - Whether to enable `TCP_NODELAY` (defaults to `true`).
 
- `pool_min` - Lower bound of opened connections for `Pool` (defaults to `10`).
- `pool_max` - Upper bound of opened connections for `Pool` (defaults to `20`).

- `ping_before_query` - Ping server every time before execute any query. (defaults to `true`).
- `send_retries` - Count of retry to send request to server. (defaults to `3`).
- `retry_timeout` - Amount of time to wait before next retry. (defaults to `5 sec`).
- `ping_timeout` - Timeout for ping (defaults to `500 ms`).


- `alt_hosts` - Comma separated list of single address host for load-balancing.

example:
```url
tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms
```

## Optional features

`clickhouse-rs` puts some functionality behind optional features to optimize compile time 
for the most common use cases. The following features are available.

- `tokio_io` *(enabled by default)* — I/O based on [Tokio](https://tokio.rs/).
- `async_std` — I/O based on [async-std](https://async.rs/) (doesn't work together with `tokio_io`).
- `tls` — TLS support (allowed only with `tokio_io`).

## Example

```rust
use clickhouse_rs::{Block, Pool};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name Nullable(FixedString(3))
        ) Engine=Memory";

    let block = Block::new()
        .column("customer_id",  vec![1_u32,  3,  5,  7,  9])
        .column("amount",       vec![2_u32,  4,  6,  8, 10])
        .column("account_name", vec![Some("foo"), None, None, None, Some("bar")]);

    let pool = Pool::new(database_url);

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;
    client.insert("payment", block).await?;
    let block = client.query("SELECT * FROM payment").fetch_all().await?;

    for row in block.rows() {
        let id: u32             = row.get("customer_id")?;
        let amount: u32         = row.get("amount")?;
        let name: Option<&str>  = row.get("account_name")?;
        println!("Found payment {}: {} {:?}", id, amount, name);
    }

    Ok(())
}
```
